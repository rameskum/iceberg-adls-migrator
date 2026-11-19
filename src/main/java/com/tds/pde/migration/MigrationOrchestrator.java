package com.tds.pde.migration;

import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.dataset.DatasetClient;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.dataset.DatasetFormat;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.BlobStoreFactory;
import com.tds.pde.migration.target.LockUnavailableException;
import com.tds.pde.migration.target.ParquetDataWriter;
import com.tds.pde.migration.target.RunLock;
import com.tds.pde.migration.target.azure.AzureLocation;
import com.tds.pde.migration.processing.DataFileProcessor;
import com.tds.pde.migration.processing.FileResult;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.processing.PublishedFile;
import com.tds.pde.migration.progress.MigrationProgressTracker;
import com.tds.pde.migration.run.FileOutcomeRecorder;
import com.tds.pde.migration.run.FileRunner;
import com.tds.pde.migration.run.PartitionTasks;
import com.tds.pde.migration.run.RunStarter;
import com.tds.pde.migration.run.StartedRun;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileOutcome;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Coordinates one migration: resolve the datasets, lock the target, start or resume the run, process its
 * partitions file by file, and record how the run ended.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MigrationOrchestrator {

    /** Failed files listed in the failure summary; the state store holds all of them. */
    private static final int MAX_LISTED_FAILURES = 20;

    private final MigrationProperties properties;
    private final DatasetClient datasetClient;
    private final BlobStoreFactory blobStoreFactory;
    private final MigrationStateRepository state;
    private final RunStarter runStarter;
    private final IcebergDataReader dataReader;
    private final ParquetDataWriter dataWriter;

    /**
     * @throws MigrationFailedException when the run does not complete successfully, with the cause attached
     */
    public void migrate() {
        log.info("Migration started: sourceDataset={} targetDataset={} runId={}",
                properties.source().datasetId(),
                properties.target().datasetId(),
                properties.runId() == null ? "<new or auto-resume>" : properties.runId());
        try {
            run();
        } catch (MigrationFailedException e) {
            log.error("Migration failed: sourceDataset={} targetDataset={} reason={}",
                    properties.source().datasetId(), properties.target().datasetId(), e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("Migration failed: sourceDataset={} targetDataset={} reason={}",
                    properties.source().datasetId(), properties.target().datasetId(), e.getMessage(), e);
            throw new MigrationFailedException("Migration failed: " + e.getMessage(), e);
        }
    }

    private void run() {
        DatasetDescriptor source = dataset(properties.source().datasetId().toString(), DatasetFormat.ICEBERG);
        DatasetDescriptor target = dataset(properties.target().datasetId().toString(), DatasetFormat.PARQUET);
        AzureLocation location = location(target);
        state.createTablesIfMissing();

        BlobStore blobStore = blobStoreFactory.open(location);
        try (RunLock lock = blobStore.acquireLock(location.blobName(RunStarter.BOOKKEEPING_FOLDER + "/lock"))) {
            StartedRun run = runStarter.start(source, target, location, blobStore);
            try (MigrationProgressTracker progress = new MigrationProgressTracker(properties.progress())) {
                progress.start(run.plan().totalRecords(), run.plan().totalBytes(), run.plan().totalFiles(),
                        run.plan().totalPartitions());
                try {
                    execute(run, location, blobStore, lock, progress);
                } catch (MigrationFailedException e) {
                    throw e;
                } catch (LockUnavailableException e) {
                    // Another run may own this run's state now; leave it to that run.
                    throw e;
                } catch (RuntimeException e) {
                    recordFailure(run, progress, e.getMessage());
                    throw e;
                }
            }
        }
    }

    private void execute(StartedRun run, AzureLocation location, BlobStore blobStore, RunLock lock,
                         MigrationProgressTracker progress) {
        FileOutcomeRecorder outcomes = new FileOutcomeRecorder(state, run.runId(),
                properties.state().outcomeBatchSize(), properties.state().outcomeFlushInterval());
        Map<String, PublishedFile.Counts> published = run.resumed()
                ? restorePublished(run, location, blobStore, progress, outcomes)
                : Map.of();

        log.info("Migration overview: runId={} resumed={} table={} snapshotId={}\n  Total partitions: {}\n"
                        + "  Total data files: {}\n  Total records: {}\n  Total bytes: {}\n  Delete files: {}\n  Target: {}",
                run.runId(), run.resumed(), run.plan().table().location(), run.plan().table().snapshotId(),
                run.plan().totalPartitions(), run.plan().totalFiles(), run.plan().totalRecords(),
                run.plan().totalBytes(), run.plan().totalDeleteFiles(), run.run().targetLocation());
        if (!published.isEmpty()) {
            log.info("Resuming after already published work:\n  {}", progress.summary());
        }
        saveControl(run, RunRecord.Status.RUNNING);

        log.info("Copying files: parallelFiles={} outcomeBatchSize={}", properties.processing().parallelFiles(),
                properties.state().outcomeBatchSize());
        FileRunner.Result result = new FileRunner(run,
                new DataFileProcessor(dataReader, dataWriter, blobStore, lock::ensureHeld), outcomes, progress, lock,
                properties.failure().onFileFailure(), properties.processing().parallelFiles()).run(published);
        if (result.stopped()) {
            log.error("Stopped starting files after the first failed file "
                    + "(migration.failure.on-file-failure=FAIL_FAST); files already being copied finished");
        }

        List<FileResult> failures = result.failures();
        if (failures.isEmpty()) {
            Instant finished = Instant.now();
            state.updateRunStatus(run.runId(), RunRecord.Status.SUCCEEDED, finished, null);
            saveControl(run, RunRecord.Status.SUCCEEDED);
            log.info("Migration completed: runId={}\n  {}", run.runId(), progress.summary());
            return;
        }
        String reason = failures.size() + " of " + run.plan().totalFiles() + " files failed";
        recordFailure(run, progress, reason);
        log.error("Migration failed: runId={} {}\n  {}\n  Failed files:\n{}", run.runId(), reason, progress.summary(),
                failureList(failures));
        throw new MigrationFailedException("Migration run " + run.runId() + " failed: " + reason
                + "; resume it with migration.run-id=" + run.runId());
    }

    /**
     * Counts files a resumed run had already published as done. The blob's metadata is the proof: a file whose
     * completion the state store missed is recorded now instead of being written again. The target is listed once,
     * whatever the number of files.
     */
    private Map<String, PublishedFile.Counts> restorePublished(StartedRun run, AzureLocation location,
                                                               BlobStore blobStore,
                                                               MigrationProgressTracker progress,
                                                               FileOutcomeRecorder outcomes) {
        Map<String, PublishedFile.Counts> published = PublishedFile.find(blobStore, location.blobPrefix(),
                run.context(), run.partitions().stream().flatMap(partition -> partition.files().stream()).toList());
        for (PartitionTasks partition : run.partitions()) {
            for (FileTask task : partition.files()) {
                String sourcePath = task.source().location();
                PublishedFile.Counts counts = published.get(sourcePath);
                if (counts == null) {
                    continue;
                }
                FileRecord stored = run.fileStates().get(sourcePath);
                if (stored == null || stored.status() != FileRecord.Status.COMPLETED) {
                    log.info("Source file was already published; recording it as completed: source={} target={}",
                            task.source().location(), task.target());
                    outcomes.record(FileOutcome.completed(sourcePath, counts.recordsWritten()));
                }
                progress.fileAlreadyCompleted(counts.physicalRowsRead(), counts.recordsWritten(),
                        task.source().sizeInBytes());
            }
            if (published.keySet().containsAll(keys(partition))) {
                progress.partitionAlreadyCompleted();
            }
        }
        outcomes.flush();
        return published;
    }

    private void recordFailure(StartedRun run, MigrationProgressTracker progress, String reason) {
        try {
            Instant finished = Instant.now();
            state.updateRunStatus(run.runId(), RunRecord.Status.FAILED, finished, reason);
            saveControl(run, RunRecord.Status.FAILED);
        } catch (RuntimeException e) {
            log.error("Could not record the run as failed; it stays RUNNING until resumed: runId={} reason={}",
                    run.runId(), e.getMessage());
        }
    }

    /** The control table's view of this table migration: the source table at the snapshot, the run's status. */
    private void saveControl(StartedRun run, RunRecord.Status status) {
        state.saveControl(new ControlRecord(run.run().sourceDatasetId(), run.run().targetDatasetId(), status,
                run.runId(), run.plan().table().snapshotId(), run.plan().totalFiles(), run.plan().totalRecords()));
    }

    private static List<String> keys(PartitionTasks partition) {
        return partition.files().stream().map(task -> task.source().location()).toList();
    }

    private static String failureList(List<FileResult> failures) {
        StringBuilder list = new StringBuilder();
        failures.stream().limit(MAX_LISTED_FAILURES).forEach(failure -> list.append("    source=")
                .append(failure.task().source().location()).append(" target=")
                .append(failure.task().target()).append(" reason=").append(failure.failureReason())
                .append('\n'));
        if (failures.size() > MAX_LISTED_FAILURES) {
            list.append("    ... and ").append(failures.size() - MAX_LISTED_FAILURES)
                    .append(" more in the file table\n");
        }
        return list.toString().stripTrailing();
    }

    private DatasetDescriptor dataset(String datasetId, DatasetFormat expectedFormat) {
        DatasetDescriptor descriptor = datasetClient.getDataset(datasetId);
        if (descriptor.format() != expectedFormat) {
            throw new MigrationFailedException("Dataset '" + datasetId + "' has format " + descriptor.format()
                    + "; expected " + expectedFormat);
        }
        return descriptor;
    }

    private static AzureLocation location(DatasetDescriptor target) {
        try {
            return AzureLocation.parse(target.location());
        } catch (IllegalArgumentException e) {
            throw new MigrationFailedException("Dataset '" + target.datasetId() + "': " + e.getMessage(), e);
        }
    }
}
