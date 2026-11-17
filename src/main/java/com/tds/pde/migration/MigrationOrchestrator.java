package com.tds.pde.migration;

import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.dataset.DatasetClient;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.dataset.DatasetFormat;
import com.tds.pde.migration.destination.BlobStore;
import com.tds.pde.migration.destination.BlobStoreFactory;
import com.tds.pde.migration.destination.ParquetDataWriter;
import com.tds.pde.migration.destination.RunLock;
import com.tds.pde.migration.destination.azure.AzureLocation;
import com.tds.pde.migration.processing.DataFileProcessor;
import com.tds.pde.migration.processing.FileResult;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.processing.PublishedFile;
import com.tds.pde.migration.progress.MigrationProgressTracker;
import com.tds.pde.migration.run.PartitionProcessor;
import com.tds.pde.migration.run.PartitionTasks;
import com.tds.pde.migration.run.RunStarter;
import com.tds.pde.migration.run.StartedRun;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Coordinates one migration: resolve the datasets, lock the destination, start or resume the run, process its
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
        log.info("Migration started: sourceDataset={} destinationDataset={} runId={}",
                properties.source().datasetId(),
                properties.destination().datasetId(),
                properties.runId() == null ? "<new or auto-resume>" : properties.runId());
        try {
            run();
        } catch (MigrationFailedException e) {
            log.error("Migration failed: sourceDataset={} destinationDataset={} reason={}",
                    properties.source().datasetId(), properties.destination().datasetId(), e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("Migration failed: sourceDataset={} destinationDataset={} reason={}",
                    properties.source().datasetId(), properties.destination().datasetId(), e.getMessage(), e);
            throw new MigrationFailedException("Migration failed: " + e.getMessage(), e);
        }
    }

    private void run() {
        DatasetDescriptor source = dataset(properties.source().datasetId(), DatasetFormat.ICEBERG);
        DatasetDescriptor destination = dataset(properties.destination().datasetId(), DatasetFormat.PARQUET);
        AzureLocation location = location(destination);
        if (properties.state().createTables()) {
            state.createTablesIfMissing();
        }

        BlobStore blobStore = blobStoreFactory.open(location);
        try (RunLock lock = blobStore.acquireLock(location.blobName(RunStarter.BOOKKEEPING_FOLDER + "/lock"))) {
            StartedRun run = runStarter.start(source, destination, location, blobStore);
            try {
                execute(run, blobStore, lock);
            } catch (MigrationFailedException e) {
                throw e;
            } catch (RuntimeException e) {
                recordFailure(run, e.getMessage());
                throw e;
            }
        }
    }

    private void execute(StartedRun run, BlobStore blobStore, RunLock lock) {
        MigrationProgressTracker progress = new MigrationProgressTracker(properties.progress());
        progress.start(run.plan().totalRecords(), run.plan().totalFiles(), run.plan().totalPartitions());
        Map<String, PublishedFile.Counts> published = run.resumed() ? restorePublished(run, blobStore, progress)
                : Map.of();

        log.info("Migration overview: runId={} resumed={} table={} snapshotId={}\n  Total partitions: {}\n"
                        + "  Total data files: {}\n  Total records: {}\n  Delete files: {}\n  Destination: {}",
                run.runId(), run.resumed(), run.plan().table().location(), run.plan().table().snapshotId(),
                run.plan().totalPartitions(), run.plan().totalFiles(), run.plan().totalRecords(),
                run.plan().totalDeleteFiles(), run.run().destinationLocation());
        if (!published.isEmpty()) {
            log.info("Resuming after already published work:\n  {}", progress.summary());
        }

        PartitionProcessor partitions = new PartitionProcessor(run,
                new DataFileProcessor(dataReader, dataWriter, blobStore), state, progress, lock,
                properties.failure().onFileFailure());
        for (PartitionTasks partition : run.partitions()) {
            if (published.keySet().containsAll(keys(partition))) {
                continue;
            }
            log.info("Partition processing started: partition={} specId={} files={} physicalRecords={}",
                    partition.partition().displayName(), partition.partition().specId(), partition.files().size(),
                    partition.partition().recordCount());
            if (partitions.process(partition, published)) {
                log.info("Partition completed: partition={} files={}", partition.partition().displayName(),
                        partition.files().size());
                progress.partitionCompleted();
            }
            if (partitions.stopped()) {
                log.error("Stopping at the first failed file (migration.failure.on-file-failure=FAIL_FAST)");
                break;
            }
        }

        List<FileResult> failures = partitions.failures();
        if (failures.isEmpty()) {
            state.updateRunStatus(run.runId(), RunRecord.Status.SUCCEEDED, Instant.now(), null);
            log.info("Migration completed: runId={}\n  {}", run.runId(), progress.summary());
            return;
        }
        String reason = failures.size() + " of " + run.plan().totalFiles() + " files failed";
        recordFailure(run, reason);
        log.error("Migration failed: runId={} {}\n  {}\n  Failed files:\n{}", run.runId(), reason, progress.summary(),
                failureList(failures));
        throw new MigrationFailedException("Migration run " + run.runId() + " failed: " + reason
                + "; resume it with migration.run-id=" + run.runId());
    }

    /**
     * Counts files a resumed run had already published as done. The blob's metadata is the proof: a file whose
     * completion the state store missed is recorded now instead of being written again.
     */
    private Map<String, PublishedFile.Counts> restorePublished(StartedRun run, BlobStore blobStore,
                                                               MigrationProgressTracker progress) {
        Map<String, PublishedFile.Counts> published = new HashMap<>();
        for (PartitionTasks partition : run.partitions()) {
            for (FileTask task : partition.files()) {
                String fileKey = task.source().key();
                Optional<PublishedFile.Counts> counts = PublishedFile.find(blobStore, run.context(), task);
                if (counts.isEmpty()) {
                    continue;
                }
                FileRecord stored = run.fileStates().get(fileKey);
                if (stored == null || stored.status() != FileRecord.Status.COMPLETED) {
                    log.info("Source file was already published; recording it as completed: source={} destination={}",
                            task.source().location(), task.destination());
                    state.markFileCompleted(run.runId(), fileKey, counts.get().recordsWritten(),
                            counts.get().physicalRowsRead() - counts.get().recordsWritten());
                }
                published.put(fileKey, counts.get());
                progress.fileAlreadyCompleted(counts.get().physicalRowsRead(), counts.get().recordsWritten());
            }
            if (published.keySet().containsAll(keys(partition))) {
                progress.partitionAlreadyCompleted();
            }
        }
        return published;
    }

    private void recordFailure(StartedRun run, String reason) {
        try {
            state.updateRunStatus(run.runId(), RunRecord.Status.FAILED, Instant.now(), reason);
        } catch (RuntimeException e) {
            log.error("Could not record the run as failed; it stays RUNNING until resumed: runId={} reason={}",
                    run.runId(), e.getMessage());
        }
    }

    private static List<String> keys(PartitionTasks partition) {
        return partition.files().stream().map(task -> task.source().key()).toList();
    }

    private static String failureList(List<FileResult> failures) {
        StringBuilder list = new StringBuilder();
        failures.stream().limit(MAX_LISTED_FAILURES).forEach(failure -> list.append("    source=")
                .append(failure.task().source().location()).append(" destination=")
                .append(failure.task().destination()).append(" reason=").append(failure.failureReason())
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

    private static AzureLocation location(DatasetDescriptor destination) {
        try {
            return AzureLocation.parse(destination.location());
        } catch (IllegalArgumentException e) {
            throw new MigrationFailedException("Dataset '" + destination.datasetId() + "': " + e.getMessage(), e);
        }
    }
}
