package com.tds.pde.migration;

import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.dataset.DatasetClient;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.dataset.DatasetFormat;
import com.tds.pde.migration.processing.DataFileProcessor;
import com.tds.pde.migration.processing.FileResult;
import com.tds.pde.migration.progress.MigrationProgressTracker;
import com.tds.pde.migration.run.ClaimLostException;
import com.tds.pde.migration.run.FileOutcomeRecorder;
import com.tds.pde.migration.run.FileRunner;
import com.tds.pde.migration.run.RunClaim;
import com.tds.pde.migration.run.RunStarter;
import com.tds.pde.migration.run.StartedRun;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.BlobStoreFactory;
import com.tds.pde.migration.target.ParquetDataWriter;
import com.tds.pde.migration.target.azure.AzureLocation;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

/**
 * Coordinates one migration: resolve the datasets, start a new run that claims the migration, prepare the target,
 * copy its files while the run's heartbeat and progress are written to the control row, and record how it ended.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MigrationOrchestrator {

    /** Failed files listed in the failure summary; the state store holds all of them. */
    private static final int MAX_LISTED_FAILURES = 20;
    private static final String START_OVER = "start again from the beginning with migration.target.clean=true";

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
        log.info("Migration started: sourceDataset={} targetDataset={} clean={}", properties.source().datasetId(),
                properties.target().datasetId(), properties.target().clean());
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
        StartedRun run = runStarter.start(source, target, location, blobStore);
        RunClaim claim = new RunClaim(state, run.control(), properties.state().heartbeatTimeout());
        // The heartbeat starts with the claim, so it is renewed while a clean deletes the target's files.
        try (MigrationProgressTracker progress = new MigrationProgressTracker(properties.progress(), claim::renew)) {
            progress.start(run.plan().totalRecords(), run.plan().totalBytes(), run.plan().totalFiles(),
                    run.plan().totalPartitions());
            try {
                runStarter.prepare(run, location, blobStore);
                execute(run, blobStore, claim, progress);
            } catch (ClaimLostException e) {
                // Another run owns the migration now; its state is left to that run.
                throw e;
            } catch (RuntimeException e) {
                if (!claim.isFinished()) {
                    finish(claim, progress, ControlRecord.Status.FAILED, e.getMessage());
                }
                throw e;
            }
        }
    }

    private void execute(StartedRun run, BlobStore blobStore, RunClaim claim, MigrationProgressTracker progress) {
        log.info("Migration overview: runId={} table={} snapshotId={}\n  Total partitions: {}\n"
                        + "  Total data files: {}\n  Total records: {}\n  Total bytes: {}\n  Delete files: {}\n"
                        + "  Target: {}", run.runId(), run.plan().table().location(),
                run.plan().table().snapshotId(), run.plan().totalPartitions(), run.plan().totalFiles(),
                run.plan().totalRecords(), run.plan().totalBytes(), run.plan().totalDeleteFiles(),
                run.control().targetLocation());

        log.info("Copying files: parallelFiles={} outcomeBatchSize={}", properties.processing().parallelFiles(),
                properties.state().outcomeBatchSize());
        FileOutcomeRecorder outcomes = new FileOutcomeRecorder(state, run.runId(),
                properties.state().outcomeBatchSize(), properties.state().outcomeFlushInterval());
        FileRunner.Result result = new FileRunner(run,
                new DataFileProcessor(dataReader, dataWriter, blobStore, claim::ensureHeld), outcomes, progress, claim,
                properties.failure().onFileFailure(), properties.processing().parallelFiles()).run();
        if (result.stopped()) {
            log.error("Stopped starting files after the first failed file "
                    + "(migration.failure.on-file-failure=FAIL_FAST); files already being copied finished");
        }

        List<FileResult> failures = result.failures();
        if (failures.isEmpty()) {
            if (!claim.finish(ControlRecord.Status.COMPLETED, null, progress.snapshot())) {
                throw new ClaimLostException("Run " + run.runId() + " copied every file, but another run took the "
                        + "migration over before it could record that");
            }
            log.info("Migration completed: runId={}\n  {}", run.runId(), progress.summary());
            return;
        }
        String reason = failures.size() + " of " + run.plan().totalFiles() + " files failed";
        finish(claim, progress, ControlRecord.Status.FAILED, reason);
        log.error("Migration failed: runId={} {}\n  {}\n  Failed files:\n{}", run.runId(), reason, progress.summary(),
                failureList(failures));
        throw new MigrationFailedException("Migration run " + run.runId() + " failed: " + reason + "; "
                + START_OVER);
    }

    /** Records a failed run; a failure to do so is logged, as the run's own failure is what gets reported. */
    private static void finish(RunClaim claim, MigrationProgressTracker progress, ControlRecord.Status status,
                               @Nullable String reason) {
        try {
            if (!claim.finish(status, reason, progress.snapshot())) {
                log.warn("Run not recorded as {}: another run owns the migration now: runId={}", status,
                        claim.runId());
            }
        } catch (RuntimeException e) {
            log.error("Could not record the run as {}; it stays RUNNING, and counts as failed once its heartbeat is "
                    + "older than migration.state.heartbeat-timeout: runId={} reason={}", status, claim.runId(),
                    e.getMessage());
        }
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
