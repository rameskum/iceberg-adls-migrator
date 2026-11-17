package com.tds.pde.migration.run;

import com.tds.pde.migration.config.MigrationProperties.Failure.OnFileFailure;
import com.tds.pde.migration.target.LockUnavailableException;
import com.tds.pde.migration.target.RunLock;
import com.tds.pde.migration.processing.DataFileProcessor;
import com.tds.pde.migration.processing.FileResult;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.processing.PublishedFile;
import com.tds.pde.migration.progress.MigrationProgressTracker;
import com.tds.pde.migration.state.MigrationStateRepository;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Runs the files of one partition at a time through the file-level checkpoint: check the run lock, process the
 * file, record the outcome, and count it. One instance serves one run.
 */
public final class PartitionProcessor {

    private final StartedRun run;
    private final DataFileProcessor fileProcessor;
    private final MigrationStateRepository state;
    private final MigrationProgressTracker progress;
    private final RunLock lock;
    private final OnFileFailure onFileFailure;
    private final List<FileResult> failures = new ArrayList<>();
    private boolean stopped;

    public PartitionProcessor(StartedRun run, DataFileProcessor fileProcessor, MigrationStateRepository state,
                              MigrationProgressTracker progress, RunLock lock, OnFileFailure onFileFailure) {
        this.run = run;
        this.fileProcessor = fileProcessor;
        this.state = state;
        this.progress = progress;
        this.lock = lock;
        this.onFileFailure = onFileFailure;
    }

    /**
     * @param published files this run had already published, by file key; they are skipped
     * @return whether every file of the partition is now published
     */
    public boolean process(PartitionTasks partition, Map<String, PublishedFile.Counts> published) {
        boolean allPublished = true;
        for (FileTask task : partition.files()) {
            String fileKey = task.source().key();
            if (published.containsKey(fileKey)) {
                continue;
            }
            // Never write while another run may hold the target.
            lock.ensureHeld();
            FileResult result = fileProcessor.process(run.context(), task, progress::onFileProgress);
            if (result.succeeded()) {
                state.markFileCompleted(run.runId(), fileKey, result.recordsWritten(), result.recordsDeleted());
                progress.fileCompleted(result.physicalRowsRead(), result.recordsWritten());
            } else if (result.error() instanceof LockUnavailableException lockLost) {
                // Another run may own this run's state now: stop without writing to it.
                throw lockLost;
            } else {
                progress.fileFailed();
                state.markFileFailed(run.runId(), fileKey, result.failureReason());
                failures.add(result);
                allPublished = false;
                if (onFileFailure == OnFileFailure.FAIL_FAST) {
                    stopped = true;
                    return false;
                }
            }
        }
        return allPublished;
    }

    /** Whether a failure under {@link OnFileFailure#FAIL_FAST} ended processing. */
    public boolean stopped() {
        return stopped;
    }

    public List<FileResult> failures() {
        return List.copyOf(failures);
    }
}
