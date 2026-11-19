package com.tds.pde.migration.run;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.config.MigrationProperties.Failure.OnFileFailure;
import com.tds.pde.migration.processing.DataFileProcessor;
import com.tds.pde.migration.processing.FileResult;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.progress.MigrationProgressTracker;
import com.tds.pde.migration.state.FileOutcome;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Copies a run's files, up to {@code parallelFiles} at a time, in plan order: check the run still owns the
 * migration, copy the file, count it, and record its outcome. Files are independent, so their order of completion
 * does not matter.
 *
 * <p>A failed file stops new files from starting under {@link OnFileFailure#FAIL_FAST}; files already being
 * copied finish. A lost claim, or a state write that keeps failing, stops the run the same way and is then
 * rethrown; after a lost claim nothing more is written to the state tables, as another run owns the migration now.
 */
@Slf4j
public final class FileRunner {

    /**
     * @param failures files that failed, in the order they failed
     * @param stopped  whether a failure under {@link OnFileFailure#FAIL_FAST} kept remaining files from starting
     */
    public record Result(List<FileResult> failures, boolean stopped) {
    }

    private final StartedRun run;
    private final DataFileProcessor fileProcessor;
    private final FileOutcomeRecorder outcomes;
    private final MigrationProgressTracker progress;
    private final RunClaim claim;
    private final OnFileFailure onFileFailure;
    private final int parallelFiles;

    private final List<FileResult> failures = Collections.synchronizedList(new ArrayList<>());
    private final AtomicReference<Throwable> fatal = new AtomicReference<>();
    /** Set by a failed file under {@link OnFileFailure#FAIL_FAST}: start no more files. */
    private volatile boolean stopStarting;

    public FileRunner(StartedRun run, DataFileProcessor fileProcessor, FileOutcomeRecorder outcomes,
                      MigrationProgressTracker progress, RunClaim claim, OnFileFailure onFileFailure,
                      int parallelFiles) {
        this.run = run;
        this.fileProcessor = fileProcessor;
        this.outcomes = outcomes;
        this.progress = progress;
        this.claim = claim;
        this.onFileFailure = onFileFailure;
        this.parallelFiles = parallelFiles;
    }

    /**
     * @throws ClaimLostException when the run no longer owns the migration
     */
    public Result run() {
        AtomicInteger workerNumber = new AtomicInteger();
        ExecutorService workers = Executors.newFixedThreadPool(parallelFiles, runnable -> {
            // Short enough for the log pattern's 15-character thread column.
            Thread thread = new Thread(runnable, "file-worker-" + workerNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        Semaphore slots = new Semaphore(parallelFiles);
        boolean allStarted = false;
        try {
            allStarted = submit(workers, slots);
            // Every slot back means every started file has finished.
            slots.acquire(parallelFiles);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fatal.compareAndSet(null, new MigrationFailedException("Interrupted while copying files", e));
        } finally {
            workers.shutdownNow();
        }

        Throwable failure = fatal.get();
        if (failure instanceof ClaimLostException) {
            outcomes.discard();
        } else {
            try {
                outcomes.flush();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new Result(List.copyOf(failures), stopStarting && !allStarted);
    }

    /** @return whether every file was started */
    private boolean submit(ExecutorService workers, Semaphore slots) throws InterruptedException {
        for (PartitionTasks partition : run.partitions()) {
            List<FileTask> files = partition.files();
            if (files.isEmpty()) {
                continue;
            }
            log.info("Partition processing started: partition={} specId={} files={} physicalRecords={}",
                    partition.partition().displayName(), partition.partition().specId(), partition.files().size(),
                    partition.partition().recordCount());
            AtomicInteger unfinished = new AtomicInteger(files.size());
            for (FileTask task : files) {
                slots.acquire();
                if (stopStarting || fatal.get() != null) {
                    slots.release();
                    return false;
                }
                workers.execute(() -> {
                    try {
                        copy(task, partition, unfinished);
                    } catch (Throwable e) {
                        fatal.compareAndSet(null, e);
                    } finally {
                        slots.release();
                    }
                });
            }
        }
        return true;
    }

    private void copy(FileTask task, PartitionTasks partition, AtomicInteger unfinished) {
        String sourcePath = task.source().location();
        // Never write while another run may own the target.
        claim.ensureHeld();
        progress.fileStarted(sourcePath, task.source().sizeInBytes(), task.source().recordCount());
        FileResult result = fileProcessor.process(run.context(), task,
                (physicalRowsRead, written, bytesWritten) ->
                        progress.onFileProgress(sourcePath, physicalRowsRead, written, bytesWritten));
        if (result.succeeded()) {
            progress.fileCompleted(sourcePath, result.physicalRowsRead(), result.recordsWritten(),
                    result.bytesWritten());
            outcomes.record(FileOutcome.completed(sourcePath, result.recordsWritten()));
            if (unfinished.decrementAndGet() == 0) {
                log.info("Partition completed: partition={} files={}", partition.partition().displayName(),
                        partition.files().size());
                progress.partitionCompleted();
            }
            return;
        }
        progress.fileFailed(sourcePath);
        if (result.error() instanceof ClaimLostException claimLost) {
            throw claimLost;
        }
        failures.add(result);
        outcomes.record(FileOutcome.failed(sourcePath, result.failureReason()));
        if (onFileFailure == OnFileFailure.FAIL_FAST) {
            stopStarting = true;
        }
    }
}
