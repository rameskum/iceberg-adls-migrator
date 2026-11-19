package com.tds.pde.migration.run;

import com.tds.pde.migration.state.FileOutcome;
import com.tds.pde.migration.state.MigrationStateRepository;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Collects the outcomes of a run's files and writes them to the file table in batches, because every write is a
 * Delta commit taking seconds. A batch is written once it holds {@code batchSize} outcomes, or when an outcome
 * arrives after {@code flushInterval} has passed since the last write.
 *
 * <p>Outcomes still waiting when a run crashes are lost and their files stay PENDING; such a run did not complete,
 * so the next run cleans the target and copies every file again.
 *
 * <p>Thread-safe: parallel file workers report to one recorder. A write blocks the reporting worker, and others
 * reporting meanwhile, for its duration.
 */
@Slf4j
public final class FileOutcomeRecorder {

    private final MigrationStateRepository state;
    private final String runId;
    private final int batchSize;
    private final long flushIntervalNanos;
    private final LongSupplier nanoClock;

    /** By source path, so a file reported twice is written once, with its latest outcome. */
    private final Map<String, FileOutcome> pending = new LinkedHashMap<>();
    private long lastWrite;

    public FileOutcomeRecorder(MigrationStateRepository state, String runId, int batchSize, Duration flushInterval) {
        this(state, runId, batchSize, flushInterval, System::nanoTime);
    }

    FileOutcomeRecorder(MigrationStateRepository state, String runId, int batchSize, Duration flushInterval,
                        LongSupplier nanoClock) {
        this.state = state;
        this.runId = runId;
        this.batchSize = batchSize;
        this.flushIntervalNanos = flushInterval.toNanos();
        this.nanoClock = nanoClock;
        this.lastWrite = nanoClock.getAsLong();
    }

    /**
     * @throws com.tds.pde.migration.state.StateStoreException when the batch is due and cannot be written
     */
    public synchronized void record(FileOutcome outcome) {
        pending.put(outcome.sourcePath(), outcome);
        if (pending.size() >= batchSize || nanoClock.getAsLong() - lastWrite >= flushIntervalNanos) {
            flush();
        }
    }

    /** Writes every pending outcome now. */
    public synchronized void flush() {
        if (!pending.isEmpty()) {
            state.recordFileOutcomes(runId, new ArrayList<>(pending.values()));
            log.debug("File outcomes recorded: runId={} files={}", runId, pending.size());
            pending.clear();
        }
        lastWrite = nanoClock.getAsLong();
    }

    /** Drops pending outcomes unwritten, when this run may no longer write its state. */
    public synchronized void discard() {
        if (!pending.isEmpty()) {
            log.warn("File outcomes not recorded, as the run no longer owns the migration: runId={} files={}", runId,
                    pending.size());
            pending.clear();
        }
    }
}
