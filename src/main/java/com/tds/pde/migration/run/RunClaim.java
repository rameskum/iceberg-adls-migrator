package com.tds.pde.migration.run;

import com.tds.pde.migration.progress.ProgressSnapshot;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * A run's ownership of its migration's control row, from the claim {@link RunStarter} made until the run ends.
 *
 * <p>{@link #renew} renews the heartbeat and writes the progress, on every {@code Progress:} line. A run whose row
 * names another run id now has lost the migration: another run cleaned the target after this one's heartbeat went
 * stale. {@link #ensureHeld()}, checked before each file starts and again just before it is published, then fails,
 * and so it does while this run has not renewed its heartbeat for half the heartbeat timeout: from the timeout on,
 * another run may clean the target, so this one stops publishing well before.
 *
 * <p>Thread-safe: renewals come from the progress timer, checks from the file workers.
 */
@Slf4j
public final class RunClaim {

    private final MigrationStateRepository state;
    private final String targetDatasetId;
    private final String runId;
    private final Duration heartbeatTimeout;
    private final Clock clock;

    /** The heartbeat last written to the row. */
    private volatile Instant renewedAt;
    private volatile @Nullable String lostReason;
    private volatile boolean finished;

    public RunClaim(MigrationStateRepository state, ControlRecord claimed, Duration heartbeatTimeout) {
        this(state, claimed, heartbeatTimeout, Clock.systemUTC());
    }

    RunClaim(MigrationStateRepository state, ControlRecord claimed, Duration heartbeatTimeout, Clock clock) {
        this.state = state;
        this.targetDatasetId = claimed.targetDatasetId();
        this.runId = claimed.runId();
        this.heartbeatTimeout = heartbeatTimeout;
        this.clock = clock;
        this.renewedAt = claimed.heartbeatAt();
    }

    public String runId() {
        return runId;
    }

    /** Whether {@link #finish} was called. */
    public boolean isFinished() {
        return finished;
    }

    /**
     * Renews the heartbeat and writes the progress. A write that fails even after retries is logged and left to the
     * next renewal; {@link #ensureHeld()} stops the run if renewals keep failing.
     */
    public synchronized void renew(ProgressSnapshot progress) {
        if (finished || lostReason != null) {
            return;
        }
        Instant now = clock.instant();
        try {
            long updated = state.renewControl(targetDatasetId, runId, now, progress(progress));
            if (updated == 0) {
                lose("the control row of target dataset " + targetDatasetId + " no longer names run " + runId
                        + ": another run took the migration over after this run's heartbeat went stale");
                return;
            }
            renewedAt = now;
        } catch (RuntimeException e) {
            log.warn("Could not renew the run's heartbeat; trying again with the next progress line: runId={} "
                    + "lastRenewedAt={} reason={}", runId, renewedAt, e.getMessage());
        }
    }

    /**
     * @throws ClaimLostException when the run lost its control row, could not renew its heartbeat for half the
     *                            heartbeat timeout, or has already finished
     */
    public void ensureHeld() {
        String lost = lostReason;
        if (lost != null) {
            throw new ClaimLostException("Run " + runId + " lost its claim on the migration: " + lost);
        }
        if (finished) {
            throw new ClaimLostException("Run " + runId + " has already finished");
        }
        Instant renewed = renewedAt;
        if (renewed.plus(heartbeatTimeout.dividedBy(2)).isBefore(clock.instant())) {
            throw new ClaimLostException("Run " + runId + " could not renew its heartbeat since " + renewed
                    + "; it stops publishing before another run may clean the target (heartbeat timeout "
                    + heartbeatTimeout + ")");
        }
    }

    /**
     * Records how the run ended, if it still owns the control row; afterwards nothing more is published or renewed.
     *
     * @return whether the outcome was recorded
     * @throws com.tds.pde.migration.state.StateStoreException when it cannot be written
     */
    public synchronized boolean finish(ControlRecord.Status status, @Nullable String error,
                                       ProgressSnapshot progress) {
        finished = true;
        if (lostReason != null) {
            return false;
        }
        long updated = state.finishControl(targetDatasetId, runId, status, clock.instant(), error,
                progress(progress));
        if (updated == 0) {
            lose("the control row of target dataset " + targetDatasetId + " no longer names run " + runId);
            return false;
        }
        return true;
    }

    private void lose(String reason) {
        lostReason = reason;
        log.error("Run lost its claim on the migration and stops publishing: runId={} reason={}", runId, reason);
    }

    private static ControlRecord.Progress progress(ProgressSnapshot snapshot) {
        return new ControlRecord.Progress(snapshot.completedFiles(), snapshot.failedFiles(),
                snapshot.committedRecords(), snapshot.recordsWritten(), snapshot.bytesWritten());
    }
}
