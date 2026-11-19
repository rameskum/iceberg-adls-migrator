package com.tds.pde.migration.run;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.progress.ProgressSnapshot;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.SqlExecutor;
import com.tds.pde.migration.state.StateStoreException;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.LogCapture;
import com.tds.pde.migration.testing.TestProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class RunClaimTest {

    private static final Duration TIMEOUT = Duration.ofMinutes(5);
    private static final Instant CLAIMED_AT = Instant.parse("2026-11-19T10:00:00Z");
    private static final ControlRecord CLAIMED = new ControlRecord("target-1", "source-1", "hdfs://nn/orders",
            "abfss://data@acct.dfs.core.windows.net/orders", "run-1", ControlRecord.Status.RUNNING, 42L, CLAIMED_AT,
            CLAIMED_AT, null, 3, 9, ControlRecord.Progress.NONE, null);

    private final InMemoryState.Store state = new InMemoryState.Store();
    private final MutableClock clock = new MutableClock(CLAIMED_AT);
    private final RunClaim claim = new RunClaim(state, CLAIMED, TIMEOUT, clock);

    @Test
    void renewsTheHeartbeatAndWritesTheProgress() {
        state.putControl(CLAIMED);
        clock.advance(Duration.ofSeconds(30));

        claim.renew(progress(1, 0, 3, 2, 100));

        ControlRecord row = state.findControls("target-1").getFirst();
        assertThat(row.heartbeatAt()).isEqualTo(CLAIMED_AT.plusSeconds(30));
        assertThat(row.progress()).isEqualTo(new ControlRecord.Progress(1, 0, 3, 2, 100));
        assertThat(row.status()).isEqualTo(ControlRecord.Status.RUNNING);
        claim.ensureHeld();
    }

    @Test
    void losesTheClaimWhenTheRowNamesAnotherRun() {
        ControlRecord other = new ControlRecord("target-1", "source-1", "hdfs://nn/orders",
                "abfss://data@acct.dfs.core.windows.net/orders", "run-2", ControlRecord.Status.RUNNING, 42L,
                CLAIMED_AT, CLAIMED_AT, null, 3, 9, ControlRecord.Progress.NONE, null);
        state.putControl(other);

        claim.renew(progress(1, 0, 3, 2, 100));

        assertThatThrownBy(claim::ensureHeld)
                .isInstanceOf(ClaimLostException.class)
                .hasMessageContaining("Run run-1 lost its claim on the migration: the control row of target dataset "
                        + "target-1 no longer names run run-1");
        assertThat(claim.finish(ControlRecord.Status.COMPLETED, null, progress(3, 0, 9, 9, 300))).isFalse();
        assertThat(state.controls()).containsExactly(other);
    }

    @Test
    void stopsPublishingWhenItCouldNotRenewItsHeartbeatForHalfTheTimeout() {
        MigrationStateRepository failing = new FailingRenewals();
        RunClaim unrenewed = new RunClaim(failing, CLAIMED, TIMEOUT, clock);

        try (LogCapture logs = new LogCapture(RunClaim.class.getName())) {
            clock.advance(Duration.ofSeconds(150));
            unrenewed.renew(progress(0, 0, 0, 0, 0));
            unrenewed.ensureHeld();
            assertThat(logs.messagesStartingWith("Could not renew the run's heartbeat")).hasSize(1);
        }

        clock.advance(Duration.ofSeconds(1));
        assertThatThrownBy(unrenewed::ensureHeld)
                .isInstanceOf(ClaimLostException.class)
                .hasMessageContaining("could not renew its heartbeat since 2026-11-19T10:00:00Z");
    }

    @Test
    void holdsAgainOnceARenewalSucceeds() {
        state.putControl(CLAIMED);
        clock.advance(Duration.ofMinutes(3));
        assertThatThrownBy(claim::ensureHeld).isInstanceOf(ClaimLostException.class);

        claim.renew(progress(0, 0, 0, 0, 0));

        claim.ensureHeld();
    }

    @Test
    void recordsHowTheRunEndedAndThenStopsRenewingAndPublishing() {
        state.putControl(CLAIMED);
        clock.advance(Duration.ofMinutes(1));

        assertThat(claim.finish(ControlRecord.Status.COMPLETED, null, progress(3, 0, 9, 7, 300))).isTrue();
        clock.advance(Duration.ofMinutes(1));
        claim.renew(progress(0, 0, 0, 0, 0));

        ControlRecord row = state.findControls("target-1").getFirst();
        assertThat(row.status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(row.finishedAt()).isEqualTo(CLAIMED_AT.plusSeconds(60));
        assertThat(row.progress()).isEqualTo(new ControlRecord.Progress(3, 0, 9, 7, 300));
        assertThat(state.renewals()).as("no renewal after the end").isEmpty();
        assertThat(claim.isFinished()).isTrue();
        assertThatThrownBy(claim::ensureHeld).isInstanceOf(ClaimLostException.class)
                .hasMessageContaining("has already finished");
    }

    private static ProgressSnapshot progress(long completedFiles, long failedFiles, long recordsRead,
                                             long recordsWritten, long bytesWritten) {
        return new ProgressSnapshot(9, recordsRead, recordsWritten, 0, 0, 1000, 0, bytesWritten, 0, 3,
                completedFiles, failedFiles, 1, 0);
    }

    /** A repository whose heartbeat renewals keep failing, as when the state table is unavailable. */
    private static final class FailingRenewals extends MigrationStateRepository {

        FailingRenewals() {
            super(new SqlExecutor() {
                @Override
                public long update(String sql, Map<String, ?> params) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public List<Map<String, Object>> query(String sql, Map<String, ?> params) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <T> T withRows(String schema, List<Object[]> rows, Function<String, T> body) {
                    throw new UnsupportedOperationException();
                }
            }, TestProperties.defaults());
        }

        @Override
        public long renewControl(String targetDatasetId, String runId, Instant heartbeatAt,
                                 ControlRecord.Progress progress) {
            throw new StateStoreException("State store operation failed after 5 attempts", null);
        }
    }

    private static final class MutableClock extends Clock {

        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
