package com.tds.pde.migration.progress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.tds.pde.migration.testing.LogCapture;
import com.tds.pde.migration.testing.TestProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MigrationProgressTrackerTest {

    private static final long MIB = 1024 * 1024;

    private final AtomicLong clock = new AtomicLong();

    /** A tracker whose heartbeat only runs when the test calls it, with a clock the test moves. */
    private MigrationProgressTracker tracker() {
        return new MigrationProgressTracker(TestProperties.defaults().progress(), clock::get);
    }

    private void advance(Duration time) {
        clock.addAndGet(time.toNanos());
    }

    /** The one Progress line logged by the heartbeat. */
    private static String heartbeat(MigrationProgressTracker progress) {
        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.heartbeat();
            return logs.messagesStartingWith("Progress: ").getFirst();
        }
    }

    @Test
    void computesPercentagesWithTheRequiredFormula() {
        assertThat(MigrationProgressTracker.percentage(1, 120)).isEqualTo(100.0 / 120);
        assertThat(MigrationProgressTracker.percentage(0, 0)).isEqualTo(100.0);
        assertThat(MigrationProgressTracker.percentage(250_000_000L, 250_000_000L)).isEqualTo(100.0);
        // Large counts must not overflow.
        assertThat(MigrationProgressTracker.percentage(Long.MAX_VALUE / 2, Long.MAX_VALUE)).isEqualTo(50.0);
    }

    @Test
    void formatsPercentagesLikeTheRequirementsExamples() {
        assertThat(MigrationProgressTracker.format(1, 120)).isEqualTo("0.8333");
        assertThat(MigrationProgressTracker.format(1, 15_000)).isEqualTo("0.0067");
        assertThat(MigrationProgressTracker.format(10_000, 250_000_000)).isEqualTo("0.004");
        assertThat(MigrationProgressTracker.format(120, 120)).isEqualTo("100");
        assertThat(MigrationProgressTracker.format(0, 0)).isEqualTo("100");
    }

    @Test
    void formatsBytesAndDurationsForHumans() {
        assertThat(MigrationProgressTracker.formatBytes(0)).isEqualTo("0B");
        assertThat(MigrationProgressTracker.formatBytes(1023)).isEqualTo("1023B");
        assertThat(MigrationProgressTracker.formatBytes(1024)).isEqualTo("1KiB");
        assertThat(MigrationProgressTracker.formatBytes(1536)).isEqualTo("1.5KiB");
        assertThat(MigrationProgressTracker.formatBytes(100 * MIB)).isEqualTo("100MiB");
        assertThat(MigrationProgressTracker.formatBytes(1288490189L)).isEqualTo("1.2GiB");
        assertThat(MigrationProgressTracker.formatDuration(Duration.ofSeconds(750))).isEqualTo("0:12:30");
        assertThat(MigrationProgressTracker.formatDuration(Duration.ofHours(36).plusMinutes(5).plusSeconds(10)))
                .isEqualTo("36:05:10");
    }

    @Test
    void commitsRecordsOnlyWhenTheirFileIsPublished() {
        MigrationProgressTracker progress = tracker();
        progress.start(300, 300 * MIB, 3, 2);

        progress.fileStarted("file-1", 100 * MIB, 100);
        progress.onFileProgress("file-1", 80, 70, 10 * MIB);
        assertThat(progress.snapshot().committedRecords()).isZero();
        assertThat(progress.snapshot().inFlightRecords()).isEqualTo(80);
        assertThat(progress.snapshot().inFlightFiles()).isEqualTo(1);

        progress.fileCompleted("file-1", 100, 90, 40 * MIB);
        ProgressSnapshot afterFirst = progress.snapshot();
        assertThat(afterFirst.committedRecords()).isEqualTo(100);
        assertThat(afterFirst.recordsWritten()).isEqualTo(90);
        assertThat(afterFirst.bytesRead()).isEqualTo(100 * MIB);
        assertThat(afterFirst.bytesWritten()).isEqualTo(40 * MIB);
        assertThat(afterFirst.inFlightRecords()).isZero();
        assertThat(afterFirst.inFlightFiles()).isZero();
        assertThat(afterFirst.completedFiles()).isEqualTo(1);
        assertThat(afterFirst.completedPartitions()).isZero();
    }

    @Test
    void addsUpTheInFlightRowsOfFilesProcessedInParallel() {
        MigrationProgressTracker progress = tracker();
        progress.start(300, 300 * MIB, 3, 2);

        progress.fileStarted("file-1", 100 * MIB, 100);
        progress.fileStarted("file-2", 100 * MIB, 100);
        progress.onFileProgress("file-1", 80, 70, MIB);
        progress.onFileProgress("file-2", 30, 30, MIB);
        progress.onFileProgress("file-1", 90, 80, MIB);
        assertThat(progress.snapshot().inFlightRecords()).isEqualTo(120);
        assertThat(progress.snapshot().inFlightWritten()).isEqualTo(110);
        assertThat(progress.snapshot().inFlightFiles()).isEqualTo(2);

        progress.fileCompleted("file-1", 100, 90, MIB);
        assertThat(progress.snapshot().inFlightRecords()).as("only file-2 is still in flight").isEqualTo(30);
        assertThat(progress.snapshot().committedRecords()).isEqualTo(100);
    }

    @Test
    void discardsTheInFlightRowsOfAFailedFile() {
        MigrationProgressTracker progress = tracker();
        progress.start(300, 300 * MIB, 3, 2);

        progress.fileStarted("file-1", 100 * MIB, 100);
        progress.onFileProgress("file-1", 60, 60, MIB);
        progress.fileFailed("file-1");

        ProgressSnapshot snapshot = progress.snapshot();
        assertThat(snapshot.committedRecords()).isZero();
        assertThat(snapshot.inFlightRecords()).isZero();
        assertThat(snapshot.inFlightFiles()).isZero();
        assertThat(snapshot.bytesRead()).isZero();
        assertThat(snapshot.completedFiles()).isZero();
        assertThat(snapshot.failedFiles()).isEqualTo(1);
    }

    @Test
    void heartbeatReportsFilesRecordsBytesRateElapsedAndEta() {
        MigrationProgressTracker progress = tracker();
        progress.start(1_000, 400 * MIB, 4, 2);
        progress.fileStarted("file-1", 100 * MIB, 250);
        progress.fileStarted("file-2", 100 * MIB, 250);
        progress.fileCompleted("file-1", 250, 240, 40 * MIB);
        // Half of file-2's rows are read: half of its bytes count as read.
        progress.onFileProgress("file-2", 125, 120, 20 * MIB);
        advance(Duration.ofSeconds(60));

        assertThat(heartbeat(progress)).isEqualTo("Progress: files=1/4 (25%) remaining=3 failed=0 inFlight=1 "
                + "records=375/1000 (37.5%) written=360 deleted=15 "
                + "bytesRead=150MiB/400MiB (37.5%) bytesWritten=60MiB rate=2.5MiB/s (5m) "
                + "elapsed=0:01:00 eta=0:01:40");
    }

    @Test
    void heartbeatDoesNotCountFailedFilesAsLeftToCopy() {
        MigrationProgressTracker progress = tracker();
        progress.start(1_000, 400 * MIB, 4, 2);
        progress.fileStarted("file-1", 100 * MIB, 250);
        progress.onFileProgress("file-1", 125, 125, 10 * MIB);
        progress.fileFailed("file-1");
        progress.fileStarted("file-2", 100 * MIB, 250);
        progress.fileCompleted("file-2", 250, 250, 100 * MIB);
        advance(Duration.ofSeconds(150));

        // 200 MiB are left, of the two files not yet started; the failed file is not retried by this run.
        // Its half read is work done, so it counts in the rate (150 MiB in 150s), but not as bytes read.
        assertThat(heartbeat(progress)).isEqualTo("Progress: files=1/4 (25%) remaining=2 failed=1 inFlight=0 "
                + "records=250/1000 (25%) written=250 deleted=0 "
                + "bytesRead=100MiB/400MiB (25%) bytesWritten=100MiB rate=1MiB/s (5m) "
                + "elapsed=0:02:30 eta=0:03:20");
    }

    @Test
    void heartbeatAgreesWithTheSummaryOnBytesReadWhenAFileFailsHalfway() {
        MigrationProgressTracker progress = tracker();
        progress.start(200, 200 * MIB, 2, 1);
        progress.fileStarted("file-a", 100 * MIB, 100);
        progress.onFileProgress("file-a", 50, 50, 5 * MIB);
        progress.fileFailed("file-a");
        progress.fileStarted("file-b", 100 * MIB, 100);
        progress.fileCompleted("file-b", 100, 100, 100 * MIB);
        advance(Duration.ofSeconds(10));

        assertThat(heartbeat(progress)).contains("records=100/200 (50%)", "bytesRead=100MiB/200MiB (50%)")
                .endsWith("eta=0:00:00");
        assertThat(progress.summary()).contains("Bytes: read=100MiB/200MiB");
    }

    @Test
    void heartbeatMeasuresTheRateOverTheLastFiveMinutesOnly() {
        MigrationProgressTracker progress = tracker();
        progress.start(1_000, 1_000 * MIB, 2, 1);
        progress.fileStarted("file-1", 600 * MIB, 600);
        advance(Duration.ofSeconds(60));
        progress.fileCompleted("file-1", 600, 600, 600 * MIB);
        assertThat(heartbeat(progress)).contains("rate=10MiB/s (5m)");

        progress.fileStarted("file-2", 400 * MIB, 400);
        for (int minute = 0; minute < 10; minute++) {
            advance(Duration.ofSeconds(60));
            heartbeat(progress);
        }

        // Ten idle minutes later the fast start is out of the window: nothing is moving, so no ETA.
        advance(Duration.ofSeconds(60));
        assertThat(heartbeat(progress)).contains("rate=0B/s (5m)").endsWith("eta=unknown");
    }

    @Test
    void heartbeatReportsAnEtaOfZeroWhenNothingIsLeft() {
        MigrationProgressTracker progress = tracker();
        progress.start(100, 100 * MIB, 1, 1);
        progress.fileStarted("file-1", 100 * MIB, 100);
        advance(Duration.ofSeconds(10));
        progress.fileCompleted("file-1", 100, 100, 50 * MIB);

        assertThat(heartbeat(progress)).endsWith("elapsed=0:00:10 eta=0:00:00");
    }

    @Test
    void heartbeatOfAnEmptyRunDoesNotDivideByZero() {
        MigrationProgressTracker progress = tracker();
        progress.start(0, 0, 0, 0);

        assertThat(heartbeat(progress)).isEqualTo("Progress: files=0/0 (100%) remaining=0 failed=0 inFlight=0 "
                + "records=0/0 (100%) written=0 deleted=0 bytesRead=0B/0B (100%) bytesWritten=0B "
                + "rate=0B/s (5m) elapsed=0:00:00 eta=0:00:00");
    }

    @Test
    void logsTheHeartbeatOnATimerEvenWhileNoFileReportsProgress() throws InterruptedException {
        MigrationProgressTracker progress = new MigrationProgressTracker(
                TestProperties.bind(Map.of("migration.progress.log-every", "30ms")).progress(), snapshot -> {
                });
        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.start(100, 100 * MIB, 1, 1);
            // A file that started and then went quiet.
            progress.fileStarted("file-1", 100 * MIB, 100);

            await().atMost(Duration.ofSeconds(10))
                    .until(() -> logs.messagesStartingWith("Progress: ").size() >= 3);
            assertThat(logs.messagesStartingWith("Progress: ")).allSatisfy(line -> assertThat(line)
                    .contains("files=0/1 (0%) remaining=1 failed=0 inFlight=1 records=0/100 (0%)"));

            progress.close();
            Thread.sleep(100);
            int afterClose = logs.messagesStartingWith("Progress: ").size();
            Thread.sleep(150);
            assertThat(logs.messagesStartingWith("Progress: ")).as("the timer stops with the tracker")
                    .hasSize(afterClose);
        } finally {
            progress.close();
        }
    }

    @Test
    void handsEveryHeartbeatsProgressToTheListenerAndKeepsGoingWhenItFails() {
        List<ProgressSnapshot> heard = new CopyOnWriteArrayList<>();
        MigrationProgressTracker progress = new MigrationProgressTracker(
                TestProperties.bind(Map.of("migration.progress.log-every", "20ms")).progress(), snapshot -> {
                    heard.add(snapshot);
                    throw new IllegalStateException("state table unavailable");
                });
        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.start(100, 100 * MIB, 1, 1);
            progress.fileStarted("file-1", 100 * MIB, 100);
            progress.fileCompleted("file-1", 100, 90, 7);

            await().atMost(Duration.ofSeconds(10)).until(() -> heard.size() >= 3);
            assertThat(heard.getFirst()).isEqualTo(
                    new ProgressSnapshot(100, 100, 90, 0, 0, 100 * MIB, 100 * MIB, 7, 0, 1, 1, 0, 1, 0));
            assertThat(logs.messagesStartingWith("Heartbeat listener failed: reason=state table unavailable"))
                    .isNotEmpty();
        } finally {
            progress.close();
        }
    }

    @Test
    void noLongerLogsSeparateFileAndRecordProgressLines() {
        MigrationProgressTracker progress = tracker();
        progress.start(5_000, 5 * MIB, 2, 1);

        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.fileStarted("file-1", MIB, 1_000);
            for (long read = 256; read <= 1_000; read += 256) {
                progress.onFileProgress("file-1", read, read, read);
            }
            advance(Duration.ofSeconds(31));
            progress.onFileProgress("file-1", 1_000, 1_000, 1_000);
            progress.fileCompleted("file-1", 1_000, 1_000, 1_000);

            assertThat(logs.messages()).isEmpty();
        }
    }

    @Test
    void logsPartitionProgressAsPartitionsComplete() {
        MigrationProgressTracker progress = tracker();
        progress.start(200, 2 * MIB, 2, 120);

        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.fileCompleted("file-1", 100, 100, 100);
            progress.partitionCompleted();

            assertThat(logs.messages()).containsExactly("Partition progress: 1/120 (0.8333%)");
        }
    }

    @Test
    void summarisesAllTheMeasures() {
        MigrationProgressTracker progress = tracker();
        progress.start(200, 2 * MIB, 2, 1);
        progress.fileStarted("file-1", MIB, 100);
        progress.fileStarted("file-2", MIB, 100);
        progress.fileCompleted("file-1", 100, 95, MIB / 2);
        progress.fileCompleted("file-2", 100, 100, MIB / 2);
        progress.partitionCompleted();
        advance(Duration.ofSeconds(750));

        assertThat(progress.summary()).isEqualTo("Records: 200/200 (100%) written=195 deleted=5\n"
                + "  Files: 2/2 (100%) failed=0\n"
                + "  Partitions: 1/1 (100%)\n"
                + "  Bytes: read=2MiB/2MiB written=1MiB elapsed=0:12:30");
    }
}
