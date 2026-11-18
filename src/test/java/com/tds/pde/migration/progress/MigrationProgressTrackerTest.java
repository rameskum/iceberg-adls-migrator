package com.tds.pde.migration.progress;

import static org.assertj.core.api.Assertions.assertThat;

import com.tds.pde.migration.testing.LogCapture;
import com.tds.pde.migration.testing.TestProperties;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class MigrationProgressTrackerTest {

    private final AtomicLong clock = new AtomicLong();

    private MigrationProgressTracker tracker(long logEveryRecords, Duration logEvery) {
        return new MigrationProgressTracker(TestProperties.bind(Map.of(
                "migration.progress.log-every-records", String.valueOf(logEveryRecords),
                "migration.progress.log-every", logEvery.toMillis() + "ms")).progress(), clock::get);
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
    void commitsRecordsOnlyWhenTheirFileIsPublished() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofHours(1));
        progress.start(300, 3, 2);

        progress.onFileProgress("file-1", 80, 70);
        assertThat(progress.snapshot().committedRecords()).isZero();
        assertThat(progress.snapshot().inFlightRecords()).isEqualTo(80);

        progress.fileCompleted("file-1", 100, 90);
        ProgressSnapshot afterFirst = progress.snapshot();
        assertThat(afterFirst.committedRecords()).isEqualTo(100);
        assertThat(afterFirst.recordsWritten()).isEqualTo(90);
        assertThat(afterFirst.inFlightRecords()).isZero();
        assertThat(afterFirst.completedFiles()).isEqualTo(1);
        assertThat(afterFirst.completedPartitions()).isZero();
    }

    @Test
    void addsUpTheInFlightRowsOfFilesProcessedInParallel() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofHours(1));
        progress.start(300, 3, 2);

        progress.onFileProgress("file-1", 80, 70);
        progress.onFileProgress("file-2", 30, 30);
        progress.onFileProgress("file-1", 90, 80);
        assertThat(progress.snapshot().inFlightRecords()).isEqualTo(120);
        assertThat(progress.snapshot().inFlightWritten()).isEqualTo(110);

        progress.fileCompleted("file-1", 100, 90);
        assertThat(progress.snapshot().inFlightRecords()).as("only file-2 is still in flight").isEqualTo(30);
        assertThat(progress.snapshot().committedRecords()).isEqualTo(100);
    }

    @Test
    void discardsTheInFlightRowsOfAFailedFile() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofHours(1));
        progress.start(300, 3, 2);

        progress.onFileProgress("file-1", 60, 60);
        progress.fileFailed("file-1");

        ProgressSnapshot snapshot = progress.snapshot();
        assertThat(snapshot.committedRecords()).isZero();
        assertThat(snapshot.inFlightRecords()).isZero();
        assertThat(snapshot.completedFiles()).isZero();
        assertThat(snapshot.failedFiles()).isEqualTo(1);
    }

    @Test
    void countsWorkAResumedRunHadAlreadyDone() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofHours(1));
        progress.start(300, 3, 2);

        progress.fileAlreadyCompleted(100, 100);
        progress.partitionAlreadyCompleted();

        assertThat(progress.snapshot()).isEqualTo(
                new ProgressSnapshot(300, 100, 100, 0, 0, 3, 1, 0, 2, 1));
        assertThat(progress.snapshot().partitionPercentage()).isEqualTo(50.0);
    }

    @Test
    void logsRecordProgressEveryConfiguredNumberOfRecords() {
        MigrationProgressTracker progress = tracker(1_000, Duration.ofHours(1));
        progress.start(5_000, 1, 1);

        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            for (long read = 256; read <= 5_000; read += 256) {
                progress.onFileProgress("file-1", read, read);
            }
            assertThat(logs.messagesStartingWith("Record progress: ")).containsExactly(
                    "Record progress: 1024/5000 (20.48%) committed=0 inFlight=1024 written=1024",
                    "Record progress: 2048/5000 (40.96%) committed=0 inFlight=2048 written=2048",
                    "Record progress: 3072/5000 (61.44%) committed=0 inFlight=3072 written=3072",
                    "Record progress: 4096/5000 (81.92%) committed=0 inFlight=4096 written=4096");
        }
    }

    @Test
    void logsRecordProgressAfterTheConfiguredInterval() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofSeconds(30));
        progress.start(5_000, 1, 1);

        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.onFileProgress("file-1", 100, 100);
            clock.addAndGet(Duration.ofSeconds(31).toNanos());
            progress.onFileProgress("file-1", 200, 200);
            progress.onFileProgress("file-1", 300, 300);

            assertThat(logs.messagesStartingWith("Record progress: ")).containsExactly(
                    "Record progress: 200/5000 (4%) committed=0 inFlight=200 written=200");
        }
    }

    @Test
    void logsFileAndPartitionProgressAsTheyComplete() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofHours(1));
        progress.start(200, 2, 120);

        try (LogCapture logs = new LogCapture(MigrationProgressTracker.class.getName())) {
            progress.fileCompleted("file-1", 100, 100);
            progress.partitionCompleted();

            assertThat(logs.messages())
                    .containsExactly("File progress: 1/2 (50%)", "Partition progress: 1/120 (0.8333%)");
        }
    }

    @Test
    void summarisesAllThreeMeasures() {
        MigrationProgressTracker progress = tracker(1_000_000, Duration.ofHours(1));
        progress.start(200, 2, 1);
        progress.fileCompleted("file-1", 100, 95);
        progress.fileCompleted("file-2", 100, 100);
        progress.partitionCompleted();

        assertThat(progress.summary()).isEqualTo("Records: 200/200 (100%) written=195 deleted=5\n"
                + "  Files: 2/2 (100%) failed=0\n"
                + "  Partitions: 1/1 (100%)");
    }
}
