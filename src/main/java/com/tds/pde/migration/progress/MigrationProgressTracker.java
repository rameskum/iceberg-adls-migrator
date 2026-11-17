package com.tds.pde.migration.progress;

import com.tds.pde.migration.config.MigrationProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps and reports the three independent progress measures of a run: records, data files and partitions.
 *
 * <p>Counts only become final ("committed") when the work behind them is: records and files once their
 * target file is published, partitions once all their files are. Rows of the file being processed are
 * reported separately as in flight and are dropped if that file fails.
 *
 * <p>Record totals are physical source rows (before deletes), the only total Iceberg metadata gives exactly
 * without scanning; rows removed by deletes count as processed, and are reported as deleted.
 *
 * <p>Not thread-safe: one run thread drives it.
 */
@Slf4j
public final class MigrationProgressTracker {

    private final long logEveryRecords;
    private final long logEveryNanos;
    private final LongSupplier nanoClock;

    private long totalRecords;
    private long totalFiles;
    private long totalPartitions;

    private long committedRecords;
    private long recordsWritten;
    private long completedFiles;
    private long failedFiles;
    private long completedPartitions;

    private long inFlightRecords;
    private long inFlightWritten;

    private long lastLoggedRecords;
    private long lastLoggedAt;

    public MigrationProgressTracker(MigrationProperties.Progress settings) {
        this(settings, System::nanoTime);
    }

    MigrationProgressTracker(MigrationProperties.Progress settings, LongSupplier nanoClock) {
        this.logEveryRecords = settings.logEveryRecords();
        this.logEveryNanos = settings.logEvery().toNanos();
        this.nanoClock = nanoClock;
    }

    /** As written by the migration requirements: an empty total counts as fully done. */
    public static double percentage(long completed, long total) {
        return total == 0 ? 100.0 : (completed * 100.0) / total;
    }

    public void start(long totalRecords, long totalFiles, long totalPartitions) {
        this.totalRecords = totalRecords;
        this.totalFiles = totalFiles;
        this.totalPartitions = totalPartitions;
        this.lastLoggedAt = nanoClock.getAsLong();
    }

    /** A file a resumed run had already published. */
    public void fileAlreadyCompleted(long physicalRecords, long written) {
        committedRecords += physicalRecords;
        recordsWritten += written;
        completedFiles++;
        lastLoggedRecords = committedRecords;
    }

    /** A partition a resumed run had already finished. */
    public void partitionAlreadyCompleted() {
        completedPartitions++;
    }

    /** Cumulative counts of the file being processed. */
    public void onFileProgress(long physicalRowsRead, long written) {
        inFlightRecords = physicalRowsRead;
        inFlightWritten = written;
        long processed = committedRecords + inFlightRecords;
        long now = nanoClock.getAsLong();
        if (processed - lastLoggedRecords >= logEveryRecords || now - lastLoggedAt >= logEveryNanos) {
            logRecordProgress(processed, now);
        }
    }

    /** The current file was published: its rows become committed. */
    public void fileCompleted(long physicalRowsRead, long written) {
        inFlightRecords = 0;
        inFlightWritten = 0;
        committedRecords += physicalRowsRead;
        recordsWritten += written;
        completedFiles++;
        log.info("File progress: {}/{} ({}%)", completedFiles, totalFiles, format(completedFiles, totalFiles));
        long now = nanoClock.getAsLong();
        if (committedRecords - lastLoggedRecords >= logEveryRecords || now - lastLoggedAt >= logEveryNanos) {
            logRecordProgress(committedRecords, now);
        }
    }

    /** The current file failed: nothing it read counts. */
    public void fileFailed() {
        inFlightRecords = 0;
        inFlightWritten = 0;
        failedFiles++;
    }

    public void partitionCompleted() {
        completedPartitions++;
        log.info("Partition progress: {}/{} ({}%)", completedPartitions, totalPartitions,
                format(completedPartitions, totalPartitions));
    }

    public ProgressSnapshot snapshot() {
        return new ProgressSnapshot(totalRecords, committedRecords, recordsWritten, inFlightRecords, inFlightWritten,
                totalFiles, completedFiles, failedFiles, totalPartitions, completedPartitions);
    }

    public String summary() {
        return "Records: " + committedRecords + "/" + totalRecords + " (" + format(committedRecords, totalRecords)
                + "%) written=" + recordsWritten + " deleted=" + (committedRecords - recordsWritten)
                + "\n  Files: " + completedFiles + "/" + totalFiles + " (" + format(completedFiles, totalFiles)
                + "%) failed=" + failedFiles
                + "\n  Partitions: " + completedPartitions + "/" + totalPartitions + " ("
                + format(completedPartitions, totalPartitions) + "%)";
    }

    private void logRecordProgress(long processed, long now) {
        log.info("Record progress: {}/{} ({}%) committed={} inFlight={} written={}", processed, totalRecords,
                format(processed, totalRecords), committedRecords, inFlightRecords, recordsWritten + inFlightWritten);
        lastLoggedRecords = processed;
        lastLoggedAt = now;
    }

    /** Up to four decimals, without trailing zeros: 100, 0.83, 0.0067. */
    static String format(long completed, long total) {
        return BigDecimal.valueOf(percentage(completed, total)).setScale(4, RoundingMode.HALF_UP)
                .stripTrailingZeros().toPlainString();
    }
}
