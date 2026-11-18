package com.tds.pde.migration.progress;

import com.tds.pde.migration.config.MigrationProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Keeps and reports the three independent progress measures of a run: records, data files and partitions.
 *
 * <p>Counts only become final ("committed") when the work behind them is: records and files once their
 * target file is published, partitions once all their files are. Rows of the files being processed are
 * reported separately as in flight and are dropped if their file fails.
 *
 * <p>Record totals are physical source rows (before deletes), the only total Iceberg metadata gives exactly
 * without scanning; rows removed by deletes count as processed, and are reported as deleted.
 *
 * <p>Thread-safe: files copied in parallel report to one tracker.
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

    /** Cumulative physical rows read and rows written of each file being processed, by source path. */
    private final Map<String, long[]> inFlight = new HashMap<>();

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

    public synchronized void start(long totalRecords, long totalFiles, long totalPartitions) {
        this.totalRecords = totalRecords;
        this.totalFiles = totalFiles;
        this.totalPartitions = totalPartitions;
        this.lastLoggedAt = nanoClock.getAsLong();
    }

    /** A file a resumed run had already published. */
    public synchronized void fileAlreadyCompleted(long physicalRecords, long written) {
        committedRecords += physicalRecords;
        recordsWritten += written;
        completedFiles++;
        lastLoggedRecords = committedRecords;
    }

    /** A partition a resumed run had already finished. */
    public synchronized void partitionAlreadyCompleted() {
        completedPartitions++;
    }

    /** Cumulative counts of a file being processed. */
    public synchronized void onFileProgress(String sourcePath, long physicalRowsRead, long written) {
        inFlight.put(sourcePath, new long[] {physicalRowsRead, written});
        long processed = committedRecords + inFlightRecords();
        long now = nanoClock.getAsLong();
        if (processed - lastLoggedRecords >= logEveryRecords || now - lastLoggedAt >= logEveryNanos) {
            logRecordProgress(processed, now);
        }
    }

    /** A file was published: its rows become committed. */
    public synchronized void fileCompleted(String sourcePath, long physicalRowsRead, long written) {
        inFlight.remove(sourcePath);
        committedRecords += physicalRowsRead;
        recordsWritten += written;
        completedFiles++;
        log.info("File progress: {}/{} ({}%)", completedFiles, totalFiles, format(completedFiles, totalFiles));
        long now = nanoClock.getAsLong();
        if (committedRecords - lastLoggedRecords >= logEveryRecords || now - lastLoggedAt >= logEveryNanos) {
            logRecordProgress(committedRecords, now);
        }
    }

    /** A file failed: nothing it read counts. */
    public synchronized void fileFailed(String sourcePath) {
        inFlight.remove(sourcePath);
        failedFiles++;
    }

    public synchronized void partitionCompleted() {
        completedPartitions++;
        log.info("Partition progress: {}/{} ({}%)", completedPartitions, totalPartitions,
                format(completedPartitions, totalPartitions));
    }

    public synchronized ProgressSnapshot snapshot() {
        return new ProgressSnapshot(totalRecords, committedRecords, recordsWritten, inFlightRecords(),
                inFlightWritten(),
                totalFiles, completedFiles, failedFiles, totalPartitions, completedPartitions);
    }

    public synchronized String summary() {
        return "Records: " + committedRecords + "/" + totalRecords + " (" + format(committedRecords, totalRecords)
                + "%) written=" + recordsWritten + " deleted=" + (committedRecords - recordsWritten)
                + "\n  Files: " + completedFiles + "/" + totalFiles + " (" + format(completedFiles, totalFiles)
                + "%) failed=" + failedFiles
                + "\n  Partitions: " + completedPartitions + "/" + totalPartitions + " ("
                + format(completedPartitions, totalPartitions) + "%)";
    }

    private void logRecordProgress(long processed, long now) {
        log.info("Record progress: {}/{} ({}%) committed={} inFlight={} written={}", processed, totalRecords,
                format(processed, totalRecords), committedRecords, inFlightRecords(),
                recordsWritten + inFlightWritten());
        lastLoggedRecords = processed;
        lastLoggedAt = now;
    }

    private long inFlightRecords() {
        return inFlight.values().stream().mapToLong(counts -> counts[0]).sum();
    }

    private long inFlightWritten() {
        return inFlight.values().stream().mapToLong(counts -> counts[1]).sum();
    }

    /** Up to four decimals, without trailing zeros: 100, 0.83, 0.0067. */
    static String format(long completed, long total) {
        return BigDecimal.valueOf(percentage(completed, total)).setScale(4, RoundingMode.HALF_UP)
                .stripTrailingZeros().toPlainString();
    }
}
