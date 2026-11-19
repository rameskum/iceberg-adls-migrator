package com.tds.pde.migration.progress;

import com.tds.pde.migration.config.MigrationProperties;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;

/**
 * Keeps and reports the independent progress measures of a run: records, bytes, data files and partitions.
 *
 * <p>Counts only become final ("committed") when the work behind them is: records, bytes and files once their
 * target file is published, partitions once all their files are. The files being processed are reported
 * separately as in flight and are dropped if their file fails.
 *
 * <p>Record totals are physical source rows (before deletes), the only total Iceberg metadata gives exactly
 * without scanning; rows removed by deletes count as processed, and are reported as deleted. Byte totals are the
 * data file sizes in Iceberg metadata; a file in flight counts for the share of its rows read so far.
 *
 * <p>While a run is going, a timer logs one {@code Progress:} line every {@code migration.progress.log-every},
 * whether or not any file reports progress, so a stalled file shows as numbers that stop moving. The rate is
 * measured over the last five minutes of this run, and the ETA extrapolates it over the source bytes still to
 * copy; neither counts files an earlier run had published.
 *
 * <p>Thread-safe: files copied in parallel report to one tracker, and the timer reads it from its own thread.
 * {@link #close()} stops the timer.
 */
@Slf4j
public final class MigrationProgressTracker implements AutoCloseable {

    /** How far back the rate looks. */
    static final Duration RATE_WINDOW = Duration.ofMinutes(5);

    private final Duration heartbeatInterval;
    private final LongSupplier nanoClock;
    private final @Nullable ScheduledExecutorService timer;

    private long totalRecords;
    private long totalBytes;
    private long totalFiles;
    private long totalPartitions;

    private long committedRecords;
    private long recordsWritten;
    private long completedFiles;
    private long failedFiles;
    private long completedPartitions;

    /** Source bytes of the files an earlier run had published. */
    private long resumedSourceBytes;
    /** Source bytes of the files this run published. */
    private long committedSourceBytes;
    /** Source bytes of the files that failed, and the share of them read before they did. */
    private long failedSourceBytes;
    private long failedSourceBytesRead;
    /** Target bytes of the files this run published; earlier runs' files are not measured. */
    private long committedBytesWritten;

    /** The files being processed, by source path. */
    private final Map<String, InFlightFile> inFlight = new HashMap<>();

    private long startedAt;
    /** Bytes processed by this run at the times the heartbeat looked, newest last; the oldest is the rate's start. */
    private final Deque<Sample> samples = new ArrayDeque<>();

    /** Logs a heartbeat on a timer of its own. */
    public MigrationProgressTracker(MigrationProperties.Progress settings) {
        this(settings, System::nanoTime, true);
    }

    /** Logs a heartbeat only when {@link #heartbeat()} is called. */
    MigrationProgressTracker(MigrationProperties.Progress settings, LongSupplier nanoClock) {
        this(settings, nanoClock, false);
    }

    private MigrationProgressTracker(MigrationProperties.Progress settings, LongSupplier nanoClock,
                                     boolean timed) {
        this.heartbeatInterval = settings.logEvery();
        this.nanoClock = nanoClock;
        this.timer = timed ? Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "progress-timer");
            thread.setDaemon(true);
            return thread;
        }) : null;
    }

    /** As written by the migration requirements: an empty total counts as fully done. */
    public static double percentage(long completed, long total) {
        return total == 0 ? 100.0 : (completed * 100.0) / total;
    }

    /** Takes the totals of the run and starts the heartbeat. */
    public synchronized void start(long totalRecords, long totalBytes, long totalFiles, long totalPartitions) {
        this.totalRecords = totalRecords;
        this.totalBytes = totalBytes;
        this.totalFiles = totalFiles;
        this.totalPartitions = totalPartitions;
        this.startedAt = nanoClock.getAsLong();
        samples.clear();
        samples.add(new Sample(startedAt, 0));
        if (timer != null) {
            long interval = heartbeatInterval.toNanos();
            timer.scheduleWithFixedDelay(this::heartbeat, interval, interval, TimeUnit.NANOSECONDS);
        }
    }

    /** A file a resumed run had already published. */
    public synchronized void fileAlreadyCompleted(long physicalRecords, long written, long sourceBytes) {
        committedRecords += physicalRecords;
        recordsWritten += written;
        resumedSourceBytes += sourceBytes;
        completedFiles++;
    }

    /** A partition a resumed run had already finished. */
    public synchronized void partitionAlreadyCompleted() {
        completedPartitions++;
    }

    /** A file is about to be processed. */
    public synchronized void fileStarted(String sourcePath, long sourceBytes, long physicalRecords) {
        inFlight.put(sourcePath, new InFlightFile(sourceBytes, physicalRecords));
    }

    /** Cumulative counts of a file being processed. */
    public synchronized void onFileProgress(String sourcePath, long physicalRowsRead, long written,
                                            long bytesWritten) {
        InFlightFile file = inFlight.computeIfAbsent(sourcePath, path -> new InFlightFile(0, 0));
        file.physicalRowsRead = physicalRowsRead;
        file.written = written;
        file.bytesWritten = bytesWritten;
    }

    /** A file was published: what it read and wrote becomes committed. */
    public synchronized void fileCompleted(String sourcePath, long physicalRowsRead, long written,
                                           long bytesWritten) {
        InFlightFile file = inFlight.remove(sourcePath);
        committedRecords += physicalRowsRead;
        recordsWritten += written;
        committedSourceBytes += file == null ? 0 : file.sourceBytes;
        committedBytesWritten += bytesWritten;
        completedFiles++;
    }

    /** A file failed: nothing it read counts, and its bytes no longer count as left to copy. */
    public synchronized void fileFailed(String sourcePath) {
        InFlightFile file = inFlight.remove(sourcePath);
        if (file != null) {
            failedSourceBytes += file.sourceBytes;
            failedSourceBytesRead += file.sourceBytesRead();
        }
        failedFiles++;
    }

    public synchronized void partitionCompleted() {
        completedPartitions++;
        log.info("Partition progress: {}/{} ({}%)", completedPartitions, totalPartitions,
                format(completedPartitions, totalPartitions));
    }

    public synchronized ProgressSnapshot snapshot() {
        return new ProgressSnapshot(totalRecords, committedRecords, recordsWritten, inFlightRecords(),
                inFlightWritten(), totalBytes, resumedSourceBytes + committedSourceBytes,
                committedBytesWritten, inFlight.size(),
                totalFiles, completedFiles, failedFiles, totalPartitions, completedPartitions);
    }

    /** Logs one {@code Progress:} line; called by the timer. */
    synchronized void heartbeat() {
        long now = nanoClock.getAsLong();
        long processedBytes = processedSourceBytes();
        samples.add(new Sample(now, processedBytes));
        long windowStart = now - RATE_WINDOW.toNanos();
        while (samples.size() > 2 && samples.peekFirst().at < windowStart) {
            samples.removeFirst();
        }
        Sample from = samples.peekFirst();
        double rate = now == from.at ? 0 : (processedBytes - from.bytes) / ((now - from.at) / 1e9);

        long readRecords = committedRecords + inFlightRecords();
        long writtenRecords = recordsWritten + inFlightWritten();
        long readBytes = resumedSourceBytes + processedBytes;
        // Failed files are not copied again by this run, so they are not left to copy.
        long remainingBytes = Math.max(0, totalBytes - readBytes - (failedSourceBytes - failedSourceBytesRead));
        log.info("Progress: files={}/{} ({}%) remaining={} failed={} inFlight={} "
                        + "records={}/{} ({}%) written={} deleted={} "
                        + "bytesRead={}/{} ({}%) bytesWritten={} rate={}/s (5m) elapsed={} eta={}",
                completedFiles, totalFiles, format(completedFiles, totalFiles),
                Math.max(0, totalFiles - completedFiles - failedFiles), failedFiles, inFlight.size(),
                readRecords, totalRecords, format(readRecords, totalRecords), writtenRecords,
                readRecords - writtenRecords,
                formatBytes(readBytes), formatBytes(totalBytes), format(readBytes, totalBytes),
                formatBytes(committedBytesWritten + inFlightBytesWritten()), formatBytes(Math.round(rate)),
                formatDuration(Duration.ofNanos(now - startedAt)), eta(remainingBytes, rate));
    }

    public synchronized String summary() {
        return "Records: " + committedRecords + "/" + totalRecords + " (" + format(committedRecords, totalRecords)
                + "%) written=" + recordsWritten + " deleted=" + (committedRecords - recordsWritten)
                + "\n  Files: " + completedFiles + "/" + totalFiles + " (" + format(completedFiles, totalFiles)
                + "%) failed=" + failedFiles
                + "\n  Partitions: " + completedPartitions + "/" + totalPartitions + " ("
                + format(completedPartitions, totalPartitions) + "%)"
                + "\n  Bytes: read=" + formatBytes(resumedSourceBytes + committedSourceBytes) + "/"
                + formatBytes(totalBytes) + " written=" + formatBytes(committedBytesWritten)
                + " elapsed=" + formatDuration(Duration.ofNanos(nanoClock.getAsLong() - startedAt));
    }

    /** Stops the heartbeat. */
    @Override
    public void close() {
        if (timer != null) {
            timer.shutdownNow();
        }
    }

    /** Source bytes this run has read: its published files, and the share read of the other files. */
    private long processedSourceBytes() {
        long bytes = committedSourceBytes + failedSourceBytesRead;
        for (InFlightFile file : inFlight.values()) {
            bytes += file.sourceBytesRead();
        }
        return bytes;
    }

    private long inFlightRecords() {
        return inFlight.values().stream().mapToLong(file -> file.physicalRowsRead).sum();
    }

    private long inFlightWritten() {
        return inFlight.values().stream().mapToLong(file -> file.written).sum();
    }

    private long inFlightBytesWritten() {
        return inFlight.values().stream().mapToLong(file -> file.bytesWritten).sum();
    }

    private static String eta(long remainingBytes, double bytesPerSecond) {
        if (remainingBytes == 0) {
            return formatDuration(Duration.ZERO);
        }
        return bytesPerSecond <= 0 ? "unknown" : formatDuration(Duration.ofSeconds(
                Math.round(remainingBytes / bytesPerSecond)));
    }

    /** Up to four decimals, without trailing zeros: 100, 0.83, 0.0067. */
    static String format(long completed, long total) {
        return BigDecimal.valueOf(percentage(completed, total)).setScale(4, RoundingMode.HALF_UP)
                .stripTrailingZeros().toPlainString();
    }

    /** Binary units with one decimal where it matters: 512B, 1.5KiB, 12MiB, 1.2GiB. */
    static String formatBytes(long bytes) {
        String[] units = {"B", "KiB", "MiB", "GiB", "TiB", "PiB"};
        double value = bytes;
        int unit = 0;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        if (unit == 0) {
            return bytes + "B";
        }
        return new BigDecimal(value).setScale(1, RoundingMode.HALF_UP).stripTrailingZeros().toPlainString()
                + units[unit];
    }

    /** Hours may exceed a day: 0:12:30, 36:05:10. */
    static String formatDuration(Duration duration) {
        long seconds = duration.toSeconds();
        return String.format(Locale.ROOT, "%d:%02d:%02d", seconds / 3600, seconds / 60 % 60, seconds % 60);
    }

    private record Sample(long at, long bytes) {
    }

    private static final class InFlightFile {

        final long sourceBytes;
        final long sourceRecords;
        long physicalRowsRead;
        long written;
        long bytesWritten;

        InFlightFile(long sourceBytes, long sourceRecords) {
            this.sourceBytes = sourceBytes;
            this.sourceRecords = sourceRecords;
        }

        /** The file's bytes in proportion to its rows read, as Parquet gives no byte position of the reader. */
        long sourceBytesRead() {
            return sourceRecords == 0 ? 0
                    : (long) (sourceBytes * (Math.min(physicalRowsRead, sourceRecords) / (double) sourceRecords));
        }
    }
}
