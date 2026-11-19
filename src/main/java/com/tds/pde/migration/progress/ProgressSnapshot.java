package com.tds.pde.migration.progress;

/**
 * Point-in-time copy of a run's progress counters.
 *
 * @param committedRecords physical source rows of published files
 * @param recordsWritten   live rows in published files
 * @param inFlightRecords  physical rows read so far from the files being processed
 * @param totalBytes       size of all source data files
 * @param bytesRead        size of the source data files that are published, including those of earlier runs
 * @param bytesWritten     size of the target files this run published
 * @param inFlightFiles    files being processed
 */
public record ProgressSnapshot(long totalRecords, long committedRecords, long recordsWritten, long inFlightRecords,
                               long inFlightWritten, long totalBytes, long bytesRead, long bytesWritten,
                               long inFlightFiles, long totalFiles, long completedFiles, long failedFiles,
                               long totalPartitions, long completedPartitions) {

    public double recordPercentage() {
        return MigrationProgressTracker.percentage(committedRecords, totalRecords);
    }

    public double filePercentage() {
        return MigrationProgressTracker.percentage(completedFiles, totalFiles);
    }

    public double partitionPercentage() {
        return MigrationProgressTracker.percentage(completedPartitions, totalPartitions);
    }
}
