package com.tds.pde.migration.progress;

/**
 * Point-in-time copy of a run's progress counters.
 *
 * @param committedRecords physical source rows of published files
 * @param recordsWritten   live rows in published files
 * @param inFlightRecords  physical rows read so far from the file being processed
 */
public record ProgressSnapshot(long totalRecords, long committedRecords, long recordsWritten, long inFlightRecords,
                               long inFlightWritten, long totalFiles, long completedFiles, long failedFiles,
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
