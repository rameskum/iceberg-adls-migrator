package com.tds.pde.migration.state;

import org.jspecify.annotations.Nullable;

/**
 * The control table's row for one table migration, identified by its source and target datasets: the source table
 * at the pinned snapshot and the latest run's status. Every run of the migration carries the same dataset pair;
 * {@code lastRunId} is the latest of them. Progress is in the file table.
 *
 * @param totalRecords physical source rows before deletes
 */
public record ControlRecord(String sourceDatasetId, String targetDatasetId, RunRecord.Status status,
                            String lastRunId, @Nullable Long snapshotId, long totalFiles, long totalRecords) {
}
