package com.tds.pde.migration.state;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * The control table's row for one table migration (a source table copied to a destination): the source table's
 * metadata at the pinned snapshot, the latest run and its progress. Updated across runs.
 *
 * @param partitionSpecs partition specs in use at the snapshot, e.g. {@code 0:[region: identity(2)]}
 * @param totalRecords   physical source rows before deletes
 * @param totalBytes     size of the source data files
 */
public record ControlRecord(String sourceDatasetId, String destinationDatasetId, String sourceTableLocation,
                            String destinationLocation, RunRecord.Status status, String lastRunId,
                            int formatVersion, @Nullable Long snapshotId, @Nullable Instant snapshotTimestamp,
                            @Nullable Integer schemaId, String partitionSpecs, long totalPartitions, long totalFiles,
                            long totalRecords, long totalDeleteFiles, long totalBytes, long completedPartitions,
                            long completedFiles, long failedFiles, long recordsWritten, long recordsDeleted,
                            Instant startedAt, @Nullable Instant finishedAt, @Nullable String error) {
}
