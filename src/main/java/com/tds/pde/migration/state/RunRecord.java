package com.tds.pde.migration.state;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * One migration run as stored in the run table. Everything a resume must reproduce is pinned here.
 *
 * @param snapshotId       pinned source snapshot; {@code null} when the table had never been written
 * @param sourceSchemaJson schema the source rows are read into, as Iceberg schema JSON
 * @param outputSchemaJson schema every destination file is written with, as Iceberg schema JSON
 */
public record RunRecord(String runId, Status status, String sourceDatasetId, String destinationDatasetId,
                        String sourceTableLocation, String destinationLocation, @Nullable Long snapshotId,
                        String sourceSchemaJson, String outputSchemaJson, long totalPartitions, long totalFiles,
                        long totalRecords, Instant startedAt, @Nullable Instant finishedAt, @Nullable String error) {

    public enum Status {
        RUNNING,
        SUCCEEDED,
        FAILED,
        /** Replaced by a clean start; its output was deleted. */
        SUPERSEDED
    }

    public boolean isUnfinished() {
        return status == Status.RUNNING || status == Status.FAILED;
    }
}
