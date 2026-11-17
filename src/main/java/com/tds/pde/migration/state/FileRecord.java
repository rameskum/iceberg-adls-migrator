package com.tds.pde.migration.state;

import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * One source data file of a run, as stored in the file table.
 *
 * @param fileKey         SHA-256 of the source location; unique within a run
 * @param physicalRecords rows in the source file before deletes, from Iceberg metadata
 * @param recordsWritten  live rows published; {@code null} until the file completes
 * @param recordsDeleted  rows dropped by deletes; {@code null} until the file completes
 */
public record FileRecord(String runId, String fileKey, String sourcePath, int specId, String partitionPath,
                         String targetPath, long physicalRecords, Status status, @Nullable Long recordsWritten,
                         @Nullable Long recordsDeleted, int attempts, @Nullable String lastError, Instant updatedAt) {

    /** Only outcomes are recorded: every state write is a Delta commit, and published blobs prove completion. */
    public enum Status {
        PENDING,
        COMPLETED,
        FAILED
    }
}
