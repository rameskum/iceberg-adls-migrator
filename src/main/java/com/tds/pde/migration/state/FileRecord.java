package com.tds.pde.migration.state;

import org.jspecify.annotations.Nullable;

/**
 * One source data file of a run, as stored in the file table.
 *
 * @param sourcePath      the data file's location; unique within a run
 * @param physicalRecords rows in the source file before deletes, from Iceberg metadata; those not written were
 *                        deleted
 * @param recordsWritten  live rows published; {@code null} until the file completes
 * @param error           why the file last failed; {@code null} once it completes
 */
public record FileRecord(String runId, String sourcePath, String targetPath, long physicalRecords, Status status,
                         @Nullable Long recordsWritten, @Nullable String error) {

    /** Only outcomes are recorded: every state write is a Delta commit, and published blobs prove completion. */
    public enum Status {
        PENDING,
        COMPLETED,
        FAILED
    }
}
