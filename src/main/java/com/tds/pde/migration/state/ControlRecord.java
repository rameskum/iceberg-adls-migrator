package com.tds.pde.migration.state;

import java.time.Duration;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * The control table's row for one table migration, identified by its target dataset: the latest run, whether it is
 * still live, and its progress. A run owns the migration while the row names its run id; only the owner writes the
 * target and the row.
 *
 * @param runId        the latest run; its files are in the file table
 * @param snapshotId   source snapshot; {@code null} when the table had never been written
 * @param heartbeatAt  renewed while the run is live
 * @param finishedAt   when the run ended; {@code null} while it is {@link Status#RUNNING}
 * @param totalRecords physical source rows before deletes
 * @param error        why the run failed; {@code null} unless it did
 */
public record ControlRecord(String targetDatasetId, String sourceDatasetId, String sourceLocation,
                            String targetLocation, String runId, Status status, @Nullable Long snapshotId,
                            Instant startedAt, Instant heartbeatAt, @Nullable Instant finishedAt, long totalFiles,
                            long totalRecords, Progress progress, @Nullable String error) {

    public enum Status {
        /** Live while its heartbeat is renewed; a run that died leaves it RUNNING with a stale heartbeat. */
        RUNNING,
        COMPLETED,
        FAILED
    }

    /**
     * What a run has done so far: counts of published files only.
     *
     * @param recordsRead    physical source rows of the published files
     * @param recordsWritten live rows in the published files; {@code recordsRead} minus this were deleted
     * @param bytesWritten   size of the published target files
     */
    public record Progress(long completedFiles, long failedFiles, long recordsRead, long recordsWritten,
                           long bytesWritten) {

        public static final Progress NONE = new Progress(0, 0, 0, 0, 0);
    }

    /** Whether the run is RUNNING but has not renewed its heartbeat within {@code timeout}: it died. */
    public boolean isStale(Instant now, Duration timeout) {
        return status == Status.RUNNING && heartbeatAt.plus(timeout).isBefore(now);
    }

    /** Whether the run may still be writing the target: RUNNING with a fresh heartbeat. */
    public boolean isLive(Instant now, Duration timeout) {
        return status == Status.RUNNING && !isStale(now, timeout);
    }
}
