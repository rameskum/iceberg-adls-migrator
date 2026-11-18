package com.tds.pde.migration.state;

import org.jspecify.annotations.Nullable;

/**
 * How one source file of a run ended, to be written to its file row.
 *
 * @param recordsWritten live rows published; {@code null} unless completed
 * @param error          why the file failed; {@code null} unless failed
 */
public record FileOutcome(String sourcePath, FileRecord.Status status, @Nullable Long recordsWritten,
                          @Nullable String error) {

    public static FileOutcome completed(String sourcePath, long recordsWritten) {
        return new FileOutcome(sourcePath, FileRecord.Status.COMPLETED, recordsWritten, null);
    }

    public static FileOutcome failed(String sourcePath, String error) {
        return new FileOutcome(sourcePath, FileRecord.Status.FAILED, null, error);
    }
}
