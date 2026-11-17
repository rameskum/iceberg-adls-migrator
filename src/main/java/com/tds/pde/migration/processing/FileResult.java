package com.tds.pde.migration.processing;

import java.time.Duration;
import org.jspecify.annotations.Nullable;

/**
 * Outcome of one {@link FileTask}. A file counts as completed only when {@link #succeeded()} is true, which
 * means its destination blob was published.
 *
 * @param physicalRowsRead rows read from the source file before deletes
 * @param recordsWritten   live rows written to the destination file
 * @param error            why the file failed; {@code null} when it succeeded
 */
public record FileResult(FileTask task, boolean succeeded, long physicalRowsRead, long recordsWritten,
                         Duration duration, @Nullable Exception error) {

    static FileResult succeeded(FileTask task, long physicalRowsRead, long recordsWritten, Duration duration) {
        return new FileResult(task, true, physicalRowsRead, recordsWritten, duration, null);
    }

    static FileResult failed(FileTask task, long physicalRowsRead, long recordsWritten, Duration duration,
                             Exception error) {
        return new FileResult(task, false, physicalRowsRead, recordsWritten, duration, error);
    }

    /** Rows dropped because a delete file removed them. */
    public long recordsDeleted() {
        return physicalRowsRead - recordsWritten;
    }
}
