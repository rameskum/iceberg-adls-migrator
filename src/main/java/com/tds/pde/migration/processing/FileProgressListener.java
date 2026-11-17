package com.tds.pde.migration.processing;

/**
 * Receives in-flight progress of the file being processed. Counts are cumulative for that file and become final
 * only once its {@link FileResult} reports success.
 */
@FunctionalInterface
public interface FileProgressListener {

    FileProgressListener NONE = (physicalRowsRead, recordsWritten) -> {
    };

    void onProgress(long physicalRowsRead, long recordsWritten);
}
