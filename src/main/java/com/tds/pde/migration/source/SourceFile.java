package com.tds.pde.migration.source;

import org.apache.iceberg.FileScanTask;

/**
 * One source data file to migrate, together with the delete files that apply to it at the pinned snapshot.
 */
public record SourceFile(FileScanTask task) {

    public String location() {
        return task.file().location();
    }

    /** Physical rows in the data file, before deletes are applied. */
    public long recordCount() {
        return task.file().recordCount();
    }

    public int deleteFileCount() {
        return task.deletes().size();
    }
}
