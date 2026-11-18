package com.tds.pde.migration.source;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
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

    /** The SHA-256 of a file location, in hex. */
    public static String keyOf(String location) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(location.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
    }
}
