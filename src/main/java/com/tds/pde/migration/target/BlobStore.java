package com.tds.pde.migration.target;

import java.util.List;

/**
 * Target object storage, addressed by blob name within the target container.
 */
public interface BlobStore {

    /**
     * Starts writing a blob whose content stays invisible to readers until {@link StagedUpload#commit} succeeds.
     */
    StagedUpload startUpload(String blobName);

    /**
     * Committed blobs whose names start with the prefix, in name order.
     */
    List<BlobInfo> list(String prefix);

    /**
     * Deletes a committed blob; does nothing when it does not exist.
     */
    void delete(String blobName);

    record BlobInfo(String name, long size) {
    }
}
