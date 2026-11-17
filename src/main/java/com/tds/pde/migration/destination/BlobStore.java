package com.tds.pde.migration.destination;

/**
 * Destination object storage, addressed by blob name within the destination container.
 */
public interface BlobStore {

    /**
     * Starts writing a blob whose content stays invisible to readers until {@link StagedUpload#commit} succeeds.
     */
    StagedUpload startUpload(String blobName);
}
