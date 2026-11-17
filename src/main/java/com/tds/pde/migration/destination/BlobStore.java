package com.tds.pde.migration.destination;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Destination object storage, addressed by blob name within the destination container.
 */
public interface BlobStore {

    /**
     * Starts writing a blob whose content stays invisible to readers until {@link StagedUpload#commit} succeeds.
     */
    StagedUpload startUpload(String blobName);

    /**
     * @return the metadata of a committed blob, or empty when no blob exists under that name
     */
    Optional<Map<String, String>> metadata(String blobName);

    /**
     * Committed blobs whose names start with the prefix, in name order.
     */
    List<BlobInfo> list(String prefix);

    /**
     * Deletes a committed blob; does nothing when it does not exist.
     */
    void delete(String blobName);

    /**
     * Takes an exclusive, self-renewing lock on the named blob, creating it if needed.
     *
     * @throws LockUnavailableException when another holder has the lock
     */
    RunLock acquireLock(String blobName);

    record BlobInfo(String name, long size) {
    }
}
