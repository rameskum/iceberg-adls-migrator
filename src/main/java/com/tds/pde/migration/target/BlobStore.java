package com.tds.pde.migration.target;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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
     * Committed blobs whose names start with the prefix, with their metadata, in name order: one listing, read a
     * page at a time as the stream is consumed, instead of a request per blob.
     */
    Stream<BlobMetadata> listWithMetadata(String prefix);

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

    record BlobMetadata(String name, Map<String, String> metadata) {
    }
}
