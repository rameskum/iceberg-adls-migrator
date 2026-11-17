package com.tds.pde.migration.destination;

import java.io.OutputStream;
import java.util.Map;

/**
 * A blob being written. Nothing becomes visible at {@link #blobName()} until {@link #commit} succeeds, so a failed
 * or interrupted write can never be mistaken for a finished file.
 */
public interface StagedUpload {

    String blobName();

    /**
     * The blob's content. Closing the stream finishes staging but publishes nothing. May be called once.
     */
    OutputStream outputStream();

    /**
     * Atomically publishes the staged content with the given metadata, replacing any existing blob.
     * Requires the output stream to be closed.
     */
    void commit(Map<String, String> metadata);

    /**
     * Discards the staged content. Safe to call after a failure and more than once; never throws.
     */
    void abort();
}
