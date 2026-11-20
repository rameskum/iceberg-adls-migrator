package com.tds.pde.migration.target;

import java.util.List;

/**
 * Target object storage, addressed by blob name within the target container.
 */
public interface BlobStore {

    /**
     * Starts writing a blob whose content stays invisible to readers until {@link StagedUpload#commit} succeeds.
     *
     * @param runId the run writing the blob, which names any temporary file the upload needs
     */
    StagedUpload startUpload(String blobName, String runId);

    /**
     * Files whose names start with the prefix, in name order: committed blobs, and any temporary file an upload that
     * did not finish left there.
     */
    List<BlobInfo> list(String prefix);

    /**
     * Deletes a file; does nothing when it does not exist.
     */
    void delete(String blobName);

    /**
     * Removes what uploads leave under the folder besides the published files, such as the folder that held their
     * temporary files, with any temporary file still in it. A clean calls it after deleting every file under the
     * folder, and a run that published every file calls it before it records its completion. Does nothing by
     * default: Azure block uploads leave no folders, and their uncommitted blocks expire on their own.
     *
     * @param prefix the folder, ending with a slash, or empty for the container root
     */
    default void deleteUploadLeftovers(String prefix) {
    }

    record BlobInfo(String name, long size) {
    }
}
