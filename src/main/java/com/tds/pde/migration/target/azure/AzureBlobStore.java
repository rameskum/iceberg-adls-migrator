package com.tds.pde.migration.target.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.StagedUpload;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * {@link BlobStore} on one Azure Blob Storage container, through the Blob API so it works without
 * hierarchical namespace: files are published by committing staged blocks, never by renaming.
 */
public final class AzureBlobStore implements BlobStore {

    /** The most blobs one List Blobs call returns. */
    private static final int LIST_PAGE_SIZE = 5_000;

    private final BlobContainerClient container;
    private final int blockSize;
    private final ExecutorService stagingThreads;

    /**
     * @param stagingThreads runs the Put Block calls of every upload, one at a time per upload
     */
    AzureBlobStore(BlobContainerClient container, int blockSize, ExecutorService stagingThreads) {
        this.container = container;
        this.blockSize = blockSize;
        this.stagingThreads = stagingThreads;
    }

    @Override
    public StagedUpload startUpload(String blobName) {
        return new AzureStagedUpload(container.getBlobClient(blobName).getBlockBlobClient(), blockSize,
                stagingThreads);
    }

    @Override
    public List<BlobInfo> list(String prefix) {
        try {
            return container.listBlobs(new ListBlobsOptions().setPrefix(prefix)
                            .setMaxResultsPerPage(LIST_PAGE_SIZE), null).stream()
                    .map(AzureBlobStore::toInfo)
                    .toList();
        } catch (BlobStorageException e) {
            // The first request of a run: turn configuration and permission problems into a readable message.
            throw new IllegalStateException("Cannot list container '" + container.getBlobContainerName() + "' of "
                    + container.getAccountUrl() + ": " + e.getStatusCode() + " " + e.getErrorCode()
                    + (e.getErrorCode() == BlobErrorCode.CONTAINER_NOT_FOUND ? " (create the container first)" : ""),
                    e);
        }
    }

    @Override
    public void delete(String blobName) {
        container.getBlobClient(blobName).deleteIfExists();
    }

    private static BlobInfo toInfo(BlobItem item) {
        Long size = item.getProperties() == null ? null : item.getProperties().getContentLength();
        return new BlobInfo(item.getName(), size == null ? 0 : size);
    }
}
