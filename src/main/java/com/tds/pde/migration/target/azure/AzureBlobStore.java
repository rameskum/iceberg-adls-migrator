package com.tds.pde.migration.target.azure;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.LockUnavailableException;
import com.tds.pde.migration.target.RunLock;
import com.tds.pde.migration.target.StagedUpload;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link BlobStore} on one Azure Blob Storage container, through the Blob API so it works without
 * hierarchical namespace: files are published by committing staged blocks, never by renaming.
 */
@Slf4j
public final class AzureBlobStore implements BlobStore {

    private final BlobContainerClient container;
    private final int blockSize;
    private final Duration lockRenewInterval;
    private final ExecutorService stagingThreads;

    /**
     * @param stagingThreads runs the Put Block calls of every upload, one at a time per upload
     */
    AzureBlobStore(BlobContainerClient container, int blockSize, Duration lockRenewInterval,
                   ExecutorService stagingThreads) {
        this.container = container;
        this.blockSize = blockSize;
        this.lockRenewInterval = lockRenewInterval;
        this.stagingThreads = stagingThreads;
    }

    @Override
    public StagedUpload startUpload(String blobName) {
        return new AzureStagedUpload(container.getBlobClient(blobName).getBlockBlobClient(), blockSize,
                stagingThreads);
    }

    @Override
    public Optional<Map<String, String>> metadata(String blobName) {
        try {
            return Optional.of(container.getBlobClient(blobName).getProperties().getMetadata());
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.BLOB_NOT_FOUND || e.getStatusCode() == 404) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public List<BlobInfo> list(String prefix) {
        return container.listBlobs(new ListBlobsOptions().setPrefix(prefix), null).stream()
                .map(AzureBlobStore::toInfo)
                .toList();
    }

    @Override
    public void delete(String blobName) {
        container.getBlobClient(blobName).deleteIfExists();
    }

    @Override
    public RunLock acquireLock(String blobName) {
        BlobClient blob = container.getBlobClient(blobName);
        try {
            blob.getBlockBlobClient().upload(BinaryData.fromBytes(new byte[0]), false);
        } catch (BlobStorageException e) {
            // Already there from an earlier run, possibly leased right now; the lease decides.
            if (e.getStatusCode() != 409) {
                // The first request of a run: turn configuration and permission problems into a readable message.
                throw new IllegalStateException("Cannot write to container '" + container.getBlobContainerName()
                        + "' of " + container.getAccountUrl() + ": " + e.getStatusCode() + " " + e.getErrorCode()
                        + (e.getErrorCode() == BlobErrorCode.CONTAINER_NOT_FOUND
                        ? " (create the container first)" : ""), e);
            }
        }
        BlobLeaseClient lease = new BlobLeaseClientBuilder().blobClient(blob).buildClient();
        try {
            lease.acquireLease((int) LeaseRunLock.LEASE_DURATION.toSeconds());
        } catch (BlobStorageException e) {
            if (e.getErrorCode() == BlobErrorCode.LEASE_ALREADY_PRESENT || e.getStatusCode() == 409) {
                throw new LockUnavailableException("Another migration run holds the lock " + blobName
                        + "; it expires within " + LeaseRunLock.LEASE_DURATION + " if that run has died", e);
            }
            throw e;
        }
        log.info("Run lock acquired: lock={} leaseDuration={}", blobName, LeaseRunLock.LEASE_DURATION);
        return new LeaseRunLock(lease, blobName, lockRenewInterval);
    }

    private static BlobInfo toInfo(BlobItem item) {
        Long size = item.getProperties() == null ? null : item.getProperties().getContentLength();
        return new BlobInfo(item.getName(), size == null ? 0 : size);
    }
}
