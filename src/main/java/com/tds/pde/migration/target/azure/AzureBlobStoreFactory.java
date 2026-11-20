package com.tds.pde.migration.target.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.BlobStoreFactory;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Opens Azure Blob Storage for a target location, through the Azure Blob SDK.
 */
@Slf4j
public class AzureBlobStoreFactory implements BlobStoreFactory {

    private final MigrationProperties.Azure.Auth auth;
    private final int blockSize;
    private final ObjectProvider<TokenCredential> credential;
    /**
     * Runs the Put Block calls so that writing continues meanwhile. Threads are created on demand and, being one
     * per upload with a block in flight, never outnumber the files copied in parallel.
     */
    private final ExecutorService stagingThreads = Executors.newCachedThreadPool(new ThreadFactory() {
        private final AtomicInteger number = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            // Short enough for the log pattern's 15-character thread column.
            Thread thread = new Thread(runnable, "block-stager-" + number.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    });

    public AzureBlobStoreFactory(MigrationProperties properties, ObjectProvider<TokenCredential> credential) {
        this.auth = properties.azure().auth();
        this.blockSize = Math.toIntExact(properties.target().blockSize().toBytes());
        this.credential = credential;
    }

    @Override
    public BlobStore open(AzureLocation location) {
        BlobServiceClient service = switch (auth.mode()) {
            case MANAGED_IDENTITY -> new BlobServiceClientBuilder()
                    .endpoint(location.blobEndpoint())
                    .credential(credential.getObject())
                    .buildClient();
            // The connection string names the endpoint itself, e.g. Azurite's http://127.0.0.1:10000/<account>.
            case CONNECTION_STRING -> new BlobServiceClientBuilder()
                    .connectionString(auth.connectionString())
                    .buildClient();
        };
        log.info("Target storage opened: client=azure-sdk endpoint={} container={} prefix={} authMode={}",
                service.getAccountUrl(), location.container(), location.prefix(), auth.mode());
        return new AzureBlobStore(service.getBlobContainerClient(location.container()), blockSize, stagingThreads);
    }

    @PreDestroy
    void close() {
        stagingThreads.shutdownNow();
    }
}
