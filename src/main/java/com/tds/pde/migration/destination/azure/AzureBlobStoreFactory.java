package com.tds.pde.migration.destination.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.destination.BlobStore;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * Opens the {@link BlobStore} for a destination location, which is only known once the destination dataset has
 * been looked up.
 */
@Slf4j
@Component
public class AzureBlobStoreFactory {

    private final MigrationProperties.Azure.Auth auth;
    private final int blockSize;
    private final ObjectProvider<TokenCredential> credential;

    public AzureBlobStoreFactory(MigrationProperties properties, ObjectProvider<TokenCredential> credential) {
        this.auth = properties.azure().auth();
        this.blockSize = Math.toIntExact(properties.destination().blockSize().toBytes());
        this.credential = credential;
    }

    public BlobStore open(AzureLocation location) {
        return open(location, LeaseRunLock.RENEW_INTERVAL);
    }

    BlobStore open(AzureLocation location, Duration lockRenewInterval) {
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
        log.info("Destination storage opened: endpoint={} container={} prefix={} authMode={}",
                service.getAccountUrl(), location.container(), location.prefix(), auth.mode());
        return new AzureBlobStore(service.getBlobContainerClient(location.container()), blockSize, lockRenewInterval);
    }
}
