package com.tds.pde.migration.target;

import com.azure.core.credential.TokenCredential;
import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.target.azure.AzureBlobStoreFactory;
import com.tds.pde.migration.target.hadoop.HadoopBlobStoreFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class TargetConfig {

    /**
     * The target storage client chosen by {@code migration.target.client}. Only the Azure SDK client signs in with
     * {@code migration.azure.auth}; the Hadoop client uses the cluster's own access to the storage.
     */
    @Bean
    BlobStoreFactory blobStoreFactory(MigrationProperties properties, ObjectProvider<TokenCredential> credential) {
        return switch (properties.target().client()) {
            case AZURE_SDK -> new AzureBlobStoreFactory(properties, credential);
            case HADOOP -> new HadoopBlobStoreFactory();
        };
    }
}
