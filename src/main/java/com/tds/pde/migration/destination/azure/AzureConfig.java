package com.tds.pde.migration.destination.azure;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.tds.pde.migration.config.MigrationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration(proxyBeanMethods = false)
public class AzureConfig {

    /**
     * Credential for {@code MANAGED_IDENTITY} mode: the cluster's managed identity, or the user-assigned identity
     * named by {@code migration.azure.auth.client-id}. This bean is the single place to plug in a different
     * sign-in mechanism. Lazy, so connection-string mode never builds it.
     */
    @Bean
    @Lazy
    TokenCredential azureTokenCredential(MigrationProperties properties) {
        ManagedIdentityCredentialBuilder builder = new ManagedIdentityCredentialBuilder();
        String clientId = properties.azure().auth().clientId();
        if (clientId != null) {
            builder.clientId(clientId);
        }
        return builder.build();
    }
}
