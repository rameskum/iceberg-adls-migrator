package com.tds.pde.migration.testing;

import com.tds.pde.migration.destination.BlobStoreFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Extra application source that replaces Azure with {@link #current}. Deliberately not a {@code @Configuration},
 * so component scanning never picks it up; pass it to {@code SpringApplication} explicitly. Tests run one at a
 * time, so one shared store is enough.
 */
public class InMemoryStorage {

    public static InMemoryBlobStore current = new InMemoryBlobStore();

    @Bean
    @Primary
    BlobStoreFactory inMemoryBlobStoreFactory() {
        return location -> current;
    }
}
