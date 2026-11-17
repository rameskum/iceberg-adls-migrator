package com.tds.pde.migration.testing;

import com.tds.pde.migration.config.MigrationProperties;
import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/** Binds {@link MigrationProperties} the way Spring Boot does, from the required values plus overrides. */
public final class TestProperties {

    private TestProperties() {
    }

    public static MigrationProperties bind(Map<String, String> overrides) {
        Map<String, String> values = new HashMap<>();
        values.put("migration.source.dataset-id", "orders-src");
        values.put("migration.destination.dataset-id", "orders-adls");
        values.put("migration.datasets.stub-directory", "/tmp/datasets");
        values.put("migration.state.run-table", "migration_run");
        values.put("migration.state.file-table", "migration_file");
        values.putAll(overrides);
        return new Binder(new MapConfigurationPropertySource(values))
                .bindOrCreate("migration", MigrationProperties.class);
    }

    public static MigrationProperties defaults() {
        return bind(Map.of());
    }
}
