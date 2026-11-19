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
        values.put("migration.source.dataset-id", "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01");
        values.put("migration.target.dataset-id", "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02");
        values.put("migration.datasets.stub-directory", "/tmp/datasets");
        values.put("migration.state.control-table", "migration_control");
        values.put("migration.state.file-table", "migration_file");
        values.putAll(overrides);
        return new Binder(new MapConfigurationPropertySource(values))
                .bindOrCreate("migration", MigrationProperties.class);
    }

    public static MigrationProperties defaults() {
        return bind(Map.of());
    }
}
