package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.dataset.DatasetLookupException;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.InMemoryStorage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.context.event.ContextClosedEvent;

/** The application lifecycle the Databricks launcher relies on: the context always closes and failures propagate. */
class MigrationApplicationTest {

    @TempDir
    Path tempDir;

    private final InMemoryState.Store stateStore = new InMemoryState.Store();
    private Path datasets;

    @BeforeEach
    void createSourceTableAndDescriptors() throws IOException {
        InMemoryStorage.current = new InMemoryBlobStore();
        InMemoryState.current = stateStore;
        Table table = IcebergTestTables.create(tempDir.resolve("orders"),
                new Schema(required(1, "id", Types.LongType.get())), PartitionSpec.unpartitioned());
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01.json"), """
                { "datasetId": "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01", "format": "ICEBERG", "location": "%s" }
                """.formatted(table.location()));
        Files.writeString(datasets.resolve("5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02.json"), """
                { "datasetId": "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02", "format": "PARQUET",
                  "location": "abfss://data@account.dfs.core.windows.net/sales/orders" }
                """);
    }

    private String[] args(String sourceDatasetId) {
        return new String[] {
                "--migration.source.dataset-id=" + sourceDatasetId,
                "--migration.target.dataset-id=5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.run-table=" + "migration_run",
                "--migration.state.file-table=" + "migration_file",
                "--migration.state.control-table=" + "migration_control",
        };
    }

    @Test
    void closesTheContextAfterASuccessfulMigration() {
        AtomicBoolean closed = new AtomicBoolean();

        MigrationApplication.run(applicationTrackingClose(closed), args("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01"));

        assertThat(closed).isTrue();
    }

    @Test
    void closesTheContextAndRethrowsWhenTheMigrationFails() {
        AtomicBoolean closed = new AtomicBoolean();

        String unknownDataset = "9d3c2b1a-0f6e-4d5c-8b7a-6e5f4d3c2b1a";

        assertThatThrownBy(() -> MigrationApplication.run(applicationTrackingClose(closed), args(unknownDataset)))
                .isInstanceOf(MigrationFailedException.class)
                .hasCauseInstanceOf(DatasetLookupException.class)
                .hasMessageContaining("Dataset '" + unknownDataset + "' not found");
        assertThat(closed).isTrue();
    }

    @Test
    void rejectsAMisspelledMigrationParameterBeforeAnythingRuns() {
        String[] args = java.util.stream.Stream.concat(java.util.Arrays.stream(args("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01")),
                java.util.stream.Stream.of("--migration.run-ld=20261117-103000-ab12cd")).toArray(String[]::new);

        assertThatThrownBy(() -> MigrationApplication.run(applicationTrackingClose(new AtomicBoolean()), args))
                .hasStackTraceContaining("migration.run-ld");
    }

    @Test
    void rejectsParametersOutsideTheKnownPrefixes() {
        assertThatThrownBy(() -> MigrationApplication.run(applicationTrackingClose(new AtomicBoolean()),
                "--migraton.run-id=20261117-103000-ab12cd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Unknown job parameter '--migraton.run-id=20261117-103000-ab12cd'");
    }

    @Test
    void rejectsADatasetIdThatIsNotAUuid() {
        assertThatThrownBy(() -> MigrationApplication.run(applicationTrackingClose(new AtomicBoolean()),
                args("orders")))
                .hasStackTraceContaining("migration.source.dataset-id");
    }

    @Test
    void failsStartupWhenRequiredConfigurationIsMissing() {
        SpringApplication application = new SpringApplication(MigrationApplication.class);

        assertThatThrownBy(() -> MigrationApplication.run(application))
                .hasRootCauseMessage("migration.source is required");
    }

    private static SpringApplication applicationTrackingClose(AtomicBoolean closed) {
        SpringApplication application = new SpringApplication(MigrationApplication.class, InMemoryStorage.class, InMemoryState.class);
        application.addListeners(event -> {
            if (event instanceof ContextClosedEvent) {
                closed.set(true);
            }
        });
        return application;
    }
}
