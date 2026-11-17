package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.dataset.DatasetLookupException;
import com.tds.pde.migration.testing.H2StateStore;
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

    private final H2StateStore stateStore = new H2StateStore();
    private Path datasets;

    @BeforeEach
    void createSourceTableAndDescriptors() throws IOException {
        InMemoryStorage.current = new InMemoryBlobStore();
        Table table = IcebergTestTables.create(tempDir.resolve("orders"),
                new Schema(required(1, "id", Types.LongType.get())), PartitionSpec.unpartitioned());
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("orders-src.json"), """
                { "datasetId": "orders-src", "format": "ICEBERG", "location": "%s" }
                """.formatted(table.location()));
        Files.writeString(datasets.resolve("orders-adls.json"), """
                { "datasetId": "orders-adls", "format": "PARQUET",
                  "location": "abfss://data@account.dfs.core.windows.net/sales/orders" }
                """);
    }

    private String[] args(String sourceDatasetId) {
        return new String[] {
                "--migration.source.dataset-id=" + sourceDatasetId,
                "--migration.destination.dataset-id=orders-adls",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.run-table=" + H2StateStore.RUN_TABLE,
                "--migration.state.file-table=" + H2StateStore.FILE_TABLE,
                "--spring.datasource.url=" + stateStore.url(),
                "--spring.datasource.driver-class-name=org.h2.Driver",
                "--spring.datasource.username=sa",
        };
    }

    @Test
    void closesTheContextAfterASuccessfulMigration() {
        AtomicBoolean closed = new AtomicBoolean();

        MigrationApplication.run(applicationTrackingClose(closed), args("orders-src"));

        assertThat(closed).isTrue();
    }

    @Test
    void closesTheContextAndRethrowsWhenTheMigrationFails() {
        AtomicBoolean closed = new AtomicBoolean();

        assertThatThrownBy(() -> MigrationApplication.run(applicationTrackingClose(closed), args("unknown-dataset")))
                .isInstanceOf(MigrationFailedException.class)
                .hasCauseInstanceOf(DatasetLookupException.class)
                .hasMessageContaining("Dataset 'unknown-dataset' not found");
        assertThat(closed).isTrue();
    }

    @Test
    void failsStartupWhenRequiredConfigurationIsMissing() {
        SpringApplication application = new SpringApplication(MigrationApplication.class);

        assertThatThrownBy(() -> MigrationApplication.run(application))
                .hasRootCauseMessage("migration.source is required");
    }

    private static SpringApplication applicationTrackingClose(AtomicBoolean closed) {
        SpringApplication application = new SpringApplication(MigrationApplication.class, InMemoryStorage.class);
        application.addListeners(event -> {
            if (event instanceof ContextClosedEvent) {
                closed.set(true);
            }
        });
        return application;
    }
}
