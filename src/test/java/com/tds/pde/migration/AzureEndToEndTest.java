package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

/**
 * The whole application against a real Blob API through the production Azure adapter. Enabled by
 * {@code AZURE_STORAGE_TEST_CONNECTION_STRING}, e.g. for Azurite.
 */
@EnabledIfEnvironmentVariable(named = "AZURE_STORAGE_TEST_CONNECTION_STRING", matches = ".+")
class AzureEndToEndTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "region", Types.StringType.get()));

    @TempDir
    Path tempDir;

    private final String connectionString = System.getenv("AZURE_STORAGE_TEST_CONNECTION_STRING");
    private final InMemoryState.Store stateStore = new InMemoryState.Store();
    private BlobContainerClient container;
    private Path datasets;

    @BeforeEach
    void createSourceTableDatasetsAndContainer() throws IOException {
        InMemoryState.current = stateStore;
        container = new BlobServiceClientBuilder().connectionString(connectionString).buildClient()
                .createBlobContainer("migrator-e2e-" + UUID.randomUUID());
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build());
        IcebergTestTables.append(table, rows("EU", 0, 3000));
        IcebergTestTables.append(table, rows("US", 3000, 2000));
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01.json"), """
                { "datasetId": "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01", "format": "ICEBERG", "location": "%s" }
                """.formatted(table.location()));
        Files.writeString(datasets.resolve("5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02.json"), """
                { "datasetId": "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02", "format": "PARQUET",
                  "location": "abfss://%s@%s.dfs.core.windows.net/sales/orders" }
                """.formatted(container.getBlobContainerName(), container.getAccountName()));
    }

    @AfterEach
    void deleteContainer() {
        container.delete();
    }

    @Test
    void migratesIntoBlobStorageAndStartsOverWhenAskedToClean() {
        migrate();

        List<String> dataBlobs = container.listBlobs().stream().map(BlobItem::getName).sorted().toList();
        assertThat(dataBlobs).hasSize(2).allMatch(name -> name.startsWith("sales/orders/region="));
        long rows = dataBlobs.stream()
                .mapToLong(name -> ParquetOutput.of(container.getBlobClient(name).downloadContent().toBytes(), tempDir)
                        .standaloneRows().size())
                .sum();
        assertThat(rows).isEqualTo(5000);
        ControlRecord first = control();
        assertThat(first.status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(first.targetLocation()).isEqualTo("abfss://" + container.getBlobContainerName() + "@"
                + container.getAccountName() + ".dfs.core.windows.net/sales/orders");
        var firstVersions = dataBlobs.stream()
                .map(name -> container.getBlobClient(name).getProperties().getETag()).toList();

        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("already holds 2 files written by run " + first.runId());

        migrate("--migration.target.clean=true");

        assertThat(control().runId()).isNotEqualTo(first.runId());
        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(container.listBlobs().stream().map(BlobItem::getName).sorted().toList()).isEqualTo(dataBlobs);
        assertThat(dataBlobs.stream().map(name -> container.getBlobClient(name).getProperties().getETag()).toList())
                .as("deleted and copied again from the beginning")
                .doesNotContainAnyElementsOf(firstVersions);
    }

    private ControlRecord control() {
        List<ControlRecord> controls = stateStore.findControls("5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02");
        assertThat(controls).hasSize(1);
        return controls.getFirst();
    }

    private void migrate(String... extraArgs) {
        List<String> args = new java.util.ArrayList<>(List.of(
                "--migration.source.dataset-id=0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01",
                "--migration.target.dataset-id=5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.file-table=" + "migration_file",
                "--migration.azure.auth.mode=CONNECTION_STRING",
                "--migration.state.control-table=" + "migration_control"));
        args.addAll(List.of(extraArgs));
        SpringApplication application = new SpringApplication(MigrationApplication.class, InMemoryState.class);
        application.setEnvironment(environmentWithConnectionString());
        MigrationApplication.run(application, args.toArray(String[]::new));
    }

    /** The connection string set as a secret is on Databricks: in the environment variable named for it. */
    private StandardEnvironment environmentWithConnectionString() {
        StandardEnvironment environment = new StandardEnvironment();
        Map<String, Object> variables = new HashMap<>(environment.getSystemEnvironment());
        variables.put("MIGRATION_AZURE_AUTH_CONNECTIONSTRING", connectionString);
        environment.getPropertySources().replace(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        variables));
        return environment;
    }

    private static List<Record> rows(String region, long firstId, int count) {
        return LongStream.range(firstId, firstId + count).mapToObj(id -> {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", id);
            row.setField("region", region);
            return (Record) row;
        }).toList();
    }
}
