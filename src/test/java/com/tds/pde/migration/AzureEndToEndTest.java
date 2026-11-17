package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.testing.H2State;
import com.tds.pde.migration.testing.H2StateStore;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
    private final H2StateStore stateStore = new H2StateStore();
    private BlobContainerClient container;
    private Path datasets;

    @BeforeEach
    void createSourceTableDatasetsAndContainer() throws IOException {
        H2State.current = stateStore;
        container = new BlobServiceClientBuilder().connectionString(connectionString).buildClient()
                .createBlobContainer("migrator-e2e-" + UUID.randomUUID());
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build());
        IcebergTestTables.append(table, rows("EU", 0, 3000));
        IcebergTestTables.append(table, rows("US", 3000, 2000));
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("orders-src.json"), """
                { "datasetId": "orders-src", "format": "ICEBERG", "location": "%s" }
                """.formatted(table.location()));
        Files.writeString(datasets.resolve("orders-adls.json"), """
                { "datasetId": "orders-adls", "format": "PARQUET",
                  "location": "abfss://%s@%s.dfs.core.windows.net/sales/orders" }
                """.formatted(container.getBlobContainerName(), container.getAccountName()));
    }

    @AfterEach
    void deleteContainer() {
        container.delete();
    }

    @Test
    void migratesIntoBlobStorageAndCanBeResumedWithoutRework() {
        migrate();

        List<String> dataBlobs = container.listBlobs().stream().map(BlobItem::getName)
                .filter(name -> !name.contains("/_migration/")).sorted().toList();
        assertThat(dataBlobs).hasSize(2).allMatch(name -> name.startsWith("sales/orders/region="));
        long rows = dataBlobs.stream()
                .mapToLong(name -> ParquetOutput.of(container.getBlobClient(name).downloadContent().toBytes(), tempDir)
                        .standaloneRows().size())
                .sum();
        assertThat(rows).isEqualTo(5000);
        RunRecord run = repository().findLatestRunForDestination(
                "abfss://" + container.getBlobContainerName() + "@" + container.getAccountName()
                        + ".dfs.core.windows.net/sales/orders").orElseThrow();
        assertThat(run.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        var firstVersions = dataBlobs.stream()
                .map(name -> container.getBlobClient(name).getProperties().getETag()).toList();

        migrate("--migration.run-id=" + run.runId());

        assertThat(dataBlobs.stream().map(name -> container.getBlobClient(name).getProperties().getETag()).toList())
                .as("a resumed run that had published everything rewrites nothing")
                .isEqualTo(firstVersions);
    }

    private void migrate(String... extraArgs) {
        List<String> args = new java.util.ArrayList<>(List.of(
                "--migration.source.dataset-id=orders-src",
                "--migration.destination.dataset-id=orders-adls",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.run-table=" + H2StateStore.RUN_TABLE,
                "--migration.state.file-table=" + H2StateStore.FILE_TABLE,
                "--migration.azure.auth.mode=CONNECTION_STRING",
                "--migration.azure.auth.connection-string=" + connectionString,
                "--migration.state.control-table=" + H2StateStore.CONTROL_TABLE));
        args.addAll(List.of(extraArgs));
        MigrationApplication.run(new SpringApplication(MigrationApplication.class, H2State.class), args.toArray(String[]::new));
    }

    private MigrationStateRepository repository() {
        return new MigrationStateRepository(stateStore.executor(), TestProperties.defaults());
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
