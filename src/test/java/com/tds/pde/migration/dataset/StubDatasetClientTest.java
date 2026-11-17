package com.tds.pde.migration.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.config.MigrationProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

class StubDatasetClientTest {

    @TempDir
    Path directory;

    private StubDatasetClient client() {
        MigrationProperties properties = new Binder(new MapConfigurationPropertySource(Map.of(
                "migration.source.dataset-id", "orders-src",
                "migration.destination.dataset-id", "orders-adls",
                "migration.datasets.stub-directory", directory.toString(),
                "migration.state.run-table", "migration_run",
                "migration.state.file-table", "migration_file")))
                .bindOrCreate("migration", MigrationProperties.class);
        return new StubDatasetClient(properties);
    }

    @Test
    void readsADescriptorWithItsSchema() throws IOException {
        Files.writeString(directory.resolve("orders-adls.json"), """
                {
                  "datasetId": "orders-adls",
                  "format": "PARQUET",
                  "location": "abfss://data@account.dfs.core.windows.net/sales/orders",
                  "owner": "sales-team",
                  "schema": { "fields": [
                    { "name": "order_id", "type": "long", "nullable": false },
                    { "name": "amount", "type": "decimal(18,2)" }
                  ] }
                }
                """);

        DatasetDescriptor descriptor = client().getDataset("orders-adls");

        assertThat(descriptor.format()).isEqualTo(DatasetFormat.PARQUET);
        assertThat(descriptor.location()).isEqualTo("abfss://data@account.dfs.core.windows.net/sales/orders");
        assertThat(descriptor.schema().fields()).containsExactly(
                new DatasetSchema.Field("order_id", "long", false),
                new DatasetSchema.Field("amount", "decimal(18,2)", true));
    }

    @Test
    void readsADescriptorWithoutASchema() throws IOException {
        Files.writeString(directory.resolve("orders-src.json"), """
                { "datasetId": "orders-src", "format": "ICEBERG", "location": "hdfs://nn/warehouse/orders", "schema": null }
                """);

        DatasetDescriptor descriptor = client().getDataset("orders-src");

        assertThat(descriptor.format()).isEqualTo(DatasetFormat.ICEBERG);
        assertThat(descriptor.schema()).isNull();
    }

    @Test
    void reportsAMissingDataset() {
        assertThatThrownBy(() -> client().getDataset("unknown"))
                .isInstanceOf(DatasetLookupException.class)
                .hasMessageStartingWith("Dataset 'unknown' not found");
    }

    @Test
    void rejectsIdsThatCouldEscapeTheStubDirectory() {
        assertThatThrownBy(() -> client().getDataset("../secrets"))
                .isInstanceOf(DatasetLookupException.class)
                .hasMessage("Invalid dataset id '../secrets'");
    }

    @Test
    void rejectsADescriptorForAnotherDataset() throws IOException {
        Files.writeString(directory.resolve("orders-src.json"), """
                { "datasetId": "customers", "format": "ICEBERG", "location": "hdfs://nn/warehouse/customers" }
                """);

        assertThatThrownBy(() -> client().getDataset("orders-src"))
                .isInstanceOf(DatasetLookupException.class)
                .hasMessageContaining("is for dataset 'customers', expected 'orders-src'");
    }

    @Test
    void rejectsADescriptorMissingRequiredFields() throws IOException {
        Files.writeString(directory.resolve("orders-src.json"), """
                { "datasetId": "orders-src", "format": "ICEBERG" }
                """);

        assertThatThrownBy(() -> client().getDataset("orders-src"))
                .isInstanceOf(DatasetLookupException.class)
                .hasMessageStartingWith("Invalid descriptor for dataset 'orders-src'")
                .rootCause()
                .hasMessage("location is required for dataset orders-src");
    }
}
