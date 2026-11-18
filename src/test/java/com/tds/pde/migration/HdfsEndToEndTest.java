package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.InMemoryStorage;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;

/**
 * Reads the source table from a real, in-process HDFS through the same shaded Hadoop client production uses,
 * with the NameNode address supplied only through {@code migration.source.hadoop.*}.
 */
class HdfsEndToEndTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "region", Types.StringType.get()));

    private static MiniDFSCluster hdfs;

    @TempDir
    Path tempDir;

    @BeforeAll
    static void startHdfs(@TempDir Path hdfsDir) throws IOException {
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.toString());
        hdfs = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
        hdfs.waitClusterUp();
    }

    @AfterAll
    static void stopHdfs() {
        if (hdfs != null) {
            hdfs.shutdown();
        }
    }

    @Test
    void migratesATableStoredOnHdfs() throws IOException {
        String nameNode = hdfs.getURI().toString();
        String tableLocation = nameNode + "/warehouse/sales/orders";
        Table table = new HadoopTables(hdfs.getConfiguration(0)).create(SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build(),
                Map.of(TableProperties.FORMAT_VERSION, "2"), tableLocation);
        append(table, "EU", 0, 1500);
        append(table, "US", 1500, 500);

        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        InMemoryStorage.current = blobStore;
        InMemoryState.Store stateStore = new InMemoryState.Store();
        InMemoryState.current = stateStore;
        Path datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01.json"), """
                { "datasetId": "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01", "format": "ICEBERG", "location": "%s" }
                """.formatted(tableLocation));
        Files.writeString(datasets.resolve("5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02.json"), """
                { "datasetId": "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02", "format": "PARQUET",
                  "location": "abfss://data@salesacct.dfs.core.windows.net/sales/orders" }
                """);

        MigrationApplication.run(new SpringApplication(MigrationApplication.class, InMemoryStorage.class, InMemoryState.class),
                "--migration.source.dataset-id=0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01",
                "--migration.target.dataset-id=5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02",
                "--migration.source.hadoop[fs.defaultFS]=" + nameNode,
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.run-table=" + "migration_run",
                "--migration.state.file-table=" + "migration_file",
                "--migration.state.control-table=" + "migration_control");

        RunRecord run = stateStore
                .findLatestRunForTarget("abfss://data@salesacct.dfs.core.windows.net/sales/orders")
                .orElseThrow();
        assertThat(run.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(run.sourceLocation()).startsWith("hdfs://");
        List<String> dataBlobs = blobStore.list("sales/orders/").stream()
                .map(blob -> blob.name()).filter(name -> !name.contains("/_migration/")).toList();
        assertThat(dataBlobs).hasSize(2);
        assertThat(dataBlobs.stream()
                .mapToLong(name -> ParquetOutput.of(blobStore.blob(name).orElseThrow().content(), tempDir).rowCount())
                .sum()).isEqualTo(2000);
    }

    private static void append(Table table, String region, long firstId, int count) throws IOException {
        List<Record> rows = LongStream.range(firstId, firstId + count).mapToObj(id -> {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", id);
            row.setField("region", region);
            return (Record) row;
        }).toList();
        PartitionKey partition = new PartitionKey(table.spec(), table.schema());
        partition.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(rows.getFirst()));
        DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(
                        table.locationProvider().newDataLocation(table.spec(), partition, UUID.randomUUID() + ".parquet")))
                .forTable(table)
                .withPartition(partition)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build();
        try (writer) {
            rows.forEach(writer::write);
        }
        DataFile file = writer.toDataFile();
        table.newAppend().appendFile(file).commit();
    }
}
