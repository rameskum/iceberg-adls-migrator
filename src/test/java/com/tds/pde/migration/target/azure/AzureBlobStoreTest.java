package com.tds.pde.migration.target.azure;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.TargetPathResolver;
import com.tds.pde.migration.target.ParquetDataWriter;
import com.tds.pde.migration.target.StagedUpload;
import com.tds.pde.migration.processing.DataFileProcessor;
import com.tds.pde.migration.processing.FileProgressListener;
import com.tds.pde.migration.processing.FileResult;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.processing.RunContext;
import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.source.SourceFile;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
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

/**
 * Runs the Azure adapter against a real Blob API: Azurite ({@code npx azurite}) or a storage account.
 * Set {@code AZURE_STORAGE_TEST_CONNECTION_STRING} to enable it; each test uses its own container.
 */
@EnabledIfEnvironmentVariable(named = AzureBlobStoreTest.CONNECTION_STRING_VARIABLE, matches = ".+")
class AzureBlobStoreTest {

    static final String CONNECTION_STRING_VARIABLE = "AZURE_STORAGE_TEST_CONNECTION_STRING";

    private static final int BLOCK_SIZE = 1024;

    @TempDir
    Path tempDir;

    private BlobContainerClient container;
    private AzureLocation location;
    private AzureBlobStoreFactory factory;

    @BeforeEach
    void createContainer() {
        String connectionString = System.getenv(CONNECTION_STRING_VARIABLE);
        container = new BlobServiceClientBuilder().connectionString(connectionString).buildClient()
                .createBlobContainer("migrator-test-" + UUID.randomUUID());
        location = AzureLocation.parse("abfss://" + container.getBlobContainerName()
                + "@" + container.getAccountName() + ".dfs.core.windows.net/sales/orders");
        factory = new AzureBlobStoreFactory(TestProperties.bind(Map.of(
                "migration.azure.auth.mode", "CONNECTION_STRING",
                "migration.azure.auth.connection-string", connectionString,
                "migration.target.block-size", BLOCK_SIZE + "B")), null);
    }

    @AfterEach
    void deleteContainer() {
        container.delete();
    }

    @Test
    void keepsStagedBlocksInvisibleUntilTheCommitPublishesThemWithMetadata() throws IOException {
        BlobStore store = factory.open(location);
        String name = location.blobName("part.parquet");
        byte[] content = randomBytes(BLOCK_SIZE * 3 + 300);

        StagedUpload upload = store.startUpload(name);
        try (OutputStream out = upload.outputStream()) {
            out.write(content, 0, 100);
            out.write(content, 100, content.length - 100);
        }

        assertThat(metadata(store, name)).isEmpty();
        assertThat(store.list(location.prefix())).isEmpty();

        upload.commit(Map.of("records_written", "42"));

        assertThat(metadata(store, name)).contains(Map.of("records_written", "42"));
        assertThat(container.getBlobClient(name).downloadContent().toBytes()).isEqualTo(content);
        assertThat(container.getBlobClient(name).getBlockBlobClient().listBlocks(
                com.azure.storage.blob.models.BlockListType.COMMITTED).getCommittedBlocks()).hasSize(4);
        assertThat(store.list(location.prefix())).containsExactly(new BlobStore.BlobInfo(name, content.length));
    }

    @Test
    void listsTheCommittedBlobsUnderAPrefix() throws IOException {
        BlobStore store = factory.open(location);
        String first = location.blobName("region=EU__part-0.parquet");
        String second = location.blobName("region=US__part-1.parquet");
        upload(store, second, randomBytes(10), Map.of("migration_run_id", "run-1", "source_file", "hdfs%3A%2F%2Fnn"));
        upload(store, first, randomBytes(10), Map.of());
        upload(store, "sales/orders-archive/part-0.parquet", randomBytes(10), Map.of("migration_run_id", "run-0"));
        StagedUpload staged = store.startUpload(location.blobName("region=EU__part-2.parquet"));
        try (OutputStream out = staged.outputStream()) {
            out.write(randomBytes(BLOCK_SIZE * 2));
        }

        assertThat(store.list(location.blobPrefix())).containsExactly(
                new BlobStore.BlobInfo(first, 10), new BlobStore.BlobInfo(second, 10));
    }

    @Test
    void explainsAMissingContainer() {
        BlobStore store = factory.open(AzureLocation.parse("abfss://missing-" + UUID.randomUUID()
                + "@devstoreaccount1.dfs.core.windows.net/sales/orders"));

        assertThatThrownBy(() -> store.list("sales/orders/"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot list container 'missing-")
                .hasMessageContaining("ContainerNotFound (create the container first)");
    }

    @Test
    void replacesAnExistingBlobOnCommit() throws IOException {
        BlobStore store = factory.open(location);
        String name = location.blobName("part.parquet");
        upload(store, name, randomBytes(5000), Map.of("attempt", "1"));
        byte[] second = randomBytes(700);

        upload(store, name, second, Map.of("attempt", "2"));

        assertThat(container.getBlobClient(name).downloadContent().toBytes()).isEqualTo(second);
        assertThat(metadata(store, name)).contains(Map.of("attempt", "2"));
    }

    @Test
    void leavesNothingBehindWhenAnUploadIsAbandoned() throws IOException {
        BlobStore store = factory.open(location);
        String name = location.blobName("part.parquet");

        StagedUpload upload = store.startUpload(name);
        try (OutputStream out = upload.outputStream()) {
            out.write(randomBytes(BLOCK_SIZE * 2));
        }
        upload.abort();

        assertThat(metadata(store, name)).isEmpty();
        assertThat(store.list("")).isEmpty();
    }

    @Test
    void refusesToCommitBeforeTheStreamIsClosed() throws IOException {
        StagedUpload upload = factory.open(location).startUpload(location.blobName("part.parquet"));
        upload.outputStream().write(1);

        assertThatThrownBy(() -> upload.commit(Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before its output stream is closed");
    }

    @Test
    void deletesBlobsAndIgnoresMissingOnes() throws IOException {
        BlobStore store = factory.open(location);
        String name = location.blobName("part.parquet");
        upload(store, name, randomBytes(10), Map.of());

        store.delete(name);
        store.delete(name);

        assertThat(metadata(store, name)).isEmpty();
    }

    @Test
    void publishesAMigratedDataFileReadableAsStandaloneParquet() {
        Schema schema = new Schema(required(1, "id", Types.LongType.get()), required(2, "note", Types.StringType.get()));
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), schema, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, LongStream.range(0, 5000).mapToObj(id -> {
            GenericRecord row = GenericRecord.create(schema);
            row.setField("id", id);
            row.setField("note", "order-" + id);
            return (Record) row;
        }).toList());
        MigrationPlan plan = new IcebergMetadataReader(new Configuration()).plan(table.location(), null);
        SourceFile source = plan.partitions().getFirst().files().getFirst();
        FileTask task = new FileTask(source, "", new TargetPathResolver().resolve(location.prefix(), "",
                source.location()));
        BlobStore store = factory.open(location);
        DataFileProcessor processor = new DataFileProcessor(
                new IcebergDataReader(), new ParquetDataWriter(TestProperties.defaults()), store);

        FileResult result = processor.process(new RunContext("run-1", plan.table(), OutputSchema.unchanged(schema)), task,
                FileProgressListener.NONE);

        assertThat(result.succeeded()).isTrue();
        assertThat(metadata(store, task.target()).orElseThrow()).containsEntry("records_written", "5000");
        ParquetOutput output = ParquetOutput.of(container.getBlobClient(task.target()).downloadContent().toBytes(),
                tempDir);
        assertThat(output.standaloneRows()).hasSize(5000);
        assertThat(output.records(schema).getLast().getField("note")).isEqualTo("order-4999");
    }

    /** The metadata of the named blob, if it is committed. */
    private Optional<Map<String, String>> metadata(BlobStore store, String name) {
        BlobClient blob = container.getBlobClient(name);
        return blob.exists() ? Optional.of(blob.getProperties().getMetadata()) : Optional.empty();
    }

    private static void upload(BlobStore store, String name, byte[] content, Map<String, String> metadata)
            throws IOException {
        StagedUpload upload = store.startUpload(name);
        try (OutputStream out = upload.outputStream()) {
            out.write(content);
        }
        upload.commit(metadata);
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        new Random(length).nextBytes(bytes);
        return bytes;
    }
}
