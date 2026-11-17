package com.tds.pde.migration.destination.azure;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.tds.pde.migration.destination.BlobStore;
import com.tds.pde.migration.destination.DestinationPathResolver;
import com.tds.pde.migration.destination.LockUnavailableException;
import com.tds.pde.migration.destination.ParquetDataWriter;
import com.tds.pde.migration.destination.RunLock;
import com.tds.pde.migration.destination.StagedUpload;
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
import java.time.Duration;
import java.util.Map;
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
                "migration.destination.block-size", BLOCK_SIZE + "B")), null);
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

        assertThat(store.metadata(name)).isEmpty();
        assertThat(store.list(location.prefix())).isEmpty();

        upload.commit(Map.of("records_written", "42"));

        assertThat(store.metadata(name)).contains(Map.of("records_written", "42"));
        assertThat(container.getBlobClient(name).downloadContent().toBytes()).isEqualTo(content);
        assertThat(container.getBlobClient(name).getBlockBlobClient().listBlocks(
                com.azure.storage.blob.models.BlockListType.COMMITTED).getCommittedBlocks()).hasSize(4);
        assertThat(store.list(location.prefix())).containsExactly(new BlobStore.BlobInfo(name, content.length));
    }

    @Test
    void replacesAnExistingBlobOnCommit() throws IOException {
        BlobStore store = factory.open(location);
        String name = location.blobName("part.parquet");
        upload(store, name, randomBytes(5000), Map.of("attempt", "1"));
        byte[] second = randomBytes(700);

        upload(store, name, second, Map.of("attempt", "2"));

        assertThat(container.getBlobClient(name).downloadContent().toBytes()).isEqualTo(second);
        assertThat(store.metadata(name)).contains(Map.of("attempt", "2"));
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

        assertThat(store.metadata(name)).isEmpty();
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

        assertThat(store.metadata(name)).isEmpty();
    }

    @Test
    void grantsTheLockToOneHolderAtATime() {
        BlobStore store = factory.open(location);
        String lockName = location.blobName("_migration/lock");

        RunLock lock = store.acquireLock(lockName);
        assertThatThrownBy(() -> store.acquireLock(lockName))
                .isInstanceOf(LockUnavailableException.class)
                .hasMessageContaining("Another migration run holds the lock");

        lock.close();
        try (RunLock again = store.acquireLock(lockName)) {
            again.ensureHeld();
        }
    }

    @Test
    void keepsRenewingTheLockAndReportsWhenItIsTakenAway() throws InterruptedException {
        BlobStore store = factory.open(location, Duration.ofMillis(200));
        String lockName = location.blobName("_migration/lock");

        try (RunLock lock = store.acquireLock(lockName)) {
            Thread.sleep(700);
            lock.ensureHeld();

            new BlobLeaseClientBuilder().blobClient(container.getBlobClient(lockName)).buildClient()
                    .breakLease();
            Thread.sleep(700);

            assertThatThrownBy(lock::ensureHeld)
                    .isInstanceOf(LockUnavailableException.class)
                    .hasMessageContaining("Lost the run lock");
        }
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
        FileTask task = new FileTask(source, "", new DestinationPathResolver().resolve(location.prefix(), "",
                source.location()));
        BlobStore store = factory.open(location);
        DataFileProcessor processor = new DataFileProcessor(
                new IcebergDataReader(), new ParquetDataWriter(TestProperties.defaults()), store);

        FileResult result = processor.process(new RunContext(plan.table(), OutputSchema.unchanged(schema)), task,
                FileProgressListener.NONE);

        assertThat(result.succeeded()).isTrue();
        assertThat(store.metadata(task.destination()).orElseThrow()).containsEntry("records_written", "5000");
        ParquetOutput output = ParquetOutput.of(container.getBlobClient(task.destination()).downloadContent().toBytes(),
                tempDir);
        assertThat(output.standaloneRows()).hasSize(5000);
        assertThat(output.records(schema).getLast().getField("note")).isEqualTo("order-4999");
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
