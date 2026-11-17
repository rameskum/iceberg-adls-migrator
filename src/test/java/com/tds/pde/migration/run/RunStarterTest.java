package com.tds.pde.migration.run;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.dataset.DatasetFormat;
import com.tds.pde.migration.dataset.DatasetSchema;
import com.tds.pde.migration.destination.DestinationPathResolver;
import com.tds.pde.migration.destination.azure.AzureLocation;
import com.tds.pde.migration.schema.DestinationSchemaValidator;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.testing.H2StateStore;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RunStarterTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.UUIDType.get()),
            optional(2, "region", Types.StringType.get()));
    private static final AzureLocation LOCATION =
            AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net/sales/orders");
    private static final String LOCK = LOCATION.blobName("_migration/lock");

    @TempDir
    Path tempDir;

    private final H2StateStore stateStore = new H2StateStore();
    private final MigrationStateRepository state =
            new MigrationStateRepository(stateStore.jdbc(), TestProperties.defaults());
    private final InMemoryBlobStore blobStore = new InMemoryBlobStore();
    private Table table;
    private DatasetDescriptor source;

    @BeforeEach
    void createSourceTable() {
        table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build());
        IcebergTestTables.append(table, rows("EU", 2));
        IcebergTestTables.append(table, rows("EU", 1));
        IcebergTestTables.append(table, rows("US", 3));
        source = new DatasetDescriptor("orders-src", DatasetFormat.ICEBERG, table.location(), null);
    }

    @Test
    void startsANewRunIntoAnEmptyDestinationAndRecordsEveryFile() {
        StartedRun started = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);

        assertThat(started.resumed()).isFalse();
        assertThat(started.runId()).matches("\\d{8}-\\d{6}-[0-9a-f]{6}");
        assertThat(started.partitions()).hasSize(2);
        RunRecord run = state.findRun(started.runId()).orElseThrow();
        assertThat(run.status()).isEqualTo(RunRecord.Status.RUNNING);
        assertThat(run.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
        assertThat(run.totalFiles()).isEqualTo(3);
        assertThat(run.totalRecords()).isEqualTo(6);
        assertThat(state.findFiles(started.runId())).hasSize(3)
                .allSatisfy(file -> {
                    assertThat(file.status()).isEqualTo(FileRecord.Status.PENDING);
                    assertThat(file.destinationPath()).startsWith("sales/orders/region=");
                });
        assertThat(started.context().runId()).isEqualTo(started.runId());
    }

    @Test
    void ignoresTheRunLockWhenCheckingThatTheDestinationIsEmpty() {
        blobStore.acquireLock(LOCK);

        assertThat(starter(Map.of()).start(source, destination(null), LOCATION, blobStore).resumed()).isFalse();
    }

    @Test
    void refusesANewRunIntoADestinationThatAlreadyHoldsFiles() throws IOException {
        StartedRun earlier = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        state.updateRunStatus(earlier.runId(), RunRecord.Status.SUCCEEDED, null, null);
        publish(LOCATION.blobName("region=EU__part-0-1234abcd.parquet"));

        assertThatThrownBy(() -> starter(Map.of()).start(source, destination(null), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("already holds 1 files written by run " + earlier.runId() + " (SUCCEEDED)")
                .hasMessageContaining("migration.destination.clean=true");
    }

    @Test
    void doesNotMistakeASiblingFolderForTheDestination() throws IOException {
        publish("sales/orders-archive/part-0.parquet");

        assertThat(starter(Map.of()).start(source, destination(null), LOCATION, blobStore).resumed()).isFalse();
    }

    @Test
    void resumesTheLatestUnfinishedRunWithItsPinnedSnapshotAndSchema() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        state.updateRunStatus(first.runId(), RunRecord.Status.FAILED, null, "boom");
        IcebergTestTables.append(table, rows("APAC", 5));
        table.updateSchema().renameColumn("region", "market").commit();

        StartedRun resumed = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);

        assertThat(resumed.resumed()).isTrue();
        assertThat(resumed.runId()).isEqualTo(first.runId());
        assertThat(resumed.context().source().snapshotId()).isEqualTo(first.context().source().snapshotId());
        assertThat(resumed.plan().totalFiles()).isEqualTo(3);
        assertThat(resumed.context().source().schema().findField("region")).isNotNull();
        assertThat(state.findRun(first.runId()).orElseThrow().status()).isEqualTo(RunRecord.Status.RUNNING);
        assertThat(resumed.fileStates()).hasSize(3);
    }

    @Test
    void resumesTheRequestedRun() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        state.updateRunStatus(first.runId(), RunRecord.Status.SUCCEEDED, null, null);

        StartedRun resumed = starter(Map.of("migration.run-id", first.runId()))
                .start(source, destination(null), LOCATION, blobStore);

        assertThat(resumed.resumed()).isTrue();
        assertThat(resumed.runId()).isEqualTo(first.runId());
    }

    @Test
    void rejectsAnUnknownRun() {
        assertThatThrownBy(() -> starter(Map.of("migration.run-id", "nope"))
                .start(source, destination(null), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessage("Run nope does not exist");
    }

    @Test
    void rejectsARunOfAnotherDestination() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        DatasetDescriptor elsewhere = new DatasetDescriptor("orders-adls", DatasetFormat.PARQUET,
                "abfss://data@salesacct.dfs.core.windows.net/elsewhere", null);

        assertThatThrownBy(() -> starter(Map.of("migration.run-id", first.runId()))
                .start(source, elsewhere, LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("not " + table.location() + " to abfss://data@salesacct.dfs.core.windows.net/elsewhere");
    }

    @Test
    void refusesToResumeWhenTheDestinationSchemaChanged() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        state.updateRunStatus(first.runId(), RunRecord.Status.FAILED, null, null);
        DatasetSchema nowConvertsUuid = new DatasetSchema(List.of(
                new DatasetSchema.Field("id", "string", false), new DatasetSchema.Field("region", "string", true)));

        assertThatThrownBy(() -> starter(Map.of()).start(source, destination(nowConvertsUuid), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("destination schema changed since run " + first.runId());
    }

    @Test
    void refusesToResumeWhenThePinnedSnapshotNoLongerMatchesTheRecordedFiles() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        state.updateRunStatus(first.runId(), RunRecord.Status.FAILED, null, null);
        stateStore.jdbc().sql("UPDATE migration_file SET physical_records = 99 WHERE run_id = ?")
                .param(first.runId()).update();

        assertThatThrownBy(() -> starter(Map.of()).start(source, destination(null), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("no longer yields the files run " + first.runId() + " recorded")
                .hasMessageContaining("changed: ");
    }

    @Test
    void refusesToResumeWithADifferentConfiguredSnapshot() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);

        assertThatThrownBy(() -> starter(Map.of("migration.run-id", first.runId(),
                "migration.source.snapshot-id", "123")).start(source, destination(null), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("is pinned to snapshot");
    }

    @Test
    void cleansTheDestinationAndSupersedesEarlierRuns() throws IOException {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        publish(LOCATION.blobName("region=EU__part-0-1234abcd.parquet"));
        publish("sales/orders-archive/keep.parquet");
        blobStore.acquireLock(LOCK);

        StartedRun fresh = starter(Map.of("migration.destination.clean", "true"))
                .start(source, destination(null), LOCATION, blobStore);

        assertThat(fresh.resumed()).isFalse();
        assertThat(fresh.runId()).isNotEqualTo(first.runId());
        assertThat(state.findRun(first.runId()).orElseThrow().status()).isEqualTo(RunRecord.Status.SUPERSEDED);
        assertThat(blobStore.blobs().keySet()).containsExactlyInAnyOrder(LOCK, "sales/orders-archive/keep.parquet");
    }

    @Test
    void refusesToCleanAWholeContainer() {
        AzureLocation root = AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net");

        assertThatThrownBy(() -> starter(Map.of("migration.destination.clean", "true"))
                .start(source, destination(null), root, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Refusing to clean");
    }

    @Test
    void refusesToCombineCleanWithARunId() {
        assertThatThrownBy(() -> starter(Map.of("migration.destination.clean", "true", "migration.run-id", "r"))
                .start(source, destination(null), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("cannot be combined with migration.run-id=r");
    }

    @Test
    void refusesToResumeASupersededRun() {
        StartedRun first = starter(Map.of()).start(source, destination(null), LOCATION, blobStore);
        state.supersedeRuns(destination(null).location());

        assertThatThrownBy(() -> starter(Map.of("migration.run-id", first.runId()))
                .start(source, destination(null), LOCATION, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("was superseded");
    }

    private RunStarter starter(Map<String, String> overrides) {
        return new RunStarter(TestProperties.bind(overrides), state, new IcebergMetadataReader(new Configuration()),
                new DestinationSchemaValidator(), new DestinationPathResolver());
    }

    private static DatasetDescriptor destination(DatasetSchema schema) {
        return new DatasetDescriptor("orders-adls", DatasetFormat.PARQUET,
                "abfss://data@salesacct.dfs.core.windows.net/sales/orders", schema);
    }

    private void publish(String blobName) throws IOException {
        var upload = blobStore.startUpload(blobName);
        try (OutputStream out = upload.outputStream()) {
            out.write(new byte[] {1, 2, 3});
        }
        upload.commit(Map.of());
    }

    private static List<Record> rows(String region, int count) {
        return java.util.stream.IntStream.range(0, count).mapToObj(i -> {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", UUID.randomUUID());
            row.setField("region", region);
            return (Record) row;
        }).toList();
    }
}
