package com.tds.pde.migration.run;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.dataset.DatasetFormat;
import com.tds.pde.migration.progress.ProgressSnapshot;
import com.tds.pde.migration.schema.TargetSchemaValidator;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.StagedUpload;
import com.tds.pde.migration.target.TargetPathResolver;
import com.tds.pde.migration.target.azure.AzureLocation;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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
    private static final String TARGET_ID = "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02";
    private static final String EARLIER_RUN = "20261117-103000-ab12cd";

    @TempDir
    Path tempDir;

    private final InMemoryState.Store state = new InMemoryState.Store();
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
        source = new DatasetDescriptor("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01", DatasetFormat.ICEBERG,
                table.location(), null);
    }

    @Test
    void claimsANewMigrationAndRecordsEveryFileOfTheRun() {
        StartedRun started = start(Map.of());

        assertThat(started.runId()).matches("\\d{8}-\\d{6}-[0-9a-f]{6}");
        assertThat(started.previous()).isNull();
        assertThat(started.partitions()).hasSize(2);
        ControlRecord control = onlyControl();
        assertThat(control).isEqualTo(started.control());
        assertThat(control.status()).isEqualTo(ControlRecord.Status.RUNNING);
        assertThat(control.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
        assertThat(control.totalFiles()).isEqualTo(3);
        assertThat(control.totalRecords()).isEqualTo(6);
        assertThat(control.progress()).isEqualTo(ControlRecord.Progress.NONE);
        assertThat(control.targetLocation()).isEqualTo(target().location());
        assertThat(state.findFiles(started.runId())).as("recorded by prepare").isEmpty();

        starter(Map.of()).prepare(started, claim(started), LOCATION, blobStore);

        assertThat(state.findFiles(started.runId())).hasSize(3)
                .allSatisfy(file -> {
                    assertThat(file.status()).isEqualTo(FileRecord.Status.PENDING);
                    assertThat(file.targetPath()).startsWith("sales/orders/region=");
                });
    }

    @Test
    void refusesANewRunIntoATargetThatAlreadyHoldsFiles() throws IOException {
        publish(LOCATION.blobName("region=EU__part-0-1234abcd.parquet"));

        assertThatThrownBy(() -> start(Map.of()))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("already holds 1 files")
                .hasMessageContaining("migration.target.clean=true");
        assertThat(state.controls()).isEmpty();
    }

    @Test
    void doesNotMistakeASiblingFolderForTheTarget() throws IOException {
        publish("sales/orders-archive/part-0.parquet");

        assertThat(start(Map.of()).previous()).isNull();
    }

    @Test
    void refusesToStartAfterARunThatFailed() {
        ControlRecord failed = control(EARLIER_RUN, ControlRecord.Status.FAILED, Instant.now(), "1 of 3 files failed");
        state.putControl(failed);

        assertThatThrownBy(() -> start(Map.of()))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Run " + EARLIER_RUN + " of this migration failed (1 of 3 files failed)")
                .hasMessageContaining("Set migration.target.clean=true");
        assertThat(state.controls()).containsExactly(failed);
    }

    @Test
    void refusesToStartAfterARunThatDiedWithoutFinishing() {
        ControlRecord died = control(EARLIER_RUN, ControlRecord.Status.RUNNING, Instant.now().minusSeconds(3600),
                null);
        state.putControl(died);

        assertThatThrownBy(() -> start(Map.of()))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("stopped without finishing: its heartbeat was last renewed 1h ago")
                .hasMessageContaining("Set migration.target.clean=true");
        assertThat(state.controls()).containsExactly(died);
    }

    @Test
    void refusesToStartWhileTheLatestRunIsLive() {
        ControlRecord live = control(EARLIER_RUN, ControlRecord.Status.RUNNING, Instant.now(), null);
        state.putControl(live);

        assertThatThrownBy(() -> start(Map.of()))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Run " + EARLIER_RUN + " of this migration is still running");
        assertThat(state.controls()).containsExactly(live);
    }

    @Test
    void refusesToCleanWhileTheLatestRunsHeartbeatIsFresh() throws IOException {
        ControlRecord live = control(EARLIER_RUN, ControlRecord.Status.RUNNING, Instant.now().minusSeconds(240),
                null);
        state.putControl(live);
        publish(LOCATION.blobName("region=EU__part-0-1234abcd.parquet"));

        assertThatThrownBy(() -> start(Map.of("migration.target.clean", "true")))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("is still running: its heartbeat was renewed 4m ago")
                .hasMessageContaining("migration.target.clean=true is refused until that run's heartbeat is older "
                        + "than migration.state.heartbeat-timeout (PT5M)");
        assertThat(state.controls()).containsExactly(live);
        assertThat(blobStore.blobs()).hasSize(1);
    }

    @Test
    void cleansTheTargetAfterARunWhoseHeartbeatWentStale() throws IOException {
        ControlRecord died = control(EARLIER_RUN, ControlRecord.Status.RUNNING, Instant.now().minusSeconds(301),
                null);
        state.putControl(died);
        state.insertFiles(List.of(new FileRecord(EARLIER_RUN, "hdfs://nn/old.parquet", "sales/orders/old.parquet", 1,
                FileRecord.Status.COMPLETED, 1L, null)));
        publish(LOCATION.blobName("old.parquet"));
        publish("sales/orders-archive/keep.parquet");

        StartedRun fresh = start(Map.of("migration.target.clean", "true"));
        starter(Map.of("migration.target.clean", "true")).prepare(fresh, claim(fresh), LOCATION, blobStore);

        assertThat(fresh.runId()).isNotEqualTo(EARLIER_RUN);
        assertThat(fresh.previous()).isEqualTo(died);
        assertThat(onlyControl().runId()).isEqualTo(fresh.runId());
        assertThat(onlyControl().status()).isEqualTo(ControlRecord.Status.RUNNING);
        assertThat(state.findFiles(EARLIER_RUN)).as("file rows of the replaced run").isEmpty();
        assertThat(state.findFiles(fresh.runId())).hasSize(3);
        assertThat(blobStore.blobs().keySet()).containsExactly("sales/orders-archive/keep.parquet");
    }

    @Test
    void cleansTheTargetRightAfterARunThatFailed() throws IOException {
        state.putControl(control(EARLIER_RUN, ControlRecord.Status.FAILED, Instant.now(), "boom"));
        publish(LOCATION.blobName("old.parquet"));

        StartedRun fresh = start(Map.of("migration.target.clean", "true"));
        starter(Map.of("migration.target.clean", "true")).prepare(fresh, claim(fresh), LOCATION, blobStore);

        assertThat(onlyControl().runId()).isEqualTo(fresh.runId());
        assertThat(blobStore.blobs()).isEmpty();
    }

    @Test
    void stopsCleaningOnceAnotherRunTookTheMigrationOver() throws IOException {
        state.putControl(control(EARLIER_RUN, ControlRecord.Status.FAILED, Instant.now(), "boom"));
        List<String> names = List.of(LOCATION.blobName("a.parquet"), LOCATION.blobName("b.parquet"),
                LOCATION.blobName("c.parquet"));
        for (String name : names) {
            publish(name);
        }
        StartedRun fresh = start(Map.of("migration.target.clean", "true"));
        RunClaim claim = claim(fresh);
        ControlRecord takenOver = control(InMemoryState.Store.OTHER_RUN, ControlRecord.Status.RUNNING, Instant.now(),
                null);
        List<String> deleted = new ArrayList<>();
        BlobStore takenOverAfterFirstDelete = new BlobStore() {
            @Override
            public StagedUpload startUpload(String blobName) {
                return blobStore.startUpload(blobName);
            }

            @Override
            public List<BlobInfo> list(String prefix) {
                return blobStore.list(prefix);
            }

            @Override
            public void delete(String blobName) {
                blobStore.delete(blobName);
                deleted.add(blobName);
                state.putControl(takenOver);
                claim.renew(new ProgressSnapshot(6, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 2, 0));
            }
        };

        assertThatThrownBy(() -> starter(Map.of("migration.target.clean", "true"))
                .prepare(fresh, claim, LOCATION, takenOverAfterFirstDelete))
                .isInstanceOf(ClaimLostException.class)
                .hasMessageContaining("lost its claim on the migration");

        assertThat(deleted).containsExactly(names.getFirst());
        assertThat(blobStore.blobs().keySet()).containsExactlyInAnyOrderElementsOf(names.subList(1, 3));
        assertThat(state.findFiles(fresh.runId())).as("file rows of the run that lost the migration").isEmpty();
        assertThat(onlyControl()).isEqualTo(takenOver);
    }

    @Test
    void startsAgainAfterACompletedRunWhenTheTargetIsEmpty() {
        state.putControl(control(EARLIER_RUN, ControlRecord.Status.COMPLETED, Instant.now(), null));
        state.insertFiles(List.of(new FileRecord(EARLIER_RUN, "hdfs://nn/old.parquet", "sales/orders/old.parquet", 1,
                FileRecord.Status.COMPLETED, 1L, null)));

        StartedRun next = start(Map.of());
        starter(Map.of()).prepare(next, claim(next), LOCATION, blobStore);

        assertThat(onlyControl().runId()).isEqualTo(next.runId());
        assertThat(state.findFiles(EARLIER_RUN)).isEmpty();
    }

    @Test
    void refusesToCleanAWholeContainer() {
        AzureLocation root = AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net");

        assertThatThrownBy(() -> starter(Map.of("migration.target.clean", "true"))
                .start(source, target(), root, blobStore))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Refusing to clean");
        assertThat(state.controls()).isEmpty();
    }

    @Test
    void refusesWhenTheControlTableHoldsTwoRowsForTheTarget() {
        state.allowDuplicateInserts();
        state.insertControl(control("20261117-103000-aaaaaa", ControlRecord.Status.FAILED, Instant.now(), null));
        state.insertControl(control("20261117-103000-bbbbbb", ControlRecord.Status.FAILED, Instant.now(), null));

        assertThatThrownBy(() -> start(Map.of("migration.target.clean", "true")))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("holds 2 rows for target dataset " + TARGET_ID)
                .hasMessageContaining("20261117-103000-aaaaaa, 20261117-103000-bbbbbb");
    }

    @Test
    void ofTwoFirstRunsRacingForTheMigrationOneWins() throws Exception {
        state.raceClaims(2);

        List<Future<StartedRun>> runs = race(Map.of());

        assertOneWins(runs);
    }

    @Test
    void ofTwoRunsRacingToCleanAfterAFailedRunOneWins() throws Exception {
        state.putControl(control(EARLIER_RUN, ControlRecord.Status.FAILED, Instant.now(), null));
        state.raceClaims(2);

        List<Future<StartedRun>> runs = race(Map.of("migration.target.clean", "true"));

        assertOneWins(runs);
    }

    @Test
    void neverLetsTwoFirstRunsWinWhenBothOfTheirInsertsCommit() throws Exception {
        // Row-level concurrency does not see two inserts of a new key as a conflict, so both rows land.
        state.allowDuplicateInserts();
        state.raceClaims(2);

        List<Future<StartedRun>> runs = race(Map.of());

        List<String> winners = new ArrayList<>();
        for (Future<StartedRun> run : runs) {
            try {
                winners.add(run.get().runId());
            } catch (java.util.concurrent.ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(MigrationFailedException.class)
                        .hasMessageContaining("Another run claimed this migration at the same time");
            }
        }
        assertThat(winners).hasSizeLessThanOrEqualTo(1);
        assertThat(state.controls()).as("losers withdraw their rows").extracting(ControlRecord::runId)
                .isEqualTo(winners);
    }

    private List<Future<StartedRun>> race(Map<String, String> overrides) throws InterruptedException {
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            List<Future<StartedRun>> runs = IntStream.range(0, 2)
                    .mapToObj(i -> threads.submit(() -> start(overrides))).toList();
            threads.shutdown();
            assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
            return runs;
        } finally {
            threads.shutdownNow();
        }
    }

    private void assertOneWins(List<Future<StartedRun>> runs) throws InterruptedException {
        List<StartedRun> winners = new ArrayList<>();
        List<Throwable> losers = new ArrayList<>();
        for (Future<StartedRun> run : runs) {
            try {
                winners.add(run.get());
            } catch (java.util.concurrent.ExecutionException e) {
                losers.add(e.getCause());
            }
        }
        assertThat(winners).hasSize(1);
        assertThat(losers).singleElement().isInstanceOf(MigrationFailedException.class)
                .asString().contains("Another run claimed this migration at the same time ([" + winners.getFirst()
                        .runId() + "])");
        assertThat(onlyControl().runId()).isEqualTo(winners.getFirst().runId());
    }

    private StartedRun start(Map<String, String> overrides) {
        return starter(overrides).start(source, target(), LOCATION, blobStore);
    }

    private RunStarter starter(Map<String, String> overrides) {
        return new RunStarter(TestProperties.bind(overrides), state, new IcebergMetadataReader(new Configuration()),
                new TargetSchemaValidator(), new TargetPathResolver());
    }

    private RunClaim claim(StartedRun run) {
        return new RunClaim(state, run.control(), Duration.ofMinutes(5));
    }

    private ControlRecord onlyControl() {
        assertThat(state.controls()).hasSize(1);
        return state.controls().getFirst();
    }

    private ControlRecord control(String runId, ControlRecord.Status status, Instant heartbeatAt,
                                  String error) {
        return new ControlRecord(TARGET_ID, source.datasetId(), source.location(), target().location(), runId, status,
                null, heartbeatAt.minus(Duration.ofHours(2)), heartbeatAt,
                status == ControlRecord.Status.RUNNING ? null : heartbeatAt, 3, 6, ControlRecord.Progress.NONE,
                error);
    }

    private static DatasetDescriptor target() {
        return new DatasetDescriptor(TARGET_ID, DatasetFormat.PARQUET,
                "abfss://data@salesacct.dfs.core.windows.net/sales/orders", null);
    }

    private void publish(String blobName) throws IOException {
        var upload = blobStore.startUpload(blobName);
        try (OutputStream out = upload.outputStream()) {
            out.write(new byte[] {1, 2, 3});
        }
        upload.commit(Map.of());
    }

    private static List<Record> rows(String region, int count) {
        return IntStream.range(0, count).mapToObj(i -> {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", UUID.randomUUID());
            row.setField("region", region);
            return (Record) row;
        }).toList();
    }
}
