package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.InMemoryStorage;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * Runs the whole application, as the Databricks launcher does, against real Iceberg tables, an in-memory state store
 * and the in-memory blob store.
 */
@ExtendWith(OutputCaptureExtension.class)
class MigrationEndToEndTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "region", Types.StringType.get()));
    private static final String PREFIX = "sales/orders";
    private static final String SOURCE_ID = "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01";
    private static final String TARGET_ID = "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02";
    private static final String TARGET_LOCATION = "abfss://data@salesacct.dfs.core.windows.net/" + PREFIX;
    private static final String EARLIER_RUN = "20261117-103000-ab12cd";

    private final InMemoryBlobStore blobStore = new InMemoryBlobStore();

    @TempDir
    Path tempDir;

    private final InMemoryState.Store stateStore = new InMemoryState.Store();
    private Table table;
    private Path datasets;

    @BeforeEach
    void createSourceTableAndDatasets() throws IOException {
        InMemoryStorage.current = blobStore;
        InMemoryState.current = stateStore;
        table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build());
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01.json"), """
                { "datasetId": "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01", "format": "ICEBERG", "location": "%s" }
                """.formatted(table.location()));
        Files.writeString(datasets.resolve("5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02.json"), """
                { "datasetId": "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02", "format": "PARQUET",
                  "location": "abfss://data@salesacct.dfs.core.windows.net/%s",
                  "schema": { "fields": [ { "name": "id", "type": "long", "nullable": false },
                                          { "name": "region", "type": "string" } ] } }
                """.formatted(PREFIX));
    }

    @Test
    void migratesEveryFileAppliesDeletesAndReportsCompletion(CapturedOutput output) throws IOException {
        appendFiles();
        DataFile us = IcebergTestTables.append(table, rows("US", 300, 4));
        table.newRowDelta().addDeletes(positionDeletes(us, 0, 1)).commit();

        migrate();

        assertThat(output.getOut())
                .contains("Total partitions: 3\n  Total data files: 4\n  Total records: 9")
                .contains("Partition progress: 1/3 (33.3333%)", "Partition progress: 2/3 (66.6667%)",
                        "Partition progress: 3/3 (100%)")
                .doesNotContain("File progress:", "Record progress:")
                .contains("Migration completed: runId=")
                .contains("Records: 9/9 (100%) written=7 deleted=2\n  Files: 4/4 (100%) failed=0\n"
                        + "  Partitions: 3/3 (100%)\n  Bytes: read=");

        ControlRecord control = control();
        assertThat(control.status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(control.finishedAt()).isNotNull();
        assertThat(control.error()).isNull();
        assertThat(control.sourceDatasetId()).isEqualTo(SOURCE_ID);
        assertThat(control.targetLocation()).isEqualTo(TARGET_LOCATION);
        assertThat(control.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
        assertThat(control.totalFiles()).isEqualTo(4);
        assertThat(control.totalRecords()).isEqualTo(9);
        long bytesInOutput = dataBlobs().stream().mapToLong(name -> blobStore.blob(name).orElseThrow().content().length)
                .sum();
        assertThat(control.progress()).isEqualTo(new ControlRecord.Progress(4, 0, 9, 7, bytesInOutput));

        List<FileRecord> files = files();
        assertThat(files).hasSize(4).allSatisfy(file -> {
            assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED);
            assertThat(file.error()).isNull();
        });
        assertThat(files).extracting(file -> file.physicalRecords() - file.recordsWritten())
                .as("deleted rows").containsExactlyInAnyOrder(0L, 0L, 0L, 2L);
        assertThat(dataBlobs()).hasSize(4);
        long rowsInOutput = dataBlobs().stream()
                .mapToLong(name -> ParquetOutput.of(blobStore.blob(name).orElseThrow().content(), tempDir)
                        .standaloneRows().size())
                .sum();
        assertThat(rowsInOutput).isEqualTo(7);
    }

    @Test
    void completesATableThatWasNeverWritten(CapturedOutput output) {
        migrate();

        assertThat(output.getOut()).contains("Migration completed: runId=")
                .contains("Records: 0/0 (100%)", "Files: 0/0 (100%)", "Partitions: 0/0 (100%)");
        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(dataBlobs()).isEmpty();
    }

    @Test
    void copiesFilesInParallel() {
        appendFiles();
        blobStore.holdCommitsUntilConcurrent(3);

        migrate("--migration.processing.parallel-files=3");

        assertThat(blobStore.maxConcurrentUploads()).isEqualTo(3);
        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(files()).extracting(FileRecord::status).containsOnly(FileRecord.Status.COMPLETED);
        assertThat(dataBlobs()).hasSize(3);
    }

    @Test
    void logsTheProgressHeartbeatWhileAFileIsStalledAndStopsItWhenTheRunEnds(CapturedOutput output)
            throws Exception {
        appendFiles();
        CountDownLatch release = new CountDownLatch(1);
        blobStore.holdCommitsUntil(release);
        ExecutorService runner = Executors.newSingleThreadExecutor();
        try {
            Future<?> run = runner.submit(() -> migrate("--migration.processing.parallel-files=1",
                    "--migration.progress.log-every=50ms"));

            // No file finishes, so nothing else reports progress; the heartbeat still does.
            await().atMost(Duration.ofSeconds(10)).until(() -> countOf(output, "Progress: files=0/3 (0%) "
                    + "remaining=3 failed=0 inFlight=1 ") >= 2);
            release.countDown();
            run.get(30, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            runner.shutdownNow();
        }

        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(output.getOut()).contains("Progress: files=").contains("bytesRead=", "rate=", "eta=");
        int lines = countOf(output, "Progress: ");
        int renewals = stateStore.renewals().size();
        Thread.sleep(200);
        assertThat(countOf(output, "Progress: ")).as("the heartbeat stops with the run").isEqualTo(lines);
        assertThat(stateStore.renewals()).as("and so do the renewals").hasSize(renewals);
    }

    @Test
    void writesTheProgressToTheControlRowWithEveryHeartbeat() throws Exception {
        appendFiles();
        // The APAC file is copied first and published; the EU files wait.
        CountDownLatch release = new CountDownLatch(1);
        blobStore.holdCommitsTo(name -> name.contains("region=EU"), release);
        ExecutorService runner = Executors.newSingleThreadExecutor();
        try {
            Future<?> run = runner.submit(() -> migrate("--migration.processing.parallel-files=1",
                    "--migration.progress.log-every=50ms"));

            // The run claims its control row on another thread, so the row may not exist yet.
            await().atMost(Duration.ofSeconds(10)).until(() -> stateStore.findControls(TARGET_ID).stream()
                    .anyMatch(row -> row.progress().completedFiles() == 1));
            ControlRecord running = control();
            assertThat(running.status()).isEqualTo(ControlRecord.Status.RUNNING);
            assertThat(running.progress().recordsRead()).isEqualTo(1);
            assertThat(running.progress().recordsWritten()).isEqualTo(1);
            assertThat(running.progress().bytesWritten()).isPositive();
            Instant heartbeat = running.heartbeatAt();
            await().atMost(Duration.ofSeconds(10)).until(() -> control().heartbeatAt().isAfter(heartbeat));

            release.countDown();
            run.get(30, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            runner.shutdownNow();
        }

        assertThat(stateStore.renewals()).extracting(ControlRecord.Progress::completedFiles)
                .as("renewed while files were copied").contains(1L).isSorted();
        assertThat(control().progress().completedFiles()).isEqualTo(3);
        assertThat(control().progress().recordsRead()).isEqualTo(5);
    }

    private static int countOf(CapturedOutput output, String text) {
        return output.getOut().split(Pattern.quote(text), -1).length - 1;
    }

    @Test
    void recordsFileOutcomesInBatches() {
        appendFiles();

        migrate("--migration.state.outcome-batch-size=2", "--migration.state.outcome-flush-interval=1h");

        assertThat(stateStore.outcomeWrites()).as("a full batch, then the rest at the end").containsExactly(2, 1);
        assertThat(files()).extracting(FileRecord::status).containsOnly(FileRecord.Status.COMPLETED);
    }

    @Test
    void recordsAnOutcomeAtOnceWhenTheFlushIntervalHasPassed() {
        appendFiles();

        migrate("--migration.state.outcome-flush-interval=0s");

        assertThat(stateStore.outcomeWrites()).containsExactly(1, 1, 1);
    }

    @Test
    void stopsAtTheFirstFailedFileByDefault(CapturedOutput output) {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU"));

        // One file at a time, so that no other file is already being copied when the first one fails.
        assertThatThrownBy(() -> migrate("--migration.processing.parallel-files=1"))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("failed: 1 of 3 files failed")
                .hasMessageContaining("start again from the beginning with migration.target.clean=true");

        ControlRecord control = control();
        assertThat(control.status()).isEqualTo(ControlRecord.Status.FAILED);
        assertThat(control.error()).isEqualTo("1 of 3 files failed");
        assertThat(control.finishedAt()).isNotNull();
        assertThat(control.progress().completedFiles()).isEqualTo(1);
        assertThat(control.progress().failedFiles()).isEqualTo(1);
        assertThat(statusByPartition()).isEqualTo(Map.of(
                "region=APAC", List.of(FileRecord.Status.COMPLETED),
                "region=EU", List.of(FileRecord.Status.FAILED, FileRecord.Status.PENDING)));
        // Only two partitions exist; with fail-fast the second EU file was never started.
        assertThat(output.getOut()).contains("Stopped starting files after the first failed file");
        assertThat(files()).filteredOn(file -> file.status() == FileRecord.Status.FAILED).singleElement()
                .satisfies(file -> assertThat(file.error()).contains("injected write failure"));
        assertThat(dataBlobs()).hasSize(1);
    }

    @Test
    void processesTheRemainingFilesWhenConfiguredToContinue(CapturedOutput output) {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU") && name.contains(firstEuFileStem()));

        assertThatThrownBy(() -> migrate("--migration.failure.on-file-failure=CONTINUE"))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("1 of 3 files failed");

        assertThat(output.getOut()).contains("Migration failed: runId=")
                .contains("Files: 2/3 (66.6667%) failed=1", "Partitions: 1/2 (50%)")
                .contains("Failed files:\n    source=" + firstEuFileLocation());
        assertThat(files()).extracting(FileRecord::status).containsExactlyInAnyOrder(
                FileRecord.Status.COMPLETED, FileRecord.Status.COMPLETED, FileRecord.Status.FAILED);
    }

    @Test
    void refusesToStartAfterAFailedRunUntilCleanIsSetThenStartsOverUnderANewRun() {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU"));
        assertThatThrownBy(() -> migrate("--migration.processing.parallel-files=1"))
                .isInstanceOf(MigrationFailedException.class);
        String failedRun = control().runId();
        String apacBlob = dataBlobs().getFirst();
        blobStore.stopFailing();

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Run " + failedRun + " of this migration failed (1 of 3 files failed)")
                .hasMessageContaining("Set migration.target.clean=true");
        assertThat(control().runId()).isEqualTo(failedRun);

        migrate("--migration.target.clean=true");

        ControlRecord control = control();
        assertThat(control.runId()).isNotEqualTo(failedRun);
        assertThat(control.status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(control.progress().completedFiles()).isEqualTo(3);
        assertThat(blobStore.uploadsStarted(apacBlob)).as("copied again from the beginning").isEqualTo(2);
        assertThat(stateStore.findFiles(failedRun)).as("file rows of the failed run").isEmpty();
        assertThat(files()).hasSize(3)
                .allSatisfy(file -> assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED));
        assertThat(dataBlobs()).hasSize(3);
    }

    @Test
    void refusesToCleanUnderALiveRunAndCleansOnceItsHeartbeatIsStale() throws IOException {
        appendFiles();
        stateStore.putControl(earlierRun(Instant.now()));
        publish(PREFIX + "/written-by-the-earlier-run.parquet");

        assertThatThrownBy(() -> migrate("--migration.target.clean=true"))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Run " + EARLIER_RUN + " of this migration is still running")
                .hasMessageContaining("migration.target.clean=true is refused");
        assertThat(control().runId()).isEqualTo(EARLIER_RUN);
        assertThat(dataBlobs()).containsExactly(PREFIX + "/written-by-the-earlier-run.parquet");

        // The earlier run died: its heartbeat is older than the timeout.
        stateStore.putControl(earlierRun(Instant.now().minus(Duration.ofMinutes(6))));

        migrate("--migration.target.clean=true");

        assertThat(control().runId()).isNotEqualTo(EARLIER_RUN);
        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(dataBlobs()).hasSize(3).doesNotContain(PREFIX + "/written-by-the-earlier-run.parquet");
    }

    @Test
    void refusesToRunWhileAnotherRunIsLive() {
        appendFiles();
        ControlRecord live = earlierRun(Instant.now());
        stateStore.putControl(live);

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Run " + EARLIER_RUN + " of this migration is still running");

        assertThat(stateStore.controls()).containsExactly(live);
        assertThat(dataBlobs()).isEmpty();
    }

    @Test
    void stopsPublishingOnceAnotherRunTookTheMigrationOver() {
        appendFiles();
        stateStore.takeOverOnFirstOutcome();

        assertThatThrownBy(() -> migrate("--migration.processing.parallel-files=1",
                "--migration.state.outcome-flush-interval=0s", "--migration.progress.log-every=50ms"))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("lost its claim on the migration");

        // The migration's state is left to the run that owns it now.
        assertThat(control().runId()).isEqualTo(InMemoryState.Store.OTHER_RUN);
        assertThat(control().status()).isEqualTo(ControlRecord.Status.RUNNING);
        assertThat(dataBlobs()).hasSize(1);
    }

    @Test
    void publishesNothingWhenTheMigrationIsTakenOverWhileAFileIsBeingWritten() {
        appendFiles();
        // The check before the file starts passes; the check just before publishing it fails.
        blobStore.onFirstWrite(stateStore::takeOver);

        assertThatThrownBy(() -> migrate("--migration.processing.parallel-files=1",
                "--migration.progress.log-every=50ms"))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("lost its claim on the migration");

        assertThat(dataBlobs()).isEmpty();
        assertThat(blobStore.aborted()).hasSize(1);
        assertThat(control().runId()).isEqualTo(InMemoryState.Store.OTHER_RUN);
    }

    @Test
    void refusesANewRunIntoATargetThatAlreadyHoldsOutput() {
        appendFiles();
        migrate();
        String completed = control().runId();

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("already holds 3 files written by run " + completed + " (COMPLETED)");
        assertThat(control().runId()).isEqualTo(completed);
    }

    @Test
    void replacesEarlierOutputWhenAskedToClean() {
        appendFiles();
        migrate();
        String firstRun = control().runId();
        IcebergTestTables.append(table, rows("US", 500, 2));

        migrate("--migration.target.clean=true");

        assertThat(control().runId()).isNotEqualTo(firstRun);
        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(stateStore.findFiles(firstRun)).isEmpty();
        assertThat(files()).hasSize(4);
        assertThat(dataBlobs()).hasSize(4);
    }

    private void migrate(String... extraArgs) {
        List<String> args = new ArrayList<>(List.of(
                "--migration.source.dataset-id=" + SOURCE_ID,
                "--migration.target.dataset-id=" + TARGET_ID,
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.file-table=" + "migration_file",
                "--migration.state.control-table=" + "migration_control"));
        args.addAll(List.of(extraArgs));
        MigrationApplication.run(
                new SpringApplication(MigrationApplication.class, InMemoryStorage.class, InMemoryState.class),
                args.toArray(String[]::new));
    }

    /** One APAC file, then two EU files: three files across two partitions. */
    private void appendFiles() {
        IcebergTestTables.append(table, rows("APAC", 0, 1));
        IcebergTestTables.append(table, rows("EU", 100, 2));
        IcebergTestTables.append(table, rows("EU", 200, 2));
    }

    private String firstEuFileLocation() {
        return euFiles().getFirst();
    }

    private String firstEuFileStem() {
        String location = firstEuFileLocation();
        return location.substring(location.lastIndexOf('/') + 1).replace(".parquet", "");
    }

    private List<String> euFiles() {
        List<String> locations = new ArrayList<>();
        table.newScan().planFiles().forEach(task -> {
            if (task.file().location().contains("region=EU")) {
                locations.add(task.file().location());
            }
        });
        return locations.stream().sorted().toList();
    }

    private ControlRecord control() {
        List<ControlRecord> controls = stateStore.findControls(TARGET_ID);
        assertThat(controls).hasSize(1);
        return controls.getFirst();
    }

    /** The file rows of the migration's latest run. */
    private List<FileRecord> files() {
        return stateStore.findFiles(control().runId());
    }

    private Map<String, List<FileRecord.Status>> statusByPartition() {
        return files().stream()
                .sorted(Comparator.comparing(FileRecord::sourcePath))
                .collect(Collectors.groupingBy(file -> file.sourcePath().replaceAll(".*/(region=[^/]+)/.*", "$1"),
                        TreeMap::new,
                        Collectors.mapping(FileRecord::status, Collectors.toList())));
    }

    /** The control row of a run that started before this test's run, last renewed at {@code heartbeatAt}. */
    private ControlRecord earlierRun(Instant heartbeatAt) {
        return new ControlRecord(TARGET_ID, SOURCE_ID, table.location(), TARGET_LOCATION, EARLIER_RUN,
                ControlRecord.Status.RUNNING, null, heartbeatAt.minus(Duration.ofHours(1)), heartbeatAt, null, 3, 5,
                ControlRecord.Progress.NONE, null);
    }

    private void publish(String blobName) throws IOException {
        var upload = blobStore.startUpload(blobName, EARLIER_RUN);
        try (var out = upload.outputStream()) {
            out.write(new byte[] {1, 2, 3});
        }
        upload.commit(Map.of());
    }

    private List<String> dataBlobs() {
        return blobStore.list(PREFIX + "/").stream()
                .map(BlobStore.BlobInfo::name)
                .sorted()
                .toList();
    }

    private static List<Record> rows(String region, long firstId, int count) {
        return LongStream.range(firstId, firstId + count).mapToObj(id -> {
            GenericRecord row = GenericRecord.create(SCHEMA);
            row.setField("id", id);
            row.setField("region", region);
            return (Record) row;
        }).toList();
    }

    private DeleteFile positionDeletes(DataFile data, long... positions) throws IOException {
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(table.io().newOutputFile(
                        tempDir.resolve("orders/data/pos-deletes.parquet").toUri().toString()))
                .withSpec(table.spec())
                .withPartition(data.partition())
                .overwrite()
                .buildPositionWriter();
        try (writer) {
            for (long position : positions) {
                writer.write(PositionDelete.<Record>create().set(data.location(), position));
            }
        }
        return writer.toDeleteFile();
    }
}
