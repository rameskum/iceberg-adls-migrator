package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.InMemoryStorage;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
    private static final String LOCK = PREFIX + "/_migration/lock";

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
                .contains("File progress: 4/4 (100%)")
                .contains("Migration completed: runId=")
                .contains("Records: 9/9 (100%) written=7 deleted=2\n  Files: 4/4 (100%) failed=0\n"
                        + "  Partitions: 3/3 (100%)");

        RunRecord run = onlyRun();
        assertThat(run.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(run.finishedAt()).isNotNull();
        List<FileRecord> files = files(run);
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
        assertThat(blobStore.isLocked(LOCK)).isFalse();

        ControlRecord control = control();
        assertThat(control.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(control.lastRunId()).isEqualTo(run.runId());
        assertThat(run.sourceDatasetId()).as("run links to its migration").isEqualTo(control.sourceDatasetId());
        assertThat(run.targetDatasetId()).isEqualTo(control.targetDatasetId());
        assertThat(control.snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
        assertThat(control.totalFiles()).isEqualTo(4);
        assertThat(control.totalRecords()).isEqualTo(9);
        assertThat(files.stream().mapToLong(FileRecord::recordsWritten).sum()).as("records written").isEqualTo(7);
    }

    @Test
    void completesATableThatWasNeverWritten(CapturedOutput output) {
        migrate();

        assertThat(output.getOut()).contains("Migration completed: runId=")
                .contains("Records: 0/0 (100%)", "Files: 0/0 (100%)", "Partitions: 0/0 (100%)");
        assertThat(onlyRun().status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(dataBlobs()).isEmpty();
    }

    @Test
    void copiesFilesInParallel() {
        appendFiles();
        blobStore.holdCommitsUntilConcurrent(3);

        migrate("--migration.processing.parallel-files=3");

        assertThat(blobStore.maxConcurrentUploads()).isEqualTo(3);
        assertThat(onlyRun().status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(files(onlyRun())).extracting(FileRecord::status).containsOnly(FileRecord.Status.COMPLETED);
        assertThat(dataBlobs()).hasSize(3);
    }

    @Test
    void recordsFileOutcomesInBatches() {
        appendFiles();

        migrate("--migration.state.outcome-batch-size=2", "--migration.state.outcome-flush-interval=1h");

        assertThat(stateStore.outcomeWrites()).as("a full batch, then the rest at the end").containsExactly(2, 1);
        assertThat(files(onlyRun())).extracting(FileRecord::status).containsOnly(FileRecord.Status.COMPLETED);
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
                .hasMessageContaining("resume it with migration.run-id=");

        RunRecord run = onlyRun();
        assertThat(run.status()).isEqualTo(RunRecord.Status.FAILED);
        assertThat(run.error()).isEqualTo("1 of 3 files failed");
        assertThat(statusByPartition(run)).isEqualTo(Map.of(
                "region=APAC", List.of(FileRecord.Status.COMPLETED),
                "region=EU", List.of(FileRecord.Status.FAILED, FileRecord.Status.PENDING)));
        // Only two partitions exist; with fail-fast the second EU file was never started.
        assertThat(output.getOut()).contains("Stopped starting files after the first failed file");
        assertThat(files(run)).filteredOn(file -> file.status() == FileRecord.Status.FAILED).singleElement()
                .satisfies(file -> assertThat(file.error()).contains("injected write failure"));
        assertThat(dataBlobs()).hasSize(1);
        assertThat(control()).satisfies(control -> {
            assertThat(control.status()).isEqualTo(RunRecord.Status.FAILED);
            assertThat(control.lastRunId()).isEqualTo(run.runId());
        });
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
        assertThat(files(onlyRun())).extracting(FileRecord::status).containsExactlyInAnyOrder(
                FileRecord.Status.COMPLETED, FileRecord.Status.COMPLETED, FileRecord.Status.FAILED);
    }

    @Test
    void resumesAFailedRunWithoutRewritingPublishedFiles(CapturedOutput output) {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU"));
        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class);
        String apacBlob = dataBlobs().getFirst();
        blobStore.stopFailing();

        migrate();

        assertThat(output.getOut()).contains("Resuming after already published work:")
                .contains("Files: 3/3 (100%) failed=0\n  Partitions: 2/2 (100%)");
        RunRecord run = onlyRun();
        assertThat(run.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(blobStore.uploadsStarted(apacBlob)).isEqualTo(1);
        assertThat(files(run)).allSatisfy(file -> assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED));
        assertThat(files(run)).allSatisfy(file -> assertThat(file.error()).isNull());
        assertThat(control()).satisfies(control -> {
            assertThat(control.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
            assertThat(control.lastRunId()).isEqualTo(run.runId());
        });
    }

    @Test
    void findsThePublishedFilesOfAResumedRunWithOneListingOfTheTarget() {
        appendFiles();
        IcebergTestTables.append(table, rows("APAC", 10, 1));
        blobStore.failWritesTo(name -> name.contains("region=EU"));
        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class);
        assertThat(dataBlobs()).as("both APAC files published").hasSize(2);
        blobStore.stopFailing();
        blobStore.clearReads();

        migrate();

        assertThat(blobStore.reads()).containsExactly("list with metadata " + PREFIX + "/");
        assertThat(onlyRun().status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(dataBlobs()).hasSize(4).filteredOn(blob -> blob.contains("region=APAC")).hasSize(2)
                .allSatisfy(blob -> assertThat(blobStore.uploadsStarted(blob)).as("not copied again").isOne());
    }

    @Test
    void resumesARunThatDiedBeforeAllItsFileRowsWereRecorded(CapturedOutput output) {
        appendFiles();
        stateStore.crashAfterFileRows(1);
        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("injected crash after 1 file rows");
        assertThat(onlyRun().status()).isEqualTo(RunRecord.Status.RUNNING);
        assertThat(files(onlyRun())).hasSize(1);

        migrate();

        assertThat(output.getOut()).contains("recording the 2 missing as PENDING");
        RunRecord run = onlyRun();
        assertThat(run.status()).isEqualTo(RunRecord.Status.SUCCEEDED);
        assertThat(files(run)).hasSize(3)
                .allSatisfy(file -> assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED));
        assertThat(dataBlobs()).hasSize(3);
    }

    @Test
    void recordsAPublishedFileWhoseCompletionTheStateStoreMissed() {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU"));
        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class);
        String apacBlob = dataBlobs().getFirst();
        FileRecord apac = files(onlyRun()).stream().filter(file -> file.targetPath().equals(apacBlob))
                .findFirst().orElseThrow();
        stateStore.replaceFile(new FileRecord(apac.runId(), apac.sourcePath(), apac.targetPath(),
                apac.physicalRecords(), FileRecord.Status.PENDING, null, null));
        blobStore.stopFailing();

        migrate();

        assertThat(blobStore.uploadsStarted(apacBlob)).isEqualTo(1);
        assertThat(files(onlyRun())).filteredOn(file -> file.targetPath().equals(apacBlob)).singleElement()
                .satisfies(file -> {
                    assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED);
                    assertThat(file.recordsWritten()).isEqualTo(1);
                });
    }

    @Test
    void refusesToRunWhileAnotherRunHoldsTheTarget() {
        appendFiles();
        blobStore.acquireLock(LOCK);

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Another migration run holds the lock " + LOCK);

        assertThat(runs()).isEmpty();
        assertThat(dataBlobs()).isEmpty();
    }

    @Test
    void stopsWritingAsSoonAsTheLockIsLost() {
        appendFiles();
        blobStore.loseLock(LOCK);

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Lost the run lock");

        // The run's state is left to whichever run took the lock over.
        assertThat(onlyRun().status()).isEqualTo(RunRecord.Status.RUNNING);
        assertThat(dataBlobs()).isEmpty();
    }

    @Test
    void publishesNothingWhenTheLockIsLostWhileAFileIsBeingWritten() {
        appendFiles();
        // The check before the first file passes; the check just before publishing it fails.
        blobStore.loseLockAfter(LOCK, 1);

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Lost the run lock");

        assertThat(dataBlobs()).isEmpty();
        assertThat(blobStore.aborted()).hasSize(1);
        assertThat(files(onlyRun())).extracting(FileRecord::status)
                .containsOnly(FileRecord.Status.PENDING);
    }

    @Test
    void refusesANewRunIntoATargetThatAlreadyHoldsOutput() {
        appendFiles();
        migrate();

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("already holds 3 files written by run " + onlyRun().runId() + " (SUCCEEDED)");
    }

    @Test
    void replacesEarlierOutputWhenAskedToClean() {
        appendFiles();
        migrate();
        String firstRun = onlyRun().runId();
        IcebergTestTables.append(table, rows("US", 500, 2));

        migrate("--migration.target.clean=true");

        assertThat(runs()).extracting(RunRecord::runId, RunRecord::status).contains(
                tuple(firstRun, RunRecord.Status.SUPERSEDED));
        assertThat(runs()).filteredOn(run -> run.status() == RunRecord.Status.SUCCEEDED).singleElement()
                .satisfies(run -> assertThat(files(run)).hasSize(4));
        assertThat(dataBlobs()).hasSize(4);
    }

    private void migrate(String... extraArgs) {
        List<String> args = new ArrayList<>(List.of(
                "--migration.source.dataset-id=0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01",
                "--migration.target.dataset-id=5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.run-table=" + "migration_run",
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

    private List<RunRecord> runs() {
        return stateStore.runs();
    }

    private RunRecord onlyRun() {
        List<RunRecord> runs = runs();
        assertThat(runs).hasSize(1);
        return runs.getFirst();
    }

    private ControlRecord control() {
        return repository().findControl("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01",
                "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02").orElseThrow();
    }

    private List<FileRecord> files(RunRecord run) {
        return repository().findFiles(run.runId());
    }

    private Map<String, List<FileRecord.Status>> statusByPartition(RunRecord run) {
        return files(run).stream()
                .sorted(Comparator.comparing(FileRecord::sourcePath))
                .collect(Collectors.groupingBy(file -> file.sourcePath().replaceAll(".*/(region=[^/]+)/.*", "$1"),
                        TreeMap::new,
                        Collectors.mapping(FileRecord::status, Collectors.toList())));
    }

    private MigrationStateRepository repository() {
        return stateStore;
    }

    private List<String> dataBlobs() {
        return blobStore.list(PREFIX + "/").stream()
                .map(BlobStore.BlobInfo::name)
                .filter(name -> !name.startsWith(PREFIX + "/_migration/"))
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
