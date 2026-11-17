package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.tds.pde.migration.destination.BlobStore;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.testing.H2StateStore;
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
 * Runs the whole application, as the Databricks launcher does, against real Iceberg tables, an H2 state store
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

    private final H2StateStore stateStore = new H2StateStore();
    private Table table;
    private Path datasets;

    @BeforeEach
    void createSourceTableAndDatasets() throws IOException {
        InMemoryStorage.current = blobStore;
        table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build());
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve("orders-src.json"), """
                { "datasetId": "orders-src", "format": "ICEBERG", "location": "%s" }
                """.formatted(table.location()));
        Files.writeString(datasets.resolve("orders-adls.json"), """
                { "datasetId": "orders-adls", "format": "PARQUET",
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
            assertThat(file.attempts()).isEqualTo(1);
        });
        assertThat(files).extracting(FileRecord::recordsDeleted).containsExactlyInAnyOrder(0L, 0L, 0L, 2L);
        assertThat(dataBlobs()).hasSize(4);
        long rowsInOutput = dataBlobs().stream()
                .mapToLong(name -> ParquetOutput.of(blobStore.blob(name).orElseThrow().content(), tempDir)
                        .standaloneRows().size())
                .sum();
        assertThat(rowsInOutput).isEqualTo(7);
        assertThat(blobStore.isLocked(LOCK)).isFalse();
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
    void stopsAtTheFirstFailedFileByDefault() {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU"));

        assertThatThrownBy(this::migrate)
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("failed: 1 of 3 files failed")
                .hasMessageContaining("resume it with migration.run-id=");

        RunRecord run = onlyRun();
        assertThat(run.status()).isEqualTo(RunRecord.Status.FAILED);
        assertThat(run.error()).isEqualTo("1 of 3 files failed");
        assertThat(statusByPartition(run)).isEqualTo(Map.of(
                "region=APAC", List.of(FileRecord.Status.COMPLETED),
                "region=EU", List.of(FileRecord.Status.FAILED, FileRecord.Status.PENDING)));
        // Only two partitions exist; with fail-fast the second EU file was never attempted.
        assertThat(files(run)).filteredOn(file -> file.status() == FileRecord.Status.FAILED).singleElement()
                .satisfies(file -> assertThat(file.lastError()).contains("injected write failure"));
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
        assertThat(files(run)).extracting(FileRecord::attempts).containsExactlyInAnyOrder(1, 2, 1);
    }

    @Test
    void recordsAPublishedFileWhoseCompletionTheStateStoreMissed() {
        appendFiles();
        blobStore.failWritesTo(name -> name.contains("region=EU"));
        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class);
        String apacBlob = dataBlobs().getFirst();
        stateStore.jdbc().sql("UPDATE migration_file SET status = 'IN_PROGRESS', records_written = NULL "
                + "WHERE destination_path = ?").param(apacBlob).update();
        blobStore.stopFailing();

        migrate();

        assertThat(blobStore.uploadsStarted(apacBlob)).isEqualTo(1);
        assertThat(files(onlyRun())).filteredOn(file -> file.destinationPath().equals(apacBlob)).singleElement()
                .satisfies(file -> {
                    assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED);
                    assertThat(file.recordsWritten()).isEqualTo(1);
                });
    }

    @Test
    void refusesToRunWhileAnotherRunHoldsTheDestination() {
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

        assertThat(onlyRun().status()).isEqualTo(RunRecord.Status.FAILED);
        assertThat(dataBlobs()).isEmpty();
    }

    @Test
    void refusesANewRunIntoADestinationThatAlreadyHoldsOutput() {
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

        migrate("--migration.destination.clean=true");

        assertThat(runs()).extracting(RunRecord::runId, RunRecord::status).contains(
                tuple(firstRun, RunRecord.Status.SUPERSEDED));
        assertThat(runs()).filteredOn(run -> run.status() == RunRecord.Status.SUCCEEDED).singleElement()
                .satisfies(run -> assertThat(run.totalFiles()).isEqualTo(4));
        assertThat(dataBlobs()).hasSize(4);
    }

    private void migrate(String... extraArgs) {
        List<String> args = new ArrayList<>(List.of(
                "--migration.source.dataset-id=orders-src",
                "--migration.destination.dataset-id=orders-adls",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.run-table=" + H2StateStore.RUN_TABLE,
                "--migration.state.file-table=" + H2StateStore.FILE_TABLE,
                "--spring.datasource.url=" + stateStore.url(),
                "--spring.datasource.driver-class-name=org.h2.Driver",
                "--spring.datasource.username=sa"));
        args.addAll(List.of(extraArgs));
        MigrationApplication.run(new SpringApplication(MigrationApplication.class, InMemoryStorage.class),
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
        return stateStore.jdbc().sql("SELECT run_id FROM migration_run").query(String.class).list().stream()
                .map(id -> repository().findRun(id).orElseThrow()).toList();
    }

    private RunRecord onlyRun() {
        List<RunRecord> runs = runs();
        assertThat(runs).hasSize(1);
        return runs.getFirst();
    }

    private List<FileRecord> files(RunRecord run) {
        return repository().findFiles(run.runId());
    }

    private Map<String, List<FileRecord.Status>> statusByPartition(RunRecord run) {
        return files(run).stream()
                .sorted(Comparator.comparing(FileRecord::sourcePath))
                .collect(Collectors.groupingBy(FileRecord::partitionPath, TreeMap::new,
                        Collectors.mapping(FileRecord::status, Collectors.toList())));
    }

    private MigrationStateRepository repository() {
        return new MigrationStateRepository(stateStore.jdbc(), TestProperties.defaults());
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
