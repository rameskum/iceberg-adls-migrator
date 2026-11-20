package com.tds.pde.migration;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.testing.FlatNamespaceFileSystem;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryState;
import com.tds.pde.migration.testing.ParquetOutput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * The whole application with {@code migration.target.client=hadoop}: the target is written through the file system
 * of the Spark session's Hadoop configuration (the stand-in {@code org.apache.spark.SparkContext}), which serves
 * {@code abfss://} with {@link FlatNamespaceFileSystem}, a fake of the ABFS driver without hierarchical namespace.
 * No Azure setting is given.
 */
@ExtendWith(OutputCaptureExtension.class)
class HadoopEndToEndTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "region", Types.StringType.get()));
    private static final String SOURCE_ID = "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01";
    private static final String TARGET_ID = "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02";
    private static final String AUTHORITY = "data@salesacct.dfs.core.windows.net";
    private static final String TARGET_LOCATION = "abfss://" + AUTHORITY + "/sales/orders";
    private static final String EARLIER_RUN = "20261117-103000-ab12cd";

    @TempDir
    Path tempDir;

    private final InMemoryState.Store stateStore = new InMemoryState.Store();
    private Table table;
    private Path datasets;

    @BeforeEach
    void createSourceTableAndDatasets() throws IOException {
        FlatNamespaceFileSystem.reset();
        InMemoryState.current = stateStore;
        table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("region").build());
        IcebergTestTables.append(table, rows("EU", 0, 3000));
        IcebergTestTables.append(table, rows("US", 3000, 2000));
        datasets = Files.createDirectory(tempDir.resolve("datasets"));
        Files.writeString(datasets.resolve(SOURCE_ID + ".json"), """
                { "datasetId": "%s", "format": "ICEBERG", "location": "%s" }
                """.formatted(SOURCE_ID, table.location()));
        Files.writeString(datasets.resolve(TARGET_ID + ".json"), """
                { "datasetId": "%s", "format": "PARQUET", "location": "%s" }
                """.formatted(TARGET_ID, TARGET_LOCATION));
    }

    @AfterEach
    void resetFileSystem() {
        FlatNamespaceFileSystem.reset();
    }

    @Test
    void migratesThroughTheSparkSessionsFileSystemAndStartsOverWhenAskedToClean(CapturedOutput output) {
        migrate();

        assertThat(output.getOut()).contains("Target storage opened: client=hadoop fileSystem="
                + FlatNamespaceFileSystem.class.getName() + " uri=abfss://" + AUTHORITY + " prefix=sales/orders");
        List<String> dataFiles = dataFiles();
        assertThat(dataFiles).hasSize(2).allMatch(path -> path.startsWith("/sales/orders/region="));
        assertThat(rowsIn(dataFiles)).isEqualTo(5000);
        assertThat(FlatNamespaceFileSystem.folders(AUTHORITY)).as("no folder of temporary files is left")
                .containsExactly("/sales", "/sales/orders");
        ControlRecord first = control();
        assertThat(first.status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(files()).hasSize(2).allSatisfy(file -> {
            assertThat(file.status()).isEqualTo(FileRecord.Status.COMPLETED);
            assertThat(dataFiles).contains("/" + file.targetPath());
        });

        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("already holds 2 files written by run " + first.runId());

        migrate("--migration.target.clean=true");

        assertThat(control().runId()).isNotEqualTo(first.runId());
        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(dataFiles()).isEqualTo(dataFiles);
        assertThat(rowsIn(dataFiles())).isEqualTo(5000);
    }

    @Test
    void aFileWhoseRenameIsRefusedIsNotPublishedAndTheCleanAfterwardsRemovesWhatTheRunLeft() {
        FlatNamespaceFileSystem.refuseRenamesTo(path -> path.contains("region=US"));

        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("1 of 2 files failed");

        assertThat(FlatNamespaceFileSystem.files(AUTHORITY).keySet()).noneMatch(path -> path.contains("region=US"));
        assertThat(control().status()).isEqualTo(ControlRecord.Status.FAILED);
        assertThat(files()).filteredOn(file -> file.status() == FileRecord.Status.FAILED).singleElement()
                .satisfies(file -> assertThat(file.error()).contains("the file system refused to rename"));
        assertThat(FlatNamespaceFileSystem.files(AUTHORITY).keySet()).as("the failed file's temporary file is deleted")
                .noneMatch(path -> path.contains("/_migration/"));

        FlatNamespaceFileSystem.stopFailing();
        migrate("--migration.target.clean=true");

        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(rowsIn(dataFiles())).isEqualTo(5000);
        assertThat(FlatNamespaceFileSystem.folders(AUTHORITY)).containsExactly("/sales", "/sales/orders");
    }

    @Test
    void aFileThatDoesNotArriveWholeIsNotPublished() {
        FlatNamespaceFileSystem.dropLastByteOf(path -> path.contains("/_migration/tmp/") && path.contains("region=EU"));

        assertThatThrownBy(() -> migrate("--migration.failure.on-file-failure=CONTINUE"))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("1 of 2 files failed");

        assertThat(dataFiles()).singleElement().satisfies(path -> assertThat(path).contains("region=US"));
        assertThat(files()).filteredOn(file -> file.status() == FileRecord.Status.FAILED).singleElement()
                .satisfies(file -> assertThat(file.error()).containsPattern("holds \\d+ bytes, but \\d+ were written"));
    }

    @Test
    void cleanDeletesTheTemporaryFilesACrashedRunLeftBehind() {
        stateStore.putControl(new ControlRecord(TARGET_ID, SOURCE_ID, table.location(), TARGET_LOCATION, EARLIER_RUN,
                ControlRecord.Status.RUNNING, null, Instant.now().minus(Duration.ofHours(2)),
                Instant.now().minus(Duration.ofHours(1)), null, 2, 5000, ControlRecord.Progress.NONE, null));
        String leftover = "/sales/orders/_migration/tmp/" + EARLIER_RUN + "/region=EU__part.parquet";
        FlatNamespaceFileSystem.put(AUTHORITY, leftover, new byte[] {1, 2});

        assertThatThrownBy(this::migrate).isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Set migration.target.clean=true");

        migrate("--migration.target.clean=true");

        assertThat(control().status()).isEqualTo(ControlRecord.Status.COMPLETED);
        assertThat(FlatNamespaceFileSystem.files(AUTHORITY)).doesNotContainKey(leftover);
        assertThat(FlatNamespaceFileSystem.folders(AUTHORITY)).containsExactly("/sales", "/sales/orders");
        assertThat(rowsIn(dataFiles())).isEqualTo(5000);
    }

    private List<String> dataFiles() {
        return FlatNamespaceFileSystem.files(AUTHORITY).keySet().stream()
                .filter(path -> path.startsWith("/sales/orders/") && !path.contains("/_migration/"))
                .toList();
    }

    private long rowsIn(List<String> paths) {
        Map<String, byte[]> files = FlatNamespaceFileSystem.files(AUTHORITY);
        return paths.stream()
                .mapToLong(path -> ParquetOutput.of(files.get(path), tempDir).standaloneRows().size())
                .sum();
    }

    private ControlRecord control() {
        List<ControlRecord> controls = stateStore.findControls(TARGET_ID);
        assertThat(controls).hasSize(1);
        return controls.getFirst();
    }

    private List<FileRecord> files() {
        return stateStore.findFiles(control().runId());
    }

    private void migrate(String... extraArgs) {
        List<String> args = new ArrayList<>(List.of(
                "--migration.source.dataset-id=" + SOURCE_ID,
                "--migration.target.dataset-id=" + TARGET_ID,
                "--migration.target.client=hadoop",
                "--migration.datasets.stub-directory=" + datasets,
                "--migration.state.file-table=migration_file",
                "--migration.state.control-table=migration_control"));
        args.addAll(List.of(extraArgs));
        MigrationApplication.run(new SpringApplication(MigrationApplication.class, InMemoryState.class),
                args.toArray(String[]::new));
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
