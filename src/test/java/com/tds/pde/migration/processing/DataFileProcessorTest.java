package com.tds.pde.migration.processing;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.tds.pde.migration.dataset.DatasetSchema;
import com.tds.pde.migration.target.TargetPathResolver;
import com.tds.pde.migration.target.ParquetDataWriter;
import com.tds.pde.migration.schema.TargetSchemaValidator;
import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.source.PlannedPartition;
import com.tds.pde.migration.source.SourceFile;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.LogCapture;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DataFileProcessorTest {

    private static final Schema ORDERS = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "region", Types.StringType.get()),
            optional(3, "amount", Types.DecimalType.of(10, 2)),
            optional(4, "ordered_at", Types.TimestampType.withZone()),
            optional(5, "ship_date", Types.DateType.get()),
            optional(6, "tags", Types.ListType.ofOptional(7, Types.StringType.get())));

    @TempDir
    Path tempDir;

    private final InMemoryBlobStore blobStore = new InMemoryBlobStore();
    private final DataFileProcessor processor = new DataFileProcessor(
            new IcebergDataReader(), new ParquetDataWriter(TestProperties.defaults()), blobStore);
    private final IcebergMetadataReader metadataReader = new IcebergMetadataReader(new Configuration());

    @Test
    void copiesEveryRowIntoAStandaloneParquetFileAndPublishesIt() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS,
                PartitionSpec.builderFor(ORDERS).identity("region").build());
        List<Record> rows = orders("EU", 1, 2, 3);
        IcebergTestTables.append(table, rows);

        Run run = plan(table, null);
        FileResult result = processor.process(run.context(), run.task(0), FileProgressListener.NONE);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.error()).isNull();
        assertThat(result.physicalRowsRead()).isEqualTo(3);
        assertThat(result.recordsWritten()).isEqualTo(3);
        assertThat(result.recordsDeleted()).isZero();
        assertThat(blobStore.aborted()).isEmpty();

        InMemoryBlobStore.Blob blob = blobStore.blob(run.task(0).target()).orElseThrow();
        assertThat(result.bytesWritten()).as("size of the target file").isEqualTo(blob.content().length);
        ParquetOutput output = ParquetOutput.of(blob.content(), tempDir);
        assertThat(output.records(ORDERS)).containsExactlyElementsOf(rows);

        // Readable without Iceberg, with the source column names.
        List<Group> standalone = output.standaloneRows();
        assertThat(standalone).hasSize(3);
        assertThat(standalone.getFirst().getLong("id", 0)).isEqualTo(1);
        assertThat(standalone.getFirst().getString("region", 0)).isEqualTo("EU");
        assertThat(output.parquetSchema().getFields()).extracting(org.apache.parquet.schema.Type::getName)
                .containsExactly("id", "region", "amount", "ordered_at", "ship_date", "tags");

        String source = run.task(0).source().location();
        assertThat(output.footerMetadata())
                .containsEntry("migration.source-file", source)
                .containsEntry("migration.source-snapshot-id", String.valueOf(table.currentSnapshot().snapshotId()));
        assertThat(blob.metadata())
                .containsEntry("source_partition", "region%3DEU")
                .containsEntry("source_snapshot_id", String.valueOf(table.currentSnapshot().snapshotId()))
                .containsEntry("physical_records", "3")
                .containsEntry("records_written", "3")
                .containsKey("source_file");
    }

    @Test
    void dropsRowsRemovedByPositionDeletes() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        DataFile data = IcebergTestTables.append(table, orders("EU", 0, 1, 2, 3, 4));
        table.newRowDelta().addDeletes(positionDeletes(table, data, 1, 3)).commit();

        FileResult result = processOnlyFile(table);

        assertThat(result.recordsWritten()).isEqualTo(3);
        assertThat(result.recordsDeleted()).isEqualTo(2);
        assertThat(idsWritten(result)).containsExactly(0L, 2L, 4L);
    }

    @Test
    void dropsRowsRemovedByEqualityDeletes() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 0, 1, 2, 3));
        table.newRowDelta().addDeletes(equalityDeletes(table, 1, 2)).commit();

        FileResult result = processOnlyFile(table);

        assertThat(result.recordsDeleted()).isEqualTo(2);
        assertThat(idsWritten(result)).containsExactly(0L, 3L);
    }

    @Test
    void appliesAnEqualityDeleteOnAColumnDroppedAfterTheMigratedSnapshot() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 0, 1));
        IcebergTestTables.append(table, orders("US", 2, 3));
        table.newRowDelta().addDeletes(equalityDeletesOnRegion(table, "EU")).commit();
        long snapshotWithDelete = table.currentSnapshot().snapshotId();
        table.updateSchema().deleteColumn("region").commit();

        // Time travel: the snapshot's own schema still has the column the delete keys on.
        Run run = planAt(table, snapshotWithDelete);
        FileTask euFile = run.tasks().stream().filter(task -> task.source().deleteFileCount() > 0).findFirst()
                .orElseThrow();
        FileResult eu = processor.process(run.context(), euFile, FileProgressListener.NONE);

        assertThat(eu.succeeded()).as("failure: %s", eu.failureReason()).isTrue();
        assertThat(eu.recordsDeleted()).isEqualTo(2);
        assertThat(eu.recordsWritten()).isZero();
    }

    @Test
    void appliesAnEqualityDeleteOnAColumnDroppedBeforeTheMigratedSnapshot() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 0, 1));
        IcebergTestTables.append(table, orders("US", 2, 3));
        table.newRowDelta().addDeletes(equalityDeletesOnRegion(table, "EU")).commit();
        table.updateSchema().deleteColumn("region").commit();
        IcebergTestTables.append(table, List.of(orderWithoutRegion(table, 4)));
        long snapshot = table.currentSnapshot().snapshotId();

        Run run = planAt(table, snapshot);
        List<FileResult> results = run.tasks().stream()
                .map(task -> processor.process(run.context(), task, FileProgressListener.NONE)).toList();

        assertThat(results).allSatisfy(result -> assertThat(result.succeeded())
                .as("failure: %s", result.failureReason()).isTrue());
        assertThat(results).extracting(FileResult::recordsWritten).containsExactlyInAnyOrder(0L, 2L, 1L);
        assertThat(results).flatExtracting(result -> output(result).records(run.context().source().schema()))
                .extracting(row -> row.getField("id")).containsExactlyInAnyOrder(2L, 3L, 4L);
    }

    @Test
    void appliesAnEqualityDeleteOnAColumnDroppedAfterTheLastSnapshotWhenReadingTheCurrentSnapshot()
            throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 0, 1));
        IcebergTestTables.append(table, orders("US", 2, 3));
        table.newRowDelta().addDeletes(equalityDeletesOnRegion(table, "EU")).commit();
        table.updateSchema().deleteColumn("region").commit();

        Run run = plan(table, null);
        List<FileResult> results = run.tasks().stream()
                .map(task -> processor.process(run.context(), task, FileProgressListener.NONE)).toList();

        assertThat(results).allSatisfy(result -> assertThat(result.succeeded())
                .as("failure: %s", result.failureReason()).isTrue());
        assertThat(results).extracting(FileResult::recordsWritten).containsExactlyInAnyOrder(0L, 2L);
        assertThat(results).extracting(FileResult::recordsDeleted).containsExactlyInAnyOrder(2L, 0L);
    }

    @Test
    void dropsRowsRemovedByDeletionVectors() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned(), 3);
        DataFile data = IcebergTestTables.append(table, orders("EU", 0, 1, 2));
        BaseDVFileWriter writer = new BaseDVFileWriter(
                () -> table.io().newOutputFile(tempDir.resolve("orders/data/dv.puffin").toUri().toString()),
                path -> null);
        try (writer) {
            writer.delete(data.location(), 0, table.spec(), null);
        }
        org.apache.iceberg.RowDelta delta = table.newRowDelta();
        writer.result().deleteFiles().forEach(delta::addDeletes);
        delta.commit();

        FileResult result = processOnlyFile(table);

        assertThat(result.recordsDeleted()).isEqualTo(1);
        assertThat(idsWritten(result)).containsExactly(1L, 2L);
    }

    @Test
    void stillPublishesAnEmptyFileWhenEveryRowIsDeleted() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        DataFile data = IcebergTestTables.append(table, orders("EU", 0, 1));
        table.newRowDelta().addDeletes(positionDeletes(table, data, 0, 1)).commit();

        FileResult result = processOnlyFile(table);

        assertThat(result.succeeded()).isTrue();
        assertThat(result.recordsWritten()).isZero();
        assertThat(result.recordsDeleted()).isEqualTo(2);
        ParquetOutput output = output(result);
        assertThat(result.bytesWritten()).isEqualTo(blobStore.blob(result.task().target()).orElseThrow()
                .content().length).isPositive();
        assertThat(output.standaloneRows()).isEmpty();
        assertThat(output.parquetSchema().getFieldCount()).isEqualTo(ORDERS.columns().size());
    }

    @Test
    void readsFilesWrittenBeforeASchemaChangeIntoTheSnapshotSchema() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 1));
        table.updateSchema().addColumn("channel", Types.StringType.get()).renameColumn("region", "market").commit();

        Run run = plan(table, null);
        FileResult result = processor.process(run.context(), run.task(0), FileProgressListener.NONE);

        Record row = output(result).records(table.schema()).getFirst();
        assertThat(row.getField("market")).isEqualTo("EU");
        assertThat(row.getField("channel")).isNull();
        assertThat(output(result).parquetSchema().containsField("market")).isTrue();
    }

    @Test
    void convertsUuidAndTimeColumnsTheTargetDeclaresAsString() {
        Schema schema = new Schema(
                required(1, "id", Types.UUIDType.get()),
                optional(2, "opened_at", Types.TimeType.get()));
        Table table = IcebergTestTables.create(tempDir.resolve("stores"), schema, PartitionSpec.unpartitioned());
        UUID id = UUID.fromString("0b6f4c2e-3f1d-4c55-9a52-5d2b8a1c7e01");
        GenericRecord row = GenericRecord.create(schema);
        row.setField("id", id);
        row.setField("opened_at", LocalTime.of(9, 30, 5, 120_000_000));
        IcebergTestTables.append(table, List.of(row));
        OutputSchema outputSchema = new TargetSchemaValidator().resolve(schema, new DatasetSchema(List.of(
                new DatasetSchema.Field("id", "string", false),
                new DatasetSchema.Field("opened_at", "string", true))));

        Run run = plan(table, outputSchema);
        FileResult result = processor.process(run.context(), run.task(0), FileProgressListener.NONE);

        Record written = output(result).records(outputSchema.schema()).getFirst();
        assertThat(written.getField("id")).isEqualTo("0b6f4c2e-3f1d-4c55-9a52-5d2b8a1c7e01");
        assertThat(written.getField("opened_at")).isEqualTo("09:30:05.120000");
    }

    @Test
    void readsImportedFilesWithoutFieldIdsThroughTheNameMapping() throws IOException {
        Schema schema = new Schema(required(1, "id", Types.LongType.get()), optional(2, "region", Types.StringType.get()));
        Table table = IcebergTestTables.create(tempDir.resolve("imported"), schema, PartitionSpec.unpartitioned());
        table.updateProperties()
                .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(MappingUtil.create(schema)))
                .commit();
        Path file = tempDir.resolve("legacy-part-0.parquet");
        MessageType withoutIds = MessageTypeParser.parseMessageType(
                "message legacy { required int64 id; optional binary region (STRING); }");
        try (ParquetWriter<Group> writer = ExampleParquetWriter
                .builder(new org.apache.hadoop.fs.Path(file.toUri())).withType(withoutIds).build()) {
            SimpleGroupFactory groups = new SimpleGroupFactory(withoutIds);
            writer.write(groups.newGroup().append("id", 7L).append("region", "APAC"));
            writer.write(groups.newGroup().append("id", 8L).append("region", "EU"));
        }
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(file.toUri().toString())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(java.nio.file.Files.size(file))
                .withRecordCount(2)
                .build()).commit();

        FileResult result = processOnlyFile(table);

        assertThat(result.succeeded()).isTrue();
        assertThat(output(result).records(schema)).extracting(r -> r.getField("id"), r -> r.getField("region"))
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(7L, "APAC"),
                        org.assertj.core.groups.Tuple.tuple(8L, "EU"));
    }

    @Test
    void publishesNothingWhenWritingFails() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 1, 2, 3));
        blobStore.failWritesAfter(16);

        Run run = plan(table, null);
        FileResult result = processor.process(run.context(), run.task(0), FileProgressListener.NONE);

        assertThat(result.succeeded()).isFalse();
        assertThat(result.error()).rootCause().hasMessageContaining("injected write failure");
        assertThat(blobStore.blobs()).isEmpty();
        assertThat(blobStore.aborted()).containsExactly(run.task(0).target());
    }

    @Test
    void publishesNothingWhenTheCommitFails() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", 1));
        blobStore.failNextCommit();

        Run run = plan(table, null);
        FileResult result = processor.process(run.context(), run.task(0), FileProgressListener.NONE);

        assertThat(result.succeeded()).isFalse();
        assertThat(result.error()).hasMessageContaining("injected commit failure");
        assertThat(blobStore.blobs()).isEmpty();
        assertThat(blobStore.aborted()).containsExactly(run.task(0).target());
    }

    @Test
    void publishesNothingWhenTheSourceHoldsFewerRowsThanItsMetadataRecords() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        DataFile data = IcebergTestTables.writeDataFile(table, orders("EU", 1, 2));
        table.newAppend().appendFile(DataFiles.builder(table.spec()).copy(data).withRecordCount(3).build()).commit();

        FileResult result = processOnlyFile(table);

        assertThat(result.succeeded()).isFalse();
        assertThat(result.error()).hasMessage("Read 2 rows but the source metadata records 3");
        assertThat(blobStore.blobs()).isEmpty();
    }

    @Test
    void reportsCumulativeInFlightProgress() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, orders("EU", LongStream.range(0, 2500).toArray()));
        List<long[]> reports = new ArrayList<>();

        Run run = plan(table, null);
        processor.process(run.context(), run.task(0), (read, written, bytes) -> reports.add(new long[] {read, written, bytes}));

        assertThat(reports).extracting(report -> report[1]).containsExactly(1024L, 2048L, 2500L);
        assertThat(reports.getLast()[0]).isEqualTo(2500);
        assertThat(reports).extracting(report -> report[2]).isSorted();
        assertThat(reports.getLast()[2]).as("the final report is the size of the target file")
                .isEqualTo(blobStore.blob(run.task(0).target()).orElseThrow().content().length);
    }

    @Test
    void logsTheSizesAndSpeedOfACompletedFile() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), ORDERS, PartitionSpec.unpartitioned());
        DataFile data = IcebergTestTables.append(table, orders("EU", 1, 2, 3));
        Run run = plan(table, null);

        try (LogCapture logs = new LogCapture(DataFileProcessor.class.getName())) {
            FileResult result = processor.process(run.context(), run.task(0), FileProgressListener.NONE);

            assertThat(logs.messagesStartingWith("Source file completed: ")).singleElement()
                    .satisfies(line -> assertThat(line).contains("recordsWritten=3 recordsDeleted=0 sourceBytes=" + data.fileSizeInBytes()
                            + " targetBytes=" + result.bytesWritten() + " durationMs=")
                    .containsPattern("readMiBPerSecond=(\\d+\\.\\d|n/a)$"));
        }
    }

    private FileResult processOnlyFile(Table table) {
        Run run = plan(table, null);
        return processor.process(run.context(), run.task(0), FileProgressListener.NONE);
    }

    private Run planAt(Table table, long snapshotId) {
        MigrationPlan plan = metadataReader.plan(table.location(), snapshotId);
        RunContext context = new RunContext("run-1", plan.table(), OutputSchema.unchanged(plan.table().schema()));
        return new Run(context, tasks(plan));
    }

    private Run plan(Table table, OutputSchema outputSchema) {
        MigrationPlan plan = metadataReader.plan(table.location(), null);
        RunContext context = new RunContext("run-1", plan.table(),
                outputSchema == null ? OutputSchema.unchanged(plan.table().schema()) : outputSchema);
        return new Run(context, tasks(plan));
    }

    private static List<FileTask> tasks(MigrationPlan plan) {
        TargetPathResolver paths = new TargetPathResolver();
        List<FileTask> tasks = new ArrayList<>();
        for (PlannedPartition partition : plan.partitions()) {
            for (SourceFile file : partition.files()) {
                tasks.add(new FileTask(file, partition.path(), paths.resolve("sales/orders", partition.path(),
                        file.location())));
            }
        }
        return tasks;
    }

    private record Run(RunContext context, List<FileTask> tasks) {
        FileTask task(int index) {
            return tasks.get(index);
        }
    }

    private ParquetOutput output(FileResult result) {
        return ParquetOutput.of(blobStore.blob(result.task().target()).orElseThrow().content(), tempDir);
    }

    private List<Long> idsWritten(FileResult result) {
        return output(result).records(ORDERS).stream().map(row -> (Long) row.getField("id")).toList();
    }

    private static List<Record> orders(String region, long... ids) {
        return LongStream.of(ids).mapToObj(id -> {
            GenericRecord row = GenericRecord.create(ORDERS);
            row.setField("id", id);
            row.setField("region", region);
            row.setField("amount", new BigDecimal("12.50"));
            row.setField("ordered_at", OffsetDateTime.of(2026, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC));
            row.setField("ship_date", LocalDate.of(2026, 1, 17));
            row.setField("tags", List.of("gift", "express"));
            return (Record) row;
        }).toList();
    }

    private DeleteFile positionDeletes(Table table, DataFile data, long... positions) throws IOException {
        PositionDeleteWriter<Record> writer = Parquet.writeDeletes(
                        table.io().newOutputFile(tempDir.resolve("orders/data/pos-deletes.parquet").toUri().toString()))
                .withSpec(table.spec())
                .overwrite()
                .buildPositionWriter();
        try (writer) {
            for (long position : positions) {
                writer.write(PositionDelete.<Record>create().set(data.location(), position));
            }
        }
        return writer.toDeleteFile();
    }

    private DeleteFile equalityDeletes(Table table, long... ids) throws IOException {
        Schema deleteSchema = table.schema().select("id");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(
                        table.io().newOutputFile(tempDir.resolve("orders/data/eq-deletes.parquet").toUri().toString()))
                .forTable(table)
                .withSpec(table.spec())
                .rowSchema(deleteSchema)
                .equalityFieldIds(1)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .buildEqualityWriter();
        try (writer) {
            for (long id : ids) {
                GenericRecord key = GenericRecord.create(deleteSchema);
                key.setField("id", id);
                writer.write(key);
            }
        }
        return writer.toDeleteFile();
    }

    private static Record orderWithoutRegion(Table table, long id) {
        GenericRecord row = GenericRecord.create(table.schema());
        row.setField("id", id);
        row.setField("amount", new BigDecimal("12.50"));
        row.setField("ordered_at", OffsetDateTime.of(2026, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC));
        row.setField("ship_date", LocalDate.of(2026, 1, 17));
        row.setField("tags", List.of("gift"));
        return row;
    }

    private DeleteFile equalityDeletesOnRegion(Table table, String... regions) throws IOException {
        Schema deleteSchema = table.schema().select("region");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(
                        table.io().newOutputFile(tempDir.resolve("orders/data/eq-region-deletes.parquet").toUri()
                                .toString()))
                .forTable(table)
                .withSpec(table.spec())
                .rowSchema(deleteSchema)
                .equalityFieldIds(2)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .buildEqualityWriter();
        try (writer) {
            for (String region : regions) {
                GenericRecord key = GenericRecord.create(deleteSchema);
                key.setField("region", region);
                writer.write(key);
            }
        }
        return writer.toDeleteFile();
    }
}
