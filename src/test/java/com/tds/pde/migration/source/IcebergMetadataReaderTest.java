package com.tds.pde.migration.source;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.testing.IcebergTestTables;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergMetadataReaderTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "region", Types.StringType.get()),
            optional(3, "event_ts", Types.TimestampType.withZone()));

    private static final PartitionSpec BY_REGION_AND_DAY =
            PartitionSpec.builderFor(SCHEMA).identity("region").day("event_ts").build();

    @TempDir
    Path tempDir;

    private final IcebergMetadataReader reader = new IcebergMetadataReader(new Configuration());

    @Test
    void plansPartitionsAndFilesFromTheCurrentSnapshot() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, BY_REGION_AND_DAY);
        IcebergTestTables.append(table, records("EU", 1, 2, 3));
        IcebergTestTables.append(table, records("US", 4));
        IcebergTestTables.append(table, records("EU", 5, 6));

        MigrationPlan plan = reader.plan(table.location(), null);

        assertThat(plan.table().snapshotId()).isEqualTo(table.currentSnapshot().snapshotId());
        assertThat(plan.table().formatVersion()).isEqualTo(2);
        assertThat(plan.totalPartitions()).isEqualTo(2);
        assertThat(plan.totalFiles()).isEqualTo(3);
        assertThat(plan.totalRecords()).isEqualTo(6);
        assertThat(plan.totalDeleteFiles()).isZero();
        assertThat(plan.partitions()).extracting(PlannedPartition::path)
                .containsExactly("region=EU/event_ts_day=2026-01-15", "region=US/event_ts_day=2026-01-15");
        assertThat(plan.partitions()).extracting(PlannedPartition::recordCount).containsExactly(5L, 1L);
        PlannedPartition eu = plan.partitions().getFirst();
        assertThat(eu.files()).hasSize(2);
        assertThat(eu.files()).extracting(SourceFile::location).isSorted();
    }

    @Test
    void treatsATableThatWasNeverWrittenAsEmpty() {
        Table table = IcebergTestTables.create(tempDir.resolve("empty"), SCHEMA, BY_REGION_AND_DAY);

        MigrationPlan plan = reader.plan(table.location(), null);

        assertThat(plan.table().snapshotId()).isNull();
        assertThat(plan.partitions()).isEmpty();
        assertThat(plan.totalFiles()).isZero();
        assertThat(plan.totalRecords()).isZero();
        assertThat(plan.table().schema().sameSchema(SCHEMA)).isTrue();
    }

    @Test
    void plansTheRequestedSnapshotInsteadOfTheCurrentOne() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        DataFile first = IcebergTestTables.append(table, records("EU", 1, 2));
        long firstSnapshot = table.currentSnapshot().snapshotId();
        IcebergTestTables.append(table, records("EU", 3));

        MigrationPlan plan = reader.plan(table.location(), firstSnapshot);

        assertThat(plan.table().snapshotId()).isEqualTo(firstSnapshot);
        assertThat(plan.totalFiles()).isEqualTo(1);
        assertThat(plan.totalRecords()).isEqualTo(2);
        assertThat(plan.partitions().getFirst().files().getFirst().location()).isEqualTo(first.location());
    }

    @Test
    void readsTheCurrentSnapshotWithSchemaChangesMadeAfterTheLastWrite() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, records("EU", 1));
        table.updateSchema().renameColumn("region", "market").commit();

        Schema schema = reader.plan(table.location(), null).table().schema();

        assertThat(schema.findField("market")).isNotNull();
        assertThat(schema.findField("region")).isNull();
    }

    @Test
    void usesTheSchemaAnExplicitlyRequestedSnapshotWasWrittenWith() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, records("EU", 1));
        long beforeSchemaChange = table.currentSnapshot().snapshotId();
        table.updateSchema().addColumn("note", Types.StringType.get()).commit();
        IcebergTestTables.append(table, records(table.schema(), "EU", 2));

        assertThat(reader.plan(table.location(), beforeSchemaChange).table().schema().findField("note")).isNull();
        assertThat(reader.plan(table.location(), null).table().schema().findField("note")).isNotNull();
    }

    @Test
    void keepsTheSamePartitionPathUnderDifferentSpecsApart() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, records("EU", 1));
        table.updateSpec().addField("region").commit();
        IcebergTestTables.append(table, records("EU", 2));

        MigrationPlan plan = reader.plan(table.location(), null);

        assertThat(plan.partitions())
                .extracting(PlannedPartition::specId, PlannedPartition::path)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(0, ""),
                        org.assertj.core.groups.Tuple.tuple(1, "region=EU"));
        assertThat(plan.partitions().getFirst().displayName()).isEqualTo("<unpartitioned>");
    }

    @Test
    void attachesDeleteFilesToTheDataFilesTheyApplyTo() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, records("EU", 1, 2));
        table.newRowDelta().addDeletes(positionDeletes(table, FileFormat.PARQUET)).commit();

        MigrationPlan plan = reader.plan(table.location(), null);

        assertThat(plan.totalDeleteFiles()).isEqualTo(1);
        assertThat(plan.partitions().getFirst().files().getFirst().deleteFileCount()).isEqualTo(1);
        // Totals stay physical: deletes are applied, and counted, while copying.
        assertThat(plan.totalRecords()).isEqualTo(2);
    }

    @Test
    void rejectsDataFilesThatAreNotParquet() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(tempDir.resolve("orders/data/legacy.avro").toString())
                .withFormat(FileFormat.AVRO)
                .withFileSizeInBytes(100)
                .withRecordCount(1)
                .build()).commit();

        assertThatThrownBy(() -> reader.plan(table.location(), null))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("legacy.avro has format AVRO; only PARQUET is supported")
                .hasMessageContaining("nothing was written");
    }

    @Test
    void rejectsDeleteFilesInAFormatThatCannotBeRead() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, records("EU", 1));
        table.newRowDelta().addDeletes(positionDeletes(table, FileFormat.ORC)).commit();

        assertThatThrownBy(() -> reader.plan(table.location(), null))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("has unsupported format ORC");
    }

    @Test
    void rejectsFormatVersionsNewerThanSupported() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned(),
                IcebergMetadataReader.MAX_FORMAT_VERSION + 1);

        assertThatThrownBy(() -> reader.plan(table.location(), null))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("uses format version 4; at most 3 is supported");
    }

    @Test
    void acceptsFormatVersionThree() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned(), 3);
        IcebergTestTables.append(table, records("EU", 1));

        assertThat(reader.plan(table.location(), null).table().formatVersion()).isEqualTo(3);
    }

    @Test
    void rejectsASnapshotThatDoesNotExist() {
        Table table = IcebergTestTables.create(tempDir.resolve("orders"), SCHEMA, PartitionSpec.unpartitioned());
        IcebergTestTables.append(table, records("EU", 1));

        assertThatThrownBy(() -> reader.plan(table.location(), 42L))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessageContaining("Snapshot 42 does not exist");
    }

    @Test
    void reportsAMissingTableClearly() {
        String location = tempDir.resolve("missing").toString();

        assertThatThrownBy(() -> reader.plan(location, null))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessage("Cannot load Iceberg table at " + location);
    }

    private static List<Record> records(String region, long... ids) {
        return records(SCHEMA, region, ids);
    }

    private static List<Record> records(Schema schema, String region, long... ids) {
        return java.util.Arrays.stream(ids).mapToObj(id -> {
            GenericRecord record = GenericRecord.create(schema);
            record.setField("id", id);
            record.setField("region", region);
            record.setField("event_ts", OffsetDateTime.of(2026, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC));
            return (Record) record;
        }).toList();
    }

    /** Delete-file metadata only: planning reads manifests, never the delete file itself. */
    private DeleteFile positionDeletes(Table table, FileFormat format) {
        return FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath(tempDir.resolve("orders/data/deletes." + format.name().toLowerCase()).toString())
                .withFormat(format)
                .withFileSizeInBytes(100)
                .withRecordCount(1)
                .build();
    }
}
