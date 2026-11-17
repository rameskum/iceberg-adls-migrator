package com.tds.pde.migration.processing;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.tds.pde.migration.target.TargetPathResolver;
import com.tds.pde.migration.target.ParquetDataWriter;
import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.source.SourceFile;
import com.tds.pde.migration.testing.IcebergTestTables;
import com.tds.pde.migration.testing.LocalFileBlobStore;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Migrates a source file larger than the whole heap. Runs in its own JVM with {@code -Xmx128m} (see the
 * {@code bounded-memory} surefire execution), so holding the file in memory would fail with OutOfMemoryError.
 */
@Tag("memory")
class BoundedMemoryTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "payload", Types.StringType.get()));
    private static final long ROWS = 2_500_000;
    private static final int PAYLOAD_LENGTH = 100;

    @TempDir
    Path tempDir;

    @Test
    void migratesAFileLargerThanTheHeapWithBoundedMemory() throws IOException {
        Table table = IcebergTestTables.create(tempDir.resolve("big"), SCHEMA, PartitionSpec.unpartitioned());
        table.updateProperties().set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(8 << 20)).commit();
        DataFile source = writeRandomRows(table);
        table.newAppend().appendFile(source).commit();
        long maxHeap = Runtime.getRuntime().maxMemory();
        assertThat(source.fileSizeInBytes()).as("source file must exceed the heap").isGreaterThan(maxHeap);

        MigrationPlan plan = new IcebergMetadataReader(new Configuration()).plan(table.location(), null);
        SourceFile file = plan.partitions().getFirst().files().getFirst();
        FileTask task = new FileTask(file, "", new TargetPathResolver().resolve("out", "", file.location()));
        LocalFileBlobStore blobStore = new LocalFileBlobStore(tempDir.resolve("target"));
        DataFileProcessor processor = new DataFileProcessor(new IcebergDataReader(),
                new ParquetDataWriter(TestProperties.bind(Map.of("migration.parquet.row-group-size", "16MB"))),
                blobStore);

        FileResult result = processor.process(new RunContext("run-1", plan.table(), OutputSchema.unchanged(SCHEMA)),
                task, FileProgressListener.NONE);

        assertThat(result.error()).isNull();
        assertThat(result.recordsWritten()).isEqualTo(ROWS);
        Path output = blobStore.file(task.target());
        assertThat(Files.size(output)).as("output file must exceed the heap").isGreaterThan(maxHeap);
        assertThat(ParquetOutput.of(output).rowCount()).isEqualTo(ROWS);
    }

    /** Streams random, incompressible rows into one data file without holding them. */
    private static DataFile writeRandomRows(Table table) throws IOException {
        Random random = new Random(42);
        char[] alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();
        char[] payload = new char[PAYLOAD_LENGTH];
        GenericRecord row = GenericRecord.create(SCHEMA);
        DataWriter<Record> writer = Parquet.writeData(
                        table.io().newOutputFile(table.locationProvider().newDataLocation("big.parquet")))
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build();
        try (writer) {
            for (long id = 0; id < ROWS; id++) {
                for (int i = 0; i < PAYLOAD_LENGTH; i++) {
                    payload[i] = alphabet[random.nextInt(alphabet.length)];
                }
                row.setField("id", id);
                row.setField("payload", new String(payload));
                writer.write(row);
            }
        }
        return writer.toDataFile();
    }
}
