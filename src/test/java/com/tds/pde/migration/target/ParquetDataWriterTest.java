package com.tds.pde.migration.target;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.tds.pde.migration.testing.InMemoryBlobStore;
import com.tds.pde.migration.testing.ParquetOutput;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetDataWriterTest {

    private static final Schema SCHEMA = new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "payload", Types.StringType.get()));

    @TempDir
    Path tempDir;

    @Test
    void startsANewRowGroupWhenTheConfiguredRowGroupSizeIsReached() throws IOException {
        // About 2 MB of incompressible rows.
        assertThat(rowGroupsWritten(Map.of("migration.parquet.row-group-size", "256KB"))).isGreaterThan(2);
    }

    @Test
    void keepsSmallFilesInOneRowGroupUnderTheDefaultRowGroupSize() throws IOException {
        assertThat(rowGroupsWritten(Map.of())).isEqualTo(1);
    }

    private int rowGroupsWritten(Map<String, String> settings) throws IOException {
        InMemoryBlobStore store = new InMemoryBlobStore();
        StagedUpload upload = store.startUpload("part.parquet");
        Random random = new Random(7);
        FileAppender<Record> appender = new ParquetDataWriter(TestProperties.bind(settings))
                .open(upload, SCHEMA, Map.of());
        try (appender) {
            for (long id = 0; id < 2000; id++) {
                byte[] payload = new byte[1000];
                random.nextBytes(payload);
                GenericRecord row = GenericRecord.create(SCHEMA);
                row.setField("id", id);
                row.setField("payload", java.util.Base64.getEncoder().encodeToString(payload));
                appender.add(row);
            }
        }
        upload.commit(Map.of());
        return ParquetOutput.of(store.blob("part.parquet").orElseThrow().content(), tempDir).rowGroupCount();
    }
}
