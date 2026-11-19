package com.tds.pde.migration.target;

import com.tds.pde.migration.config.MigrationProperties;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.io.PositionOutputStream;
import org.springframework.stereotype.Component;

/**
 * Writes standalone Parquet files with Iceberg's Parquet writer, which maps every Iceberg type exactly.
 * The files carry Parquet field ids but no Iceberg table metadata.
 */
@Component
public class ParquetDataWriter {

    /** Footer key under which Iceberg's writer stores the schema; repeated for empty files to keep them alike. */
    private static final String ICEBERG_SCHEMA_KEY = "iceberg.schema";
    private static final String MESSAGE_NAME = "table";

    private final MigrationProperties.Parquet settings;

    public ParquetDataWriter(MigrationProperties properties) {
        this.settings = properties.parquet();
    }

    /**
     * Opens a writer into the upload. Closing the writer writes the footer and stages the last bytes; it does not
     * publish the blob. A writer that receives no rows still produces a valid Parquet file with the schema.
     *
     * @param footerMetadata key-value pairs stored in the Parquet footer
     */
    public FileAppender<Record> open(StagedUpload upload, Schema schema, Map<String, String> footerMetadata)
            throws IOException {
        StagedOutputFile output = new StagedOutputFile(upload);
        Parquet.WriteBuilder builder = Parquet.write(output)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::create)
                .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, String.valueOf(settings.rowGroupSize().toBytes()))
                .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, String.valueOf(settings.pageSize().toBytes()))
                .set(TableProperties.PARQUET_COMPRESSION, settings.compression().name().toLowerCase(Locale.ROOT))
                .overwrite();
        footerMetadata.forEach(builder::meta);
        return new EmptyFileAwareAppender(builder.build(), output, upload, schema, footerMetadata);
    }

    /**
     * Iceberg's writer creates no file at all when it receives no rows. A source file whose rows were all deleted
     * must still produce its target file, so the empty file is written directly.
     */
    private final class EmptyFileAwareAppender implements FileAppender<Record> {

        private final FileAppender<Record> delegate;
        private final StagedOutputFile output;
        private final StagedUpload upload;
        private final Schema schema;
        private final Map<String, String> footerMetadata;
        private long rows;
        private long emptyFileLength;

        EmptyFileAwareAppender(FileAppender<Record> delegate, StagedOutputFile output, StagedUpload upload,
                               Schema schema, Map<String, String> footerMetadata) {
            this.delegate = delegate;
            this.output = output;
            this.upload = upload;
            this.schema = schema;
            this.footerMetadata = footerMetadata;
        }

        @Override
        public void add(Record row) {
            delegate.add(row);
            rows++;
        }

        @Override
        public Metrics metrics() {
            return delegate.metrics();
        }

        /** Bytes written to the target so far: the row groups already flushed, then the whole file once closed. */
        @Override
        public long length() {
            return emptyFileLength > 0 ? emptyFileLength : output.bytesWritten();
        }

        @Override
        public List<Long> splitOffsets() {
            return delegate.splitOffsets();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
            if (rows == 0) {
                writeEmptyFile();
            }
        }

        private void writeEmptyFile() throws IOException {
            Map<String, String> metadata = new HashMap<>(footerMetadata);
            metadata.put(ICEBERG_SCHEMA_KEY, SchemaParser.toJson(schema));
            ParquetFileWriter writer = new ParquetFileWriter(new UploadOutputFile(upload),
                    ParquetSchemaUtil.convert(schema, MESSAGE_NAME), ParquetFileWriter.Mode.OVERWRITE,
                    settings.rowGroupSize().toBytes(), 0);
            writer.start();
            writer.end(metadata);
            emptyFileLength = writer.getPos();
        }
    }

    private record UploadOutputFile(StagedUpload upload) implements org.apache.parquet.io.OutputFile {

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            OutputStream delegate = upload.outputStream();
            return new PositionOutputStream() {
                private long position;

                @Override
                public long getPos() {
                    return position;
                }

                @Override
                public void write(int b) throws IOException {
                    delegate.write(b);
                    position++;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    delegate.write(b, off, len);
                    position += len;
                }

                @Override
                public void close() throws IOException {
                    delegate.close();
                }
            };
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }

        @Override
        public String getPath() {
            return upload.blobName();
        }
    }
}
