package com.tds.pde.migration.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

/** Reads a destination file back, both through Iceberg and through plain parquet-hadoop with no Iceberg. */
public final class ParquetOutput {

    private final Path file;

    private ParquetOutput(Path file) {
        this.file = file;
    }

    public static ParquetOutput of(Path file) {
        return new ParquetOutput(file);
    }

    /** Rows according to the footer, without reading any data. */
    public long rowCount() {
        return footer().getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();
    }

    public static ParquetOutput of(byte[] content, Path directory) {
        try {
            return new ParquetOutput(Files.write(Files.createTempFile(directory, "output", ".parquet"), content));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Rows decoded into Iceberg's generic representation, for exact value comparisons. */
    public List<Record> records(Schema schema) {
        List<Record> records = new ArrayList<>();
        try (CloseableIterable<Record> rows = Parquet.read(org.apache.iceberg.Files.localInput(file.toFile()))
                .project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {
            rows.forEach(records::add);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return records;
    }

    /** Rows read with parquet-hadoop's example reader, which knows nothing about Iceberg. */
    public List<Group> standaloneRows() {
        List<Group> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(),
                new org.apache.hadoop.fs.Path(file.toUri())).build()) {
            for (Group row = reader.read(); row != null; row = reader.read()) {
                rows.add(row);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return rows;
    }

    public MessageType parquetSchema() {
        return footer().getFileMetaData().getSchema();
    }

    public Map<String, String> footerMetadata() {
        return footer().getFileMetaData().getKeyValueMetaData();
    }

    private ParquetMetadata footer() {
        try (ParquetFileReader reader = ParquetFileReader.open(
                HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toUri()), new Configuration()))) {
            return reader.getFooter();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
