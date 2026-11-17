package com.tds.pde.migration.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

/**
 * Builds real Iceberg tables on the local filesystem the same way production tables are laid out on HDFS:
 * path-based {@link HadoopTables} with Parquet data files written by Iceberg's own writer.
 */
public final class IcebergTestTables {

    private IcebergTestTables() {
    }

    public static Table create(Path location, Schema schema, PartitionSpec spec) {
        return create(location, schema, spec, 2);
    }

    public static Table create(Path location, Schema schema, PartitionSpec spec, int formatVersion) {
        return new HadoopTables(new Configuration()).create(schema, spec,
                Map.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)), location.toUri().toString());
    }

    /** Writes the records as one data file in their (shared) partition and commits it in its own snapshot. */
    public static DataFile append(Table table, List<Record> records) {
        DataFile file = writeDataFile(table, records);
        table.newAppend().appendFile(file).commit();
        return file;
    }

    public static DataFile writeDataFile(Table table, List<Record> records) {
        PartitionSpec spec = table.spec();
        PartitionKey partition = null;
        if (!spec.isUnpartitioned()) {
            partition = new PartitionKey(spec, table.schema());
            partition.partition(new InternalRecordWrapper(table.schema().asStruct()).wrap(records.getFirst()));
        }
        OutputFile output = table.io().newOutputFile(
                table.locationProvider().newDataLocation(spec, partition, UUID.randomUUID() + ".parquet"));
        try {
            DataWriter<Record> writer = Parquet.writeData(output)
                    .schema(table.schema())
                    .createWriterFunc(GenericParquetWriter::create)
                    .withSpec(spec)
                    .withPartition(partition)
                    .build();
            try (writer) {
                records.forEach(writer::write);
            }
            return writer.toDataFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
