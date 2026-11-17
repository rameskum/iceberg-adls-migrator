package com.tds.pde.migration.source;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.util.PartitionUtil;
import org.springframework.stereotype.Component;

/**
 * Reads the logical rows of one data file, the way Iceberg's own generic reader does for a single task:
 * position deletes, equality deletes and deletion vectors are applied, identity partition values come from
 * metadata, and files without field ids are resolved through the table's name mapping.
 */
@Component
public class IcebergDataReader {

    /**
     * Opens the file for streaming. Rows are decoded one at a time; only delete sets are held in memory.
     */
    public SourceRecords open(SourceTable table, FileScanTask task) {
        DeleteFilter<Record> deletes = new GenericDeleteFilter(table.io(), task, table.currentSchema(), table.schema());
        Schema readSchema = deletes.requiredSchema();

        ReadBuilder<Record, ?> builder = FormatModelRegistry
                .<Record, Object>readBuilder(task.file().format(), Record.class, table.io().newInputFile(task.file()))
                .project(readSchema)
                .idToConstant(PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant))
                .split(task.start(), task.length())
                .caseSensitive(true)
                .filter(task.residual());
        if (table.nameMapping() != null) {
            builder = builder.withNameMapping(table.nameMapping());
        }
        return new SourceRecords(readSchema, builder.build(), deletes);
    }
}
