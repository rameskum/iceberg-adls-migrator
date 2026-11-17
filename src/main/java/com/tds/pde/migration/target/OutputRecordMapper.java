package com.tds.pde.migration.target;

import com.tds.pde.migration.schema.OutputSchema;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

/**
 * Turns rows as read (which may carry extra columns needed to apply deletes) into rows of the output schema,
 * converting the columns the target declares as string.
 *
 * <p>Reuses one output record: the Parquet writer copies each value as soon as a row is added.
 */
public final class OutputRecordMapper {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");

    private final int[] sourcePositions;
    private final boolean[] toString;
    private final GenericRecord output;

    public OutputRecordMapper(Schema readSchema, OutputSchema outputSchema) {
        List<Types.NestedField> readColumns = readSchema.columns();
        List<Types.NestedField> outputColumns = outputSchema.schema().columns();
        this.sourcePositions = new int[outputColumns.size()];
        this.toString = new boolean[outputColumns.size()];
        for (int i = 0; i < outputColumns.size(); i++) {
            int fieldId = outputColumns.get(i).fieldId();
            sourcePositions[i] = positionOf(readColumns, fieldId);
            toString[i] = outputSchema.stringConversionFieldIds().contains(fieldId);
        }
        this.output = GenericRecord.create(outputSchema.schema());
    }

    public Record map(Record row) {
        for (int i = 0; i < sourcePositions.length; i++) {
            Object value = row.get(sourcePositions[i]);
            output.set(i, toString[i] && value != null ? asString(value) : value);
        }
        return output;
    }

    private static String asString(Object value) {
        return switch (value) {
            case UUID uuid -> uuid.toString();
            case LocalTime time -> TIME.format(time);
            default -> throw new IllegalStateException(
                    "No string conversion for " + value.getClass().getName());
        };
    }

    private static int positionOf(List<Types.NestedField> columns, int fieldId) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).fieldId() == fieldId) {
                return i;
            }
        }
        throw new IllegalStateException("Output column with field id " + fieldId + " is not in the read schema");
    }
}
