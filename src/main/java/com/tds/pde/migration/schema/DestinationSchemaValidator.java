package com.tds.pde.migration.schema;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.dataset.DatasetSchema;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

/**
 * Decides the output schema: the source schema unchanged, except top-level uuid or time columns that the
 * destination dataset declares as {@code string}. Any other difference fails the run before anything is written.
 */
@Slf4j
@Component
public class DestinationSchemaValidator {

    public OutputSchema resolve(Schema source, @Nullable DatasetSchema destination) {
        OutputSchema output = destination == null
                ? OutputSchema.unchanged(source)
                : validate(source, destination);
        warnAboutSparkUnreadableColumns(output.schema());
        return output;
    }

    private static OutputSchema validate(Schema source, DatasetSchema destination) {
        List<String> problems = new ArrayList<>();
        Map<String, DatasetSchema.Field> declared = new LinkedHashMap<>();
        for (DatasetSchema.Field field : destination.fields()) {
            if (declared.putIfAbsent(field.name(), field) != null) {
                problems.add("destination declares column '" + field.name() + "' more than once");
            }
        }

        List<Types.NestedField> outputColumns = new ArrayList<>();
        Set<Integer> conversions = new HashSet<>();
        for (Types.NestedField column : source.columns()) {
            DatasetSchema.Field target = declared.remove(column.name());
            if (target == null) {
                problems.add("column '" + column.name() + "' is missing from the destination schema");
                continue;
            }
            String sourceType = SparkTypes.render(column.type());
            String targetType;
            try {
                targetType = SparkTypes.normalize(target.type());
            } catch (IllegalArgumentException e) {
                problems.add("column '" + column.name() + "': " + e.getMessage());
                continue;
            }

            if (targetType.equals(sourceType)) {
                outputColumns.add(column);
            } else if (targetType.equals("string") && isConvertibleToString(column.type())) {
                conversions.add(column.fieldId());
                outputColumns.add(Types.NestedField.of(column.fieldId(), column.isOptional(), column.name(),
                        Types.StringType.get(), column.doc()));
            } else {
                problems.add("column '" + column.name() + "' is " + sourceType + " in the source but "
                        + targetType + " in the destination");
            }

            if (column.isOptional() && !target.nullable()) {
                problems.add("column '" + column.name() + "' is nullable in the source but not in the destination");
            }
        }
        declared.keySet().forEach(name ->
                problems.add("destination column '" + name + "' does not exist in the source"));

        if (!problems.isEmpty()) {
            throw new MigrationFailedException("Destination schema does not match the source schema; nothing was "
                    + "written:\n  " + String.join("\n  ", problems));
        }
        if (!conversions.isEmpty()) {
            log.info("Columns converted to string as the destination schema declares: columns={}",
                    conversions.stream().map(id -> source.findColumnName(id)).sorted().toList());
        }
        return new OutputSchema(new Schema(outputColumns, source.identifierFieldIds()), conversions);
    }

    private static boolean isConvertibleToString(Type type) {
        return type.typeId() == Type.TypeID.UUID || type.typeId() == Type.TypeID.TIME;
    }

    private static void warnAboutSparkUnreadableColumns(Schema schema) {
        List<String> unreadable = schema.columns().stream()
                .filter(column -> SparkTypes.isSparkUnreadable(column.type()))
                .map(column -> column.name() + ":" + column.type())
                .toList();
        if (!unreadable.isEmpty()) {
            log.warn("Output columns Databricks/Spark cannot read; declare them as string in the destination schema "
                    + "to convert them: columns={}", unreadable);
        }
    }
}
