package com.tds.pde.migration.schema;

import java.util.Set;
import org.apache.iceberg.Schema;

/**
 * Schema every destination Parquet file is written with.
 *
 * @param schema                 source schema at the pinned snapshot, with converted columns typed as string
 * @param stringConversionFieldIds top-level uuid or time columns the destination declares as string
 */
public record OutputSchema(Schema schema, Set<Integer> stringConversionFieldIds) {

    public OutputSchema {
        stringConversionFieldIds = Set.copyOf(stringConversionFieldIds);
    }

    public static OutputSchema unchanged(Schema schema) {
        return new OutputSchema(schema, Set.of());
    }
}
