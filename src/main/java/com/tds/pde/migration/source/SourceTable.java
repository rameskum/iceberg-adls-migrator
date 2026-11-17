package com.tds.pde.migration.source;

import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMapping;
import org.jspecify.annotations.Nullable;

/**
 * The source table as pinned for one migration run: everything needed to read its data files.
 *
 * @param snapshotId    pinned snapshot; {@code null} when the table has never been written
 * @param schema        schema as of the pinned snapshot, which every row is read into
 * @param currentSchema the table's current schema, which delete files are resolved against
 * @param nameMapping   maps column names to field ids for data files written without them (imported files)
 */
public record SourceTable(String location, int formatVersion, @Nullable Long snapshotId, Schema schema,
                          Schema currentSchema, FileIO io, @Nullable NameMapping nameMapping) {
}
