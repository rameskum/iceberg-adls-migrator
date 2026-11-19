package com.tds.pde.migration.source;

import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.mapping.NameMapping;
import org.jspecify.annotations.Nullable;

/**
 * The source table as pinned for one migration run: everything needed to read its data files.
 *
 * @param snapshotId    pinned snapshot; {@code null} when the table has never been written
 * @param schema        schema every row is read into
 * @param deleteSchema  where equality delete files find their key columns: the table's current schema plus every
 *                      column dropped from it since, because a delete file written before a drop still applies
 * @param nameMapping   maps column names to field ids for data files written without them (imported files)
 */
public record SourceTable(String location, int formatVersion, @Nullable Long snapshotId, Schema schema,
                          Schema deleteSchema, FileIO io, @Nullable NameMapping nameMapping) {

    /** The same table read into another schema, e.g. the one a resumed run pinned when it started. */
    public SourceTable withSchema(Schema readSchema) {
        return new SourceTable(location, formatVersion, snapshotId, readSchema, deleteSchema, io, nameMapping);
    }
}
