package com.tds.pde.migration.source;

import java.util.List;
import org.apache.iceberg.Schema;
import org.jspecify.annotations.Nullable;

/**
 * Everything to migrate from one pinned snapshot, in processing order.
 *
 * @param snapshotId   pinned snapshot; {@code null} when the table has never been written
 * @param schema       table schema as of the pinned snapshot
 * @param totalRecords physical rows across all data files, before deletes
 */
public record MigrationPlan(String tableLocation, int formatVersion, @Nullable Long snapshotId, Schema schema,
                            List<PlannedPartition> partitions, long totalRecords, long totalFiles,
                            long totalDeleteFiles) {

    public MigrationPlan {
        partitions = List.copyOf(partitions);
    }

    static MigrationPlan empty(String tableLocation, int formatVersion, @Nullable Long snapshotId, Schema schema) {
        return new MigrationPlan(tableLocation, formatVersion, snapshotId, schema, List.of(), 0, 0, 0);
    }

    public long totalPartitions() {
        return partitions.size();
    }
}
