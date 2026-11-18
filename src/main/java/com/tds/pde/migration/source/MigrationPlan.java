package com.tds.pde.migration.source;

import java.util.List;

/**
 * Everything to migrate from one pinned snapshot, in processing order.
 *
 * @param totalRecords physical rows across all data files, before deletes
 */
public record MigrationPlan(SourceTable table, List<PlannedPartition> partitions, long totalRecords,
                            long totalFiles, long totalDeleteFiles) {

    public MigrationPlan {
        partitions = List.copyOf(partitions);
    }

    public MigrationPlan withTable(SourceTable newTable) {
        return new MigrationPlan(newTable, partitions, totalRecords, totalFiles, totalDeleteFiles);
    }

    static MigrationPlan empty(SourceTable table) {
        return new MigrationPlan(table, List.of(), 0, 0, 0);
    }

    public long totalPartitions() {
        return partitions.size();
    }
}
