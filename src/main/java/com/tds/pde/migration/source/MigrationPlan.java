package com.tds.pde.migration.source;

import java.util.List;

/**
 * Everything to migrate from one pinned snapshot, in processing order.
 *
 * @param totalRecords   physical rows across all data files, before deletes
 * @param totalBytes     size of all data files
 * @param partitionSpecs partition specs the data files use, e.g. {@code 0:[region: identity(2)]}
 */
public record MigrationPlan(SourceTable table, List<PlannedPartition> partitions, long totalRecords,
                            long totalFiles, long totalDeleteFiles, long totalBytes, String partitionSpecs) {

    public MigrationPlan {
        partitions = List.copyOf(partitions);
    }

    public MigrationPlan withTable(SourceTable newTable) {
        return new MigrationPlan(newTable, partitions, totalRecords, totalFiles, totalDeleteFiles, totalBytes,
                partitionSpecs);
    }

    static MigrationPlan empty(SourceTable table) {
        return new MigrationPlan(table, List.of(), 0, 0, 0, 0, "");
    }

    public long totalPartitions() {
        return partitions.size();
    }
}
