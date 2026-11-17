package com.tds.pde.migration.source;

import java.util.List;

/**
 * One source partition: a partition spec plus partition values, and its data files at the pinned snapshot.
 *
 * @param path Hive-style partition path from {@code PartitionSpec.partitionToPath}; empty when unpartitioned
 */
public record PlannedPartition(int specId, String path, List<SourceFile> files) {

    public PlannedPartition {
        files = List.copyOf(files);
    }

    public String displayName() {
        return path.isEmpty() ? "<unpartitioned>" : path;
    }

    public long recordCount() {
        return files.stream().mapToLong(SourceFile::recordCount).sum();
    }
}
