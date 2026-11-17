package com.tds.pde.migration.run;

import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.source.PlannedPartition;
import java.util.List;

/**
 * The file tasks of one source partition, in processing order.
 */
public record PartitionTasks(PlannedPartition partition, List<FileTask> files) {

    public PartitionTasks {
        files = List.copyOf(files);
    }
}
