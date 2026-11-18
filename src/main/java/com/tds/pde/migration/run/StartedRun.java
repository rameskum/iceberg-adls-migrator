package com.tds.pde.migration.run;

import com.tds.pde.migration.processing.RunContext;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.RunRecord;
import java.util.List;
import java.util.Map;

/**
 * A run that is recorded in the state store and ready to process: new, or resumed with its pinned snapshot,
 * schemas and file list.
 *
 * @param fileStates stored state of every file by source path; all PENDING for a new run
 */
public record StartedRun(RunRecord run, RunContext context, MigrationPlan plan, List<PartitionTasks> partitions,
                         Map<String, FileRecord> fileStates, boolean resumed) {

    public StartedRun {
        partitions = List.copyOf(partitions);
        fileStates = Map.copyOf(fileStates);
    }

    public String runId() {
        return run.runId();
    }
}
