package com.tds.pde.migration.run;

import com.tds.pde.migration.processing.RunContext;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.state.ControlRecord;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * A new run that has claimed its migration's control row and is ready to prepare the target and copy its files.
 *
 * @param control  the control row as the run claimed it
 * @param previous the control row of the run before, which this run replaces; {@code null} on the first run
 */
public record StartedRun(ControlRecord control, @Nullable ControlRecord previous, RunContext context,
                         MigrationPlan plan, List<PartitionTasks> partitions) {

    public StartedRun {
        partitions = List.copyOf(partitions);
    }

    public String runId() {
        return control.runId();
    }
}
