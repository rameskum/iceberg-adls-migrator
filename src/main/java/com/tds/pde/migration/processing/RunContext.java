package com.tds.pde.migration.processing;

import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.source.SourceTable;

/**
 * Inputs shared by every file of one migration run.
 */
public record RunContext(String runId, SourceTable source, OutputSchema outputSchema) {
}
