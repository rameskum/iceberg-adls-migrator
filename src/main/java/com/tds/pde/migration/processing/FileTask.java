package com.tds.pde.migration.processing;

import com.tds.pde.migration.source.SourceFile;

/**
 * One independent unit of work: copy one source data file to one target blob.
 *
 * @param partitionPath Hive-style source partition path; empty when unpartitioned
 * @param target        blob name of the output file
 */
public record FileTask(SourceFile source, String partitionPath, String target) {
}
