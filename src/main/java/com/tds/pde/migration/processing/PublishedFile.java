package com.tds.pde.migration.processing;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * The metadata committed with every target blob, in the same atomic commit as its content: which run published it,
 * from which source file and snapshot, and the rows it read and wrote.
 */
final class PublishedFile {

    // Blob metadata names must be C# identifiers and values ASCII, so locations are URL-encoded.
    static final String RUN_ID = "migration_run_id";
    static final String SOURCE_FILE = "source_file";
    static final String SOURCE_PARTITION = "source_partition";
    static final String SOURCE_SNAPSHOT_ID = "source_snapshot_id";
    static final String PHYSICAL_RECORDS = "physical_records";
    static final String RECORDS_WRITTEN = "records_written";

    private PublishedFile() {
    }

    static Map<String, String> metadata(RunContext run, FileTask task, long physicalRowsRead, long recordsWritten) {
        return Map.of(
                RUN_ID, run.runId(),
                SOURCE_FILE, encode(task.source().location()),
                SOURCE_PARTITION, encode(task.partitionPath()),
                SOURCE_SNAPSHOT_ID, String.valueOf(run.source().snapshotId()),
                PHYSICAL_RECORDS, String.valueOf(physicalRowsRead),
                RECORDS_WRITTEN, String.valueOf(recordsWritten));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
