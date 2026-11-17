package com.tds.pde.migration.processing;

import com.tds.pde.migration.destination.BlobStore;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The metadata committed with every destination blob, which makes a published blob its own proof of completion:
 * the blob and its metadata appear in one atomic commit, so a blob carrying this run's markers is a finished file
 * even if recording the completion in the state store failed afterwards.
 */
public final class PublishedFile {

    // Blob metadata names must be C# identifiers and values ASCII, so locations are URL-encoded.
    static final String RUN_ID = "migration_run_id";
    static final String SOURCE_FILE = "source_file";
    static final String SOURCE_PARTITION = "source_partition";
    static final String SOURCE_SNAPSHOT_ID = "source_snapshot_id";
    static final String PHYSICAL_RECORDS = "physical_records";
    static final String RECORDS_WRITTEN = "records_written";

    private PublishedFile() {
    }

    public record Counts(long physicalRowsRead, long recordsWritten) {
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

    /**
     * @return the counts recorded at publish time when the task's destination blob was published by this run
     *         from this source file at this snapshot; empty otherwise
     */
    public static Optional<Counts> find(BlobStore blobStore, RunContext run, FileTask task) {
        return blobStore.metadata(task.destination())
                .filter(metadata -> Objects.equals(metadata.get(RUN_ID), run.runId())
                        && Objects.equals(metadata.get(SOURCE_FILE), encode(task.source().location()))
                        && Objects.equals(metadata.get(SOURCE_SNAPSHOT_ID), String.valueOf(run.source().snapshotId()))
                        && Objects.equals(metadata.get(PHYSICAL_RECORDS), String.valueOf(task.source().recordCount()))
                        && metadata.get(RECORDS_WRITTEN) != null)
                .map(metadata -> new Counts(Long.parseLong(metadata.get(PHYSICAL_RECORDS)),
                        Long.parseLong(metadata.get(RECORDS_WRITTEN))));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
