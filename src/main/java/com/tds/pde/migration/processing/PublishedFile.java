package com.tds.pde.migration.processing;

import com.tds.pde.migration.target.BlobStore;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The metadata committed with every target blob, which makes a published blob its own proof of completion:
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
     * Finds which tasks are already done with one listing of the blobs under {@code targetPrefix}, matched in
     * memory: a task is done when its target blob was published by this run from its source file at this snapshot.
     *
     * @return the counts recorded at publish time, by source path of the done tasks
     */
    public static Map<String, Counts> find(BlobStore blobStore, String targetPrefix, RunContext run,
                                           Collection<FileTask> tasks) {
        Map<String, FileTask> byTarget = tasks.stream()
                .collect(Collectors.toMap(FileTask::target, Function.identity()));
        Map<String, Counts> published = new HashMap<>();
        try (Stream<BlobStore.BlobMetadata> blobs = blobStore.listWithMetadata(targetPrefix)) {
            blobs.forEach(blob -> {
                FileTask task = byTarget.get(blob.name());
                if (task != null) {
                    counts(blob.metadata(), run, task)
                            .ifPresent(counts -> published.put(task.source().location(), counts));
                }
            });
        }
        return published;
    }

    private static Optional<Counts> counts(Map<String, String> metadata, RunContext run, FileTask task) {
        if (Objects.equals(metadata.get(RUN_ID), run.runId())
                && Objects.equals(metadata.get(SOURCE_FILE), encode(task.source().location()))
                && Objects.equals(metadata.get(SOURCE_SNAPSHOT_ID), String.valueOf(run.source().snapshotId()))
                && Objects.equals(metadata.get(PHYSICAL_RECORDS), String.valueOf(task.source().recordCount()))
                && metadata.get(RECORDS_WRITTEN) != null) {
            return Optional.of(new Counts(Long.parseLong(metadata.get(PHYSICAL_RECORDS)),
                    Long.parseLong(metadata.get(RECORDS_WRITTEN))));
        }
        return Optional.empty();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
