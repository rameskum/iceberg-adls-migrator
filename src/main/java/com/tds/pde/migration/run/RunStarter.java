package com.tds.pde.migration.run;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.processing.RunContext;
import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.schema.TargetSchemaValidator;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.source.PlannedPartition;
import com.tds.pde.migration.source.SourceFile;
import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.TargetPathResolver;
import com.tds.pde.migration.target.azure.AzureLocation;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

/**
 * Starts a new run of a migration: decides whether it may start, claims the migration's control row, and then
 * prepares the target. Every run starts from the beginning under a new run id; there is no resuming.
 *
 * <p>A run may start when the migration has no run yet, or its latest run COMPLETED, and the target is empty. After
 * a run that did not complete (FAILED, or RUNNING with a stale heartbeat because it died), or into a target that
 * holds files, it only starts with {@code migration.target.clean=true}, which deletes the target's files first.
 * Clean is refused while the latest run is RUNNING with a fresh heartbeat: it may still be writing.
 *
 * <p>The claim is a conditional write of the control row: an insert when the migration has none, otherwise an
 * update that only applies while the row is still as it was read. Of two runs racing, Delta lets one commit and the
 * other's write, retried after the conflict, no longer applies. The row is then read back, and the claim only holds
 * when it is the migration's single row and names this run: two first runs inserting at once can both commit, as
 * neither updates an existing row, and then only one of them can see a single row.
 *
 * <p>Targets of different migrations must not overlap: a target that is the same folder as another migration's
 * target, or is within it or contains it, would have its files deleted by the other's clean, or would hold files
 * the other did not write. The control table knows every target, so a run that would overlap one is refused before
 * it claims the migration, and checked again before it writes the target, after any other run that started at the
 * same moment has claimed its own.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RunStarter {

    private static final DateTimeFormatter RUN_ID_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final MigrationProperties properties;
    private final MigrationStateRepository state;
    private final IcebergMetadataReader metadataReader;
    private final TargetSchemaValidator schemaValidator;
    private final TargetPathResolver pathResolver;

    /**
     * Checks that a run may start, plans it, and claims the migration for it. Writes nothing when it refuses.
     *
     * @throws MigrationFailedException when the run may not start, or another run claimed the migration first
     */
    public StartedRun start(DatasetDescriptor source, DatasetDescriptor target, AzureLocation location,
                            BlobStore blobStore) {
        boolean clean = properties.target().clean();
        ControlRecord previous = latestRun(target.datasetId());
        if (previous != null) {
            requireStartable(previous, clean);
        }
        if (clean && location.isContainerRoot()) {
            throw new MigrationFailedException("Refusing to clean " + location
                    + ": it is a whole container. Point the target at a folder within it.");
        }
        requireNoOverlappingTarget(target.datasetId(), location, "nothing was written");

        MigrationPlan plan = metadataReader.plan(source.location(), properties.source().snapshotId());
        OutputSchema outputSchema = schemaValidator.resolve(plan.table().schema(), target.schema());
        List<PartitionTasks> tasks = tasks(plan, location);
        if (!clean) {
            requireEmptyTarget(location, blobStore, previous);
        }

        Instant now = Instant.now();
        ControlRecord claimed = new ControlRecord(target.datasetId(), source.datasetId(), source.location(),
                target.location(), newRunId(), ControlRecord.Status.RUNNING, plan.table().snapshotId(), now, now,
                null, plan.totalFiles(), plan.totalRecords(), ControlRecord.Progress.NONE, null);
        claim(previous, claimed);
        log.info("Run claimed the migration: runId={} previousRunId={} previousStatus={} clean={}",
                claimed.runId(), previous == null ? "<none>" : previous.runId(),
                previous == null ? "<none>" : previous.status(), clean);
        return new StartedRun(claimed, previous, new RunContext(claimed.runId(), plan.table(), outputSchema), plan,
                tasks);
    }

    /**
     * Gets the target ready for the claimed run: with clean, deletes every file under it; then replaces the
     * previous run's file rows, whose files are gone, with this run's, all PENDING. Stops as soon as the run no
     * longer holds its claim, so a run that lost the migration never deletes what the run that took it over wrote.
     *
     * @throws ClaimLostException when the run lost its claim, or could not renew its heartbeat for too long
     */
    public void prepare(StartedRun run, RunClaim claim, AzureLocation location, BlobStore blobStore) {
        requireNoOverlappingTarget(run.control().targetDatasetId(), location, "nothing was written to the target");
        if (properties.target().clean()) {
            clean(claim, location, blobStore);
        }
        claim.ensureHeld();
        if (run.previous() != null) {
            long deleted = state.deleteFiles(run.previous().runId());
            log.info("File rows of the previous run deleted: runId={} rows={}", run.previous().runId(),
                    deleted < 0 ? "unknown" : deleted);
        }
        List<FileRecord> files = pendingFiles(run.runId(), run.partitions());
        claim.ensureHeld();
        state.insertFiles(files);
        log.info("Run started: runId={} snapshotId={} files={}", run.runId(), run.plan().table().snapshotId(),
                files.size());
    }

    private @Nullable ControlRecord latestRun(String targetDatasetId) {
        List<ControlRecord> rows = state.findControls(targetDatasetId);
        if (rows.size() > 1) {
            throw new MigrationFailedException("The control table holds " + rows.size() + " rows for target dataset "
                    + targetDatasetId + " (runs " + rows.stream().map(ControlRecord::runId).toList() + "), left by "
                    + "two runs that started at the same moment. Delete the rows of the runs that are not running, "
                    + "then start again; nothing was written.");
        }
        return rows.isEmpty() ? null : rows.getFirst();
    }

    private void requireStartable(ControlRecord previous, boolean clean) {
        Duration timeout = properties.state().heartbeatTimeout();
        Instant now = Instant.now();
        if (previous.isLive(now, timeout)) {
            throw new MigrationFailedException("Run " + previous.runId() + " of this migration is still running: "
                    + "its heartbeat was renewed " + ago(previous.heartbeatAt(), now) + " ago. Only one run may "
                    + "write a target" + (clean ? ", and migration.target.clean=true is refused until that run's "
                    + "heartbeat is older than migration.state.heartbeat-timeout (" + timeout + ")" : "")
                    + "; nothing was written.");
        }
        if (previous.status() != ControlRecord.Status.COMPLETED && !clean) {
            String how = previous.status() == ControlRecord.Status.FAILED
                    ? "failed" + (previous.error() == null ? "" : " (" + previous.error() + ")")
                    : "stopped without finishing: its heartbeat was last renewed " + ago(previous.heartbeatAt(), now)
                            + " ago";
            throw new MigrationFailedException("Run " + previous.runId() + " of this migration " + how + ". Set "
                    + "migration.target.clean=true to delete what it wrote and start again from the beginning; "
                    + "nothing was written.");
        }
    }

    private void requireNoOverlappingTarget(String targetDatasetId, AzureLocation location, String outcome) {
        for (ControlRecord other : state.findOtherControls(targetDatasetId)) {
            AzureLocation otherLocation;
            try {
                otherLocation = AzureLocation.parse(other.targetLocation());
            } catch (IllegalArgumentException e) {
                log.warn("Control row with an unreadable target location, not checked for overlap: "
                        + "targetDataset={} targetLocation={}", other.targetDatasetId(), other.targetLocation());
                continue;
            }
            if (location.overlaps(otherLocation)) {
                // Of two overlapping folders, the shorter contains the longer.
                String relation = location.prefix().equals(otherLocation.prefix()) ? "is the same folder as"
                        : location.prefix().length() < otherLocation.prefix().length() ? "contains" : "is within";
                throw new MigrationFailedException("Target " + location + " " + relation + " the target "
                        + other.targetLocation() + " of target dataset " + other.targetDatasetId() + ", so a clean of "
                        + "either would delete the other's files. Give each target dataset a folder of its own, "
                        + "neither within the other; " + outcome + ".");
            }
        }
    }

    private void requireEmptyTarget(AzureLocation location, BlobStore blobStore, @Nullable ControlRecord previous) {
        List<BlobStore.BlobInfo> existing = blobStore.list(location.blobPrefix());
        if (existing.isEmpty()) {
            return;
        }
        String owner = previous == null ? "" : " written by run " + previous.runId() + " (" + previous.status() + ")";
        throw new MigrationFailedException("Target " + location + " already holds " + existing.size() + " files"
                + owner + ". Set migration.target.clean=true to delete them and start again from the beginning; "
                + "nothing was written.");
    }

    private void claim(@Nullable ControlRecord previous, ControlRecord claimed) {
        if (previous == null) {
            state.insertControl(claimed);
        } else {
            state.replaceControl(previous, claimed);
        }
        List<ControlRecord> rows = state.findControls(claimed.targetDatasetId());
        if (rows.size() == 1 && rows.getFirst().runId().equals(claimed.runId())) {
            return;
        }
        if (rows.stream().anyMatch(row -> row.runId().equals(claimed.runId()))) {
            // Both first runs inserted a row; this one cannot tell it was first, so it backs off.
            state.deleteControl(claimed.targetDatasetId(), claimed.runId());
        }
        throw new MigrationFailedException("Another run claimed this migration at the same time ("
                + rows.stream().map(ControlRecord::runId).filter(id -> !id.equals(claimed.runId())).toList()
                + "); only one run may write a target. Nothing was written.");
    }

    private void clean(RunClaim claim, AzureLocation location, BlobStore blobStore) {
        List<BlobStore.BlobInfo> blobs = blobStore.list(location.blobPrefix());
        long bytes = blobs.stream().mapToLong(BlobStore.BlobInfo::size).sum();
        log.warn("Cleaning the target before starting again: target={} blobs={} bytes={}", location, blobs.size(),
                bytes);
        for (BlobStore.BlobInfo blob : blobs) {
            claim.ensureHeld();
            blobStore.delete(blob.name());
        }
        claim.ensureHeld();
        blobStore.deleteUploadLeftovers(location.blobPrefix());
        log.info("Target cleaned: target={} deletedBlobs={}", location, blobs.size());
    }

    /**
     * The target of every source file. Names are shortened to fit and carry only a short hash of the source, so two
     * files could get the same name, and the second would overwrite the first; the plan fails instead.
     */
    private List<PartitionTasks> tasks(MigrationPlan plan, AzureLocation location) {
        List<PartitionTasks> partitions = new ArrayList<>(plan.partitions().size());
        Map<String, String> sourceByTarget = new HashMap<>();
        for (PlannedPartition partition : plan.partitions()) {
            List<FileTask> files = new ArrayList<>(partition.files().size());
            for (SourceFile file : partition.files()) {
                String target = pathResolver.resolve(location.prefix(), partition.path(), file.location());
                String clash = sourceByTarget.putIfAbsent(target, file.location());
                if (clash != null) {
                    throw new MigrationFailedException("Source files " + clash + " and " + file.location()
                            + " would both be written to " + target + ", the second overwriting the first; "
                            + "nothing was written.");
                }
                files.add(new FileTask(file, partition.path(), target));
            }
            partitions.add(new PartitionTasks(partition, files));
        }
        return partitions;
    }

    private static List<FileRecord> pendingFiles(String runId, List<PartitionTasks> partitions) {
        List<FileRecord> files = new ArrayList<>();
        for (PartitionTasks partition : partitions) {
            for (FileTask task : partition.files()) {
                files.add(new FileRecord(runId, task.source().location(), task.target(), task.source().recordCount(),
                        FileRecord.Status.PENDING, null, null));
            }
        }
        return files;
    }

    private static String ago(Instant then, Instant now) {
        return Duration.between(then, now).withNanos(0).toString().substring(2).toLowerCase(Locale.ROOT);
    }

    private static String newRunId() {
        byte[] suffix = new byte[3];
        RANDOM.nextBytes(suffix);
        return RUN_ID_TIME.format(Instant.now()) + "-" + HexFormat.of().formatHex(suffix);
    }
}
