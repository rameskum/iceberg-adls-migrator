package com.tds.pde.migration.run;

import com.tds.pde.migration.MigrationFailedException;
import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.TargetPathResolver;
import com.tds.pde.migration.target.azure.AzureLocation;
import com.tds.pde.migration.processing.FileTask;
import com.tds.pde.migration.processing.RunContext;
import com.tds.pde.migration.schema.TargetSchemaValidator;
import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.source.MigrationPlan;
import com.tds.pde.migration.source.PlannedPartition;
import com.tds.pde.migration.source.SourceFile;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.springframework.stereotype.Component;

/**
 * Decides which run to execute and records it before any file is processed:
 * <ul>
 *   <li>{@code migration.run-id} set: resume that run;</li>
 *   <li>{@code migration.target.clean=true}: delete the target, supersede earlier runs, start fresh;</li>
 *   <li>otherwise: resume the latest unfinished run for this source and target, or start a new run into an
 *       empty target.</li>
 * </ul>
 * The caller must hold the target's run lock.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RunStarter {

    /** Folder under the target prefix holding migration bookkeeping; Spark ignores names starting with '_'. */
    public static final String BOOKKEEPING_FOLDER = "_migration";

    private static final DateTimeFormatter RUN_ID_TIME =
            DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int MAX_LISTED_MISMATCHES = 10;

    private final MigrationProperties properties;
    private final MigrationStateRepository state;
    private final IcebergMetadataReader metadataReader;
    private final TargetSchemaValidator schemaValidator;
    private final TargetPathResolver pathResolver;

    public StartedRun start(DatasetDescriptor source, DatasetDescriptor target, AzureLocation location,
                            BlobStore blobStore) {
        String runId = properties.runId();
        boolean clean = properties.target().clean();
        if (clean && runId != null) {
            throw new MigrationFailedException("migration.target.clean starts a new run and cannot be combined "
                    + "with migration.run-id=" + runId);
        }

        if (runId != null) {
            RunRecord run = state.findRun(runId)
                    .orElseThrow(() -> new MigrationFailedException("Run " + runId + " does not exist"));
            requireSameDatasets(run, source, target);
            if (run.status() == RunRecord.Status.SUPERSEDED) {
                throw new MigrationFailedException("Run " + runId + " was superseded by a clean start; its output "
                        + "is gone. Start a new run instead.");
            }
            return resume(run, source, target, location);
        }
        if (clean) {
            clean(location, blobStore, target.location());
            return startNew(source, target, location);
        }
        Optional<RunRecord> unfinished = state.findLatestUnfinishedRun(source.location(), target.location());
        if (unfinished.isPresent()) {
            log.info("Resuming the latest unfinished run for this source and target: runId={} status={} "
                    + "startedAt={}", unfinished.get().runId(), unfinished.get().status(), unfinished.get().startedAt());
            return resume(unfinished.get(), source, target, location);
        }
        requireEmptyTarget(location, blobStore, target.location());
        return startNew(source, target, location);
    }

    private StartedRun startNew(DatasetDescriptor source, DatasetDescriptor target, AzureLocation location) {
        MigrationPlan plan = metadataReader.plan(source.location(), properties.source().snapshotId());
        OutputSchema outputSchema = schemaValidator.resolve(plan.table().schema(), target.schema());
        String runId = newRunId();
        List<PartitionTasks> partitions = tasks(plan, location);

        RunRecord run = new RunRecord(runId, RunRecord.Status.RUNNING, source.datasetId(), target.datasetId(),
                source.location(), target.location(), plan.table().snapshotId(),
                SchemaParser.toJson(plan.table().schema()), SchemaParser.toJson(outputSchema.schema()), Instant.now(),
                null, null);
        state.insertRun(run);
        List<FileRecord> files = pendingFiles(runId, partitions);
        state.insertFiles(files);
        log.info("Run started: runId={} snapshotId={} files={}", runId, plan.table().snapshotId(), files.size());

        return new StartedRun(run, new RunContext(runId, plan.table(), outputSchema), plan, partitions,
                files.stream().collect(Collectors.toMap(FileRecord::sourcePath, Function.identity())), false);
    }

    private StartedRun resume(RunRecord run, DatasetDescriptor source, DatasetDescriptor target,
                              AzureLocation location) {
        Long configuredSnapshot = properties.source().snapshotId();
        if (configuredSnapshot != null && !configuredSnapshot.equals(run.snapshotId())) {
            throw new MigrationFailedException("Run " + run.runId() + " is pinned to snapshot " + run.snapshotId()
                    + " but migration.source.snapshot-id is " + configuredSnapshot);
        }
        MigrationPlan plan = metadataReader.plan(source.location(), run.snapshotId());
        if (!Objects.equals(plan.table().snapshotId(), run.snapshotId())) {
            throw new MigrationFailedException("Run " + run.runId() + " started on a table that had never been "
                    + "written; it has been written since. Start over with migration.target.clean=true.");
        }

        // Read and write with exactly the schemas the run started with, so every file of the run looks the same.
        Schema readSchema = SchemaParser.fromJson(run.sourceSchemaJson());
        plan = plan.withTable(plan.table().withSchema(readSchema));
        OutputSchema outputSchema = schemaValidator.resolve(readSchema, target.schema());
        if (!outputSchema.schema().sameSchema(SchemaParser.fromJson(run.outputSchemaJson()))) {
            throw new MigrationFailedException("The target schema changed since run " + run.runId()
                    + " started, so resuming would mix file schemas. Restore it, or start over with "
                    + "migration.target.clean=true.");
        }

        List<PartitionTasks> partitions = tasks(plan, location);
        Map<String, FileRecord> stored = state.findFiles(run.runId()).stream()
                .collect(Collectors.toMap(FileRecord::sourcePath, Function.identity()));
        requireSameFiles(run, partitions, stored);

        state.updateRunStatus(run.runId(), RunRecord.Status.RUNNING, null, null);
        long completed = stored.values().stream().filter(f -> f.status() == FileRecord.Status.COMPLETED).count();
        log.info("Run resumed: runId={} snapshotId={} previousStatus={} completedFiles={}/{}", run.runId(),
                run.snapshotId(), run.status(), completed, stored.size());

        RunRecord running = new RunRecord(run.runId(), RunRecord.Status.RUNNING, run.sourceDatasetId(),
                run.targetDatasetId(), run.sourceLocation(), run.targetLocation(), run.snapshotId(),
                run.sourceSchemaJson(), run.outputSchemaJson(), run.startedAt(), null, null);
        return new StartedRun(running, new RunContext(run.runId(), plan.table(), outputSchema), plan, partitions,
                stored, true);
    }

    private static void requireSameDatasets(RunRecord run, DatasetDescriptor source, DatasetDescriptor target) {
        if (!run.sourceDatasetId().equals(source.datasetId()) || !run.targetDatasetId().equals(target.datasetId())) {
            throw new MigrationFailedException("Run " + run.runId() + " belongs to the migration of dataset "
                    + run.sourceDatasetId() + " to " + run.targetDatasetId() + ", not " + source.datasetId() + " to "
                    + target.datasetId());
        }
        if (!run.sourceLocation().equals(source.location()) || !run.targetLocation().equals(target.location())) {
            throw new MigrationFailedException("Run " + run.runId() + " copies " + run.sourceLocation() + " to "
                    + run.targetLocation() + ", not " + source.location() + " to " + target.location());
        }
    }

    /** The pinned snapshot is immutable, so re-planning it must yield exactly the files the run recorded. */
    private static void requireSameFiles(RunRecord run, List<PartitionTasks> partitions,
                                         Map<String, FileRecord> stored) {
        List<String> mismatches = new ArrayList<>();
        int planned = 0;
        for (PartitionTasks partition : partitions) {
            for (FileTask task : partition.files()) {
                planned++;
                FileRecord record = stored.get(task.source().location());
                if (record == null) {
                    mismatches.add("not recorded: " + task.source().location());
                } else if (!record.targetPath().equals(task.target())
                        || record.physicalRecords() != task.source().recordCount()) {
                    mismatches.add("changed: " + task.source().location());
                }
            }
        }
        if (planned != stored.size()) {
            mismatches.add(stored.size() + " files recorded but " + planned + " planned");
        }
        if (!mismatches.isEmpty()) {
            throw new MigrationFailedException("Snapshot " + run.snapshotId() + " no longer yields the files run "
                    + run.runId() + " recorded; start over with migration.target.clean=true:\n  "
                    + String.join("\n  ", mismatches.subList(0, Math.min(mismatches.size(), MAX_LISTED_MISMATCHES))));
        }
    }

    private void requireEmptyTarget(AzureLocation location, BlobStore blobStore, String targetLocation) {
        List<BlobStore.BlobInfo> existing = outputBlobs(location, blobStore);
        if (existing.isEmpty()) {
            return;
        }
        String owner = state.findLatestRunForTarget(targetLocation)
                .map(run -> " written by run " + run.runId() + " (" + run.status() + ")")
                .orElse("");
        throw new MigrationFailedException("Target " + location + " already holds " + existing.size()
                + " files" + owner + ". Resume that run with migration.run-id, or start over with "
                + "migration.target.clean=true; nothing was written.");
    }

    private void clean(AzureLocation location, BlobStore blobStore, String targetLocation) {
        if (location.isContainerRoot()) {
            throw new MigrationFailedException("Refusing to clean " + location
                    + ": it is a whole container. Point the target at a folder within it.");
        }
        List<BlobStore.BlobInfo> blobs = outputBlobs(location, blobStore);
        long bytes = blobs.stream().mapToLong(BlobStore.BlobInfo::size).sum();
        log.warn("Cleaning target before a fresh run: target={} blobs={} bytes={}", location, blobs.size(),
                bytes);
        blobs.forEach(blob -> blobStore.delete(blob.name()));
        long superseded = state.supersedeRuns(targetLocation);
        log.info("Target cleaned: target={} deletedBlobs={} supersededRuns={}", location, blobs.size(),
                superseded);
    }

    /** Everything under the target prefix except the migration's own bookkeeping, such as the lock. */
    private static List<BlobStore.BlobInfo> outputBlobs(AzureLocation location, BlobStore blobStore) {
        String prefix = location.isContainerRoot() ? "" : location.prefix() + "/";
        String bookkeeping = prefix + BOOKKEEPING_FOLDER + "/";
        return blobStore.list(prefix).stream().filter(blob -> !blob.name().startsWith(bookkeeping)).toList();
    }

    private List<PartitionTasks> tasks(MigrationPlan plan, AzureLocation location) {
        List<PartitionTasks> partitions = new ArrayList<>(plan.partitions().size());
        for (PlannedPartition partition : plan.partitions()) {
            List<FileTask> files = new ArrayList<>(partition.files().size());
            for (SourceFile file : partition.files()) {
                files.add(new FileTask(file, partition.path(),
                        pathResolver.resolve(location.prefix(), partition.path(), file.location())));
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

    private static String newRunId() {
        byte[] suffix = new byte[3];
        RANDOM.nextBytes(suffix);
        return RUN_ID_TIME.format(Instant.now()) + "-" + HexFormat.of().formatHex(suffix);
    }
}
