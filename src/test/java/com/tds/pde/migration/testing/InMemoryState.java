package com.tds.pde.migration.testing;

import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.state.SqlExecutor;
import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * State tables kept in memory for tests, in place of the Spark-backed Delta tables. Also an extra application
 * source that installs {@link #current} as the repository; deliberately not a {@code @Configuration}, so component
 * scanning never picks it up. Tests run one at a time, so one shared store is enough.
 */
public class InMemoryState {

    public static Store current = new Store();

    @Bean
    @Primary
    MigrationStateRepository inMemoryStateRepository() {
        return current;
    }

    public static final class Store extends MigrationStateRepository {

        private static final SqlExecutor NO_SQL = new SqlExecutor() {
            @Override
            public long update(String sql, Map<String, ?> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Map<String, Object>> query(String sql, Map<String, ?> params) {
                throw new UnsupportedOperationException();
            }
        };

        private final Map<String, ControlRecord> controls = new LinkedHashMap<>();
        private final Map<String, RunRecord> runs = new LinkedHashMap<>();
        private final Map<String, FileRecord> files = new LinkedHashMap<>();

        public Store() {
            super(NO_SQL, TestProperties.defaults());
        }

        public List<RunRecord> runs() {
            return List.copyOf(runs.values());
        }

        /** Replaces a file row as if the state table had been changed behind the run's back. */
        public void replaceFile(FileRecord file) {
            files.put(file.runId() + "/" + file.fileKey(), file);
        }

        @Override
        public void createTablesIfMissing() {
        }

        @Override
        public void saveControl(ControlRecord control) {
            controls.put(control.sourceTableLocation() + " -> " + control.targetLocation(), control);
        }

        @Override
        public Optional<ControlRecord> findControl(String sourceTableLocation, String targetLocation) {
            return Optional.ofNullable(controls.get(sourceTableLocation + " -> " + targetLocation));
        }

        @Override
        public void insertRun(RunRecord run) {
            runs.put(run.runId(), run);
        }

        @Override
        public Optional<RunRecord> findRun(String runId) {
            return Optional.ofNullable(runs.get(runId));
        }

        @Override
        public Optional<RunRecord> findLatestUnfinishedRun(String sourceTableLocation, String targetLocation) {
            return runs.values().stream()
                    .filter(run -> run.sourceTableLocation().equals(sourceTableLocation)
                            && run.targetLocation().equals(targetLocation) && run.isUnfinished())
                    .max(Comparator.comparing(RunRecord::startedAt));
        }

        @Override
        public Optional<RunRecord> findLatestRunForTarget(String targetLocation) {
            return runs.values().stream()
                    .filter(run -> run.targetLocation().equals(targetLocation)
                            && run.status() != RunRecord.Status.SUPERSEDED)
                    .max(Comparator.comparing(RunRecord::startedAt));
        }

        @Override
        public void updateRunStatus(String runId, RunRecord.Status status, @Nullable Instant finishedAt,
                                    @Nullable String error) {
            RunRecord run = runs.get(runId);
            runs.put(runId, new RunRecord(run.runId(), status, run.sourceDatasetId(), run.targetDatasetId(),
                    run.sourceTableLocation(), run.targetLocation(), run.snapshotId(), run.sourceSchemaJson(),
                    run.outputSchemaJson(), run.totalPartitions(), run.totalFiles(), run.totalRecords(),
                    run.startedAt(), finishedAt, error));
        }

        @Override
        public long supersedeRuns(String targetLocation) {
            List<RunRecord> owning = runs.values().stream()
                    .filter(run -> run.targetLocation().equals(targetLocation)
                            && run.status() != RunRecord.Status.SUPERSEDED)
                    .toList();
            owning.forEach(run -> updateRunStatus(run.runId(), RunRecord.Status.SUPERSEDED, run.finishedAt(),
                    run.error()));
            return owning.size();
        }

        @Override
        public void insertFiles(List<FileRecord> newFiles) {
            newFiles.forEach(this::replaceFile);
        }

        @Override
        public List<FileRecord> findFiles(String runId) {
            return files.values().stream().filter(file -> file.runId().equals(runId)).toList();
        }

        @Override
        public void markFileCompleted(String runId, String fileKey, long recordsWritten, long recordsDeleted) {
            FileRecord file = file(runId, fileKey);
            replaceFile(new FileRecord(runId, fileKey, file.sourcePath(), file.specId(), file.partitionPath(),
                    file.targetPath(), file.physicalRecords(), FileRecord.Status.COMPLETED, recordsWritten,
                    recordsDeleted, file.attempts() + 1, null, Instant.now()));
        }

        @Override
        public void markFileFailed(String runId, String fileKey, String error) {
            FileRecord file = file(runId, fileKey);
            replaceFile(new FileRecord(runId, fileKey, file.sourcePath(), file.specId(), file.partitionPath(),
                    file.targetPath(), file.physicalRecords(), FileRecord.Status.FAILED, file.recordsWritten(),
                    file.recordsDeleted(), file.attempts() + 1, error, Instant.now()));
        }

        private FileRecord file(String runId, String fileKey) {
            FileRecord file = files.get(runId + "/" + fileKey);
            if (file == null) {
                throw new IllegalStateException("file " + fileKey + " of run " + runId + " is not recorded");
            }
            return file;
        }
    }
}
