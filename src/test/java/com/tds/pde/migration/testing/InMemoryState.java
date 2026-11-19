package com.tds.pde.migration.testing;

import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileOutcome;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.RunRecord;
import com.tds.pde.migration.state.SqlExecutor;
import com.tds.pde.migration.state.StateStoreException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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

            @Override
            public <T> T withRows(String schema, List<Object[]> rows, Function<String, T> body) {
                throw new UnsupportedOperationException();
            }
        };

        private final Map<String, ControlRecord> controls = new LinkedHashMap<>();
        private final Map<String, RunRecord> runs = new LinkedHashMap<>();
        private final Map<String, FileRecord> files = new LinkedHashMap<>();
        private final List<Integer> outcomeWrites = new ArrayList<>();
        private int fileRowsBeforeCrash = -1;

        public Store() {
            super(NO_SQL, TestProperties.defaults());
        }

        public List<RunRecord> runs() {
            return List.copyOf(runs.values());
        }

        /** Replaces a file row as if the state table had been changed behind the run's back. */
        public void replaceFile(FileRecord file) {
            files.put(file.runId() + "/" + file.sourcePath(), file);
        }

        /** Removes a file row as if it had never been written. */
        public void removeFile(FileRecord file) {
            files.remove(file.runId() + "/" + file.sourcePath());
        }

        @Override
        public void createTablesIfMissing() {
        }

        @Override
        public void saveControl(ControlRecord control) {
            controls.put(control.sourceDatasetId() + " -> " + control.targetDatasetId(), control);
        }

        @Override
        public Optional<ControlRecord> findControl(String sourceDatasetId, String targetDatasetId) {
            return Optional.ofNullable(controls.get(sourceDatasetId + " -> " + targetDatasetId));
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
        public Optional<RunRecord> findLatestUnfinishedRun(String sourceLocation, String targetLocation) {
            return runs.values().stream()
                    .filter(run -> run.sourceLocation().equals(sourceLocation)
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
                    run.sourceLocation(), run.targetLocation(), run.snapshotId(), run.sourceSchemaJson(),
                    run.outputSchemaJson(), run.startedAt(), finishedAt, error));
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

        /**
         * Makes the next file row insert record only its first {@code rows} rows and then fail, as if the job died
         * part way through recording a new run's files.
         */
        public void crashAfterFileRows(int rows) {
            this.fileRowsBeforeCrash = rows;
        }

        /** Like the MERGE it stands for: rows already present are left as they are. */
        @Override
        public void insertFiles(List<FileRecord> newFiles) {
            int crashAfter = fileRowsBeforeCrash;
            fileRowsBeforeCrash = -1;
            for (int i = 0; i < newFiles.size(); i++) {
                if (i == crashAfter) {
                    throw new StateStoreException("injected crash after " + crashAfter + " file rows", null);
                }
                FileRecord file = newFiles.get(i);
                files.putIfAbsent(file.runId() + "/" + file.sourcePath(), file);
            }
        }

        @Override
        public List<FileRecord> findFiles(String runId) {
            return files.values().stream().filter(file -> file.runId().equals(runId)).toList();
        }

        @Override
        public synchronized void recordFileOutcomes(String runId, List<FileOutcome> outcomes) {
            outcomeWrites.add(outcomes.size());
            for (FileOutcome outcome : outcomes) {
                FileRecord file = file(runId, outcome.sourcePath());
                replaceFile(new FileRecord(runId, outcome.sourcePath(), file.targetPath(), file.physicalRecords(),
                        outcome.status(), outcome.recordsWritten(), outcome.error()));
            }
        }

        /** The size of every file outcome write, in order. */
        public List<Integer> outcomeWrites() {
            return List.copyOf(outcomeWrites);
        }

        private FileRecord file(String runId, String sourcePath) {
            FileRecord file = files.get(runId + "/" + sourcePath);
            if (file == null) {
                throw new IllegalStateException("file " + sourcePath + " of run " + runId + " is not recorded");
            }
            return file;
        }
    }
}
