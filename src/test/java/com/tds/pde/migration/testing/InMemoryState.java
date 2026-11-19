package com.tds.pde.migration.testing;

import com.tds.pde.migration.state.ControlRecord;
import com.tds.pde.migration.state.FileOutcome;
import com.tds.pde.migration.state.FileRecord;
import com.tds.pde.migration.state.MigrationStateRepository;
import com.tds.pde.migration.state.SqlExecutor;
import com.tds.pde.migration.state.StateStoreException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * State tables kept in memory for tests, in place of the Spark-backed Delta tables. Also an extra application
 * source that installs {@link #current} as the repository; deliberately not a {@code @Configuration}, so component
 * scanning never picks it up. Tests run one at a time, so one shared store is enough.
 *
 * <p>Control row writes are atomic, like Delta commits: of two conditional writes racing on one row, only the first
 * applies. Inserts of a new key can be made to behave like Databricks' row-level concurrency, where both commit.
 */
public class InMemoryState {

    public static Store current = new Store();

    @Bean
    @Primary
    MigrationStateRepository inMemoryStateRepository() {
        return current;
    }

    public static final class Store extends MigrationStateRepository {

        /** The run id {@link #takeOverOnFirstOutcome()} gives the control row. */
        public static final String OTHER_RUN = "20990101-000000-0th3r0";

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

        private final List<ControlRecord> controls = new ArrayList<>();
        private final Map<String, FileRecord> files = new LinkedHashMap<>();
        private final List<Integer> outcomeWrites = new ArrayList<>();
        private final List<ControlRecord.Progress> renewals = new ArrayList<>();
        private volatile boolean duplicateInserts;
        private volatile @Nullable CyclicBarrier claimBarrier;
        private volatile boolean takeOverOnFirstOutcome;
        private final CountDownLatch takeOverNoticed = new CountDownLatch(1);
        private volatile @Nullable String takenFromRun;

        public Store() {
            super(NO_SQL, TestProperties.defaults());
        }

        /** Lets two inserts of the same new key both commit, as Databricks' row-level concurrency may. */
        public void allowDuplicateInserts() {
            this.duplicateInserts = true;
        }

        /** Holds every control row claim until {@code parties} claims are waiting, so that they race. */
        public void raceClaims(int parties) {
            this.claimBarrier = new CyclicBarrier(parties);
        }

        /** {@link #takeOver()} once the first file outcome is recorded, holding the worker that recorded it. */
        public void takeOverOnFirstOutcome() {
            this.takeOverOnFirstOutcome = true;
        }

        /** Sets the control row as if written by another run. */
        public synchronized void putControl(ControlRecord control) {
            controls.removeIf(row -> row.targetDatasetId().equals(control.targetDatasetId()));
            controls.add(control);
        }

        public synchronized List<ControlRecord> controls() {
            return List.copyOf(controls);
        }

        /** The progress written by every successful heartbeat renewal, in order. */
        public synchronized List<ControlRecord.Progress> renewals() {
            return List.copyOf(renewals);
        }

        @Override
        public void createTablesIfMissing() {
        }

        @Override
        public synchronized List<ControlRecord> findControls(String targetDatasetId) {
            return controls.stream().filter(row -> row.targetDatasetId().equals(targetDatasetId)).toList();
        }

        @Override
        public synchronized List<ControlRecord> findOtherControls(String targetDatasetId) {
            return controls.stream().filter(row -> !row.targetDatasetId().equals(targetDatasetId)).toList();
        }

        @Override
        public void insertControl(ControlRecord control) {
            awaitRace();
            synchronized (this) {
                if (duplicateInserts || findControls(control.targetDatasetId()).isEmpty()) {
                    controls.add(control);
                }
            }
        }

        @Override
        public void replaceControl(ControlRecord expected, ControlRecord replacement) {
            awaitRace();
            synchronized (this) {
                int index = controls.indexOf(expected);
                if (index >= 0) {
                    controls.set(index, replacement);
                }
            }
        }

        @Override
        public synchronized void deleteControl(String targetDatasetId, String runId) {
            controls.removeIf(row -> row.targetDatasetId().equals(targetDatasetId) && row.runId().equals(runId));
        }

        @Override
        public synchronized long renewControl(String targetDatasetId, String runId, Instant heartbeatAt,
                                              ControlRecord.Progress progress) {
            long updated = update(targetDatasetId, runId, row -> new ControlRecord(row.targetDatasetId(),
                    row.sourceDatasetId(), row.sourceLocation(), row.targetLocation(), row.runId(), row.status(),
                    row.snapshotId(), row.startedAt(), heartbeatAt, row.finishedAt(), row.totalFiles(),
                    row.totalRecords(), progress, row.error()));
            if (updated > 0) {
                renewals.add(progress);
            } else if (runId.equals(takenFromRun)) {
                takeOverNoticed.countDown();
            }
            return updated;
        }

        @Override
        public synchronized long finishControl(String targetDatasetId, String runId, ControlRecord.Status status,
                                               Instant finishedAt, @Nullable String error,
                                               ControlRecord.Progress progress) {
            return update(targetDatasetId, runId, row -> new ControlRecord(row.targetDatasetId(),
                    row.sourceDatasetId(), row.sourceLocation(), row.targetLocation(), row.runId(), status,
                    row.snapshotId(), row.startedAt(), finishedAt, finishedAt, row.totalFiles(), row.totalRecords(),
                    progress, error));
        }

        private long update(String targetDatasetId, String runId, Function<ControlRecord, ControlRecord> change) {
            long updated = 0;
            for (int i = 0; i < controls.size(); i++) {
                ControlRecord row = controls.get(i);
                if (row.targetDatasetId().equals(targetDatasetId) && row.runId().equals(runId)) {
                    controls.set(i, change.apply(row));
                    updated++;
                }
            }
            return updated;
        }

        private void awaitRace() {
            CyclicBarrier barrier = claimBarrier;
            if (barrier != null) {
                try {
                    barrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("the claims did not race", e);
                }
            }
        }

        /** Like the MERGE it stands for: rows already present are left as they are. */
        @Override
        public synchronized void insertFiles(List<FileRecord> newFiles) {
            for (FileRecord file : newFiles) {
                files.putIfAbsent(file.runId() + "/" + file.sourcePath(), file);
            }
        }

        @Override
        public synchronized List<FileRecord> findFiles(String runId) {
            return files.values().stream().filter(file -> file.runId().equals(runId)).toList();
        }

        @Override
        public synchronized long deleteFiles(String runId) {
            int before = files.size();
            files.values().removeIf(file -> file.runId().equals(runId));
            return before - files.size();
        }

        @Override
        public void recordFileOutcomes(String runId, List<FileOutcome> outcomes) {
            synchronized (this) {
                outcomeWrites.add(outcomes.size());
                for (FileOutcome outcome : outcomes) {
                    FileRecord file = files.get(runId + "/" + outcome.sourcePath());
                    if (file == null) {
                        throw new StateStoreException("file " + outcome.sourcePath() + " of run " + runId
                                + " is not recorded", null);
                    }
                    files.put(runId + "/" + outcome.sourcePath(), new FileRecord(runId, outcome.sourcePath(),
                            file.targetPath(), file.physicalRecords(), outcome.status(), outcome.recordsWritten(),
                            outcome.error()));
                }
            }
            if (takeOverOnFirstOutcome) {
                takeOverOnFirstOutcome = false;
                takeOver();
            }
        }

        /**
         * Hands the only control row to {@link #OTHER_RUN}, as a run that cleaned the target after the running run's
         * heartbeat went stale would, and waits until the running run's next renewal has found the row taken.
         */
        public void takeOver() {
            synchronized (this) {
                if (controls.size() != 1) {
                    throw new IllegalStateException("expected one control row, found " + controls);
                }
                ControlRecord row = controls.getFirst();
                takenFromRun = row.runId();
                controls.set(0, new ControlRecord(row.targetDatasetId(), row.sourceDatasetId(),
                        row.sourceLocation(), row.targetLocation(), OTHER_RUN, ControlRecord.Status.RUNNING,
                        row.snapshotId(), Instant.now(), Instant.now(), null, row.totalFiles(), row.totalRecords(),
                        ControlRecord.Progress.NONE, null));
            }
            try {
                if (!takeOverNoticed.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the run never noticed that it lost the migration");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        /** The size of every file outcome write, in order. */
        public synchronized List<Integer> outcomeWrites() {
            return List.copyOf(outcomeWrites);
        }
    }
}
