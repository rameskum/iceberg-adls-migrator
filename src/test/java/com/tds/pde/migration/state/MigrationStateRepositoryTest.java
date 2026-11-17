package com.tds.pde.migration.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.testing.H2StateStore;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class MigrationStateRepositoryTest {

    private static final String SOURCE = "hdfs://nn/orders";
    private static final String DESTINATION = "abfss://d@a.dfs.core.windows.net/o";

    private final H2StateStore store = new H2StateStore();
    private final MigrationStateRepository repository = repository(store.executor());

    private static MigrationStateRepository repository(SqlExecutor executor) {
        return new MigrationStateRepository(executor, TestProperties.defaults(), Duration.ofMillis(1));
    }

    @Test
    void storesAndReadsBackARun() {
        RunRecord run = run("run-1", RunRecord.Status.RUNNING, Instant.parse("2026-11-17T10:00:00Z"));

        repository.insertRun(run);

        assertThat(repository.findRun("run-1")).contains(run);
        assertThat(repository.findRun("missing")).isEmpty();
    }

    @Test
    void storesARunWithoutSnapshot() {
        RunRecord run = new RunRecord("run-1", RunRecord.Status.RUNNING, "src", "dst", SOURCE, DESTINATION, null,
                "{}", "{}", 0, 0, 0, Instant.parse("2026-11-17T10:00:00Z"), null, null);

        repository.insertRun(run);

        assertThat(repository.findRun("run-1").orElseThrow().snapshotId()).isNull();
    }

    @Test
    void insertsManyFilesAcrossSeveralStatements() {
        List<FileRecord> files = IntStream.range(0, 247).mapToObj(i -> pending("run-1", "key-" + i)).toList();

        repository.insertFiles(files);

        assertThat(repository.findFiles("run-1")).containsExactlyInAnyOrderElementsOf(files);
    }

    @Test
    void recordsEachOutcomeAsAnAttempt() {
        repository.insertFiles(List.of(pending("run-1", "key-1")));

        repository.markFileFailed("run-1", "key-1", "boom");
        FileRecord failed = repository.findFiles("run-1").getFirst();
        assertThat(failed.status()).isEqualTo(FileRecord.Status.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.lastError()).isEqualTo("boom");

        repository.markFileCompleted("run-1", "key-1", 90, 10);
        FileRecord completed = repository.findFiles("run-1").getFirst();
        assertThat(completed.status()).isEqualTo(FileRecord.Status.COMPLETED);
        assertThat(completed.attempts()).isEqualTo(2);
        assertThat(completed.recordsWritten()).isEqualTo(90);
        assertThat(completed.recordsDeleted()).isEqualTo(10);
        assertThat(completed.lastError()).isNull();
    }

    @Test
    void truncatesLongErrors() {
        repository.insertFiles(List.of(pending("run-1", "key-1")));

        repository.markFileFailed("run-1", "key-1", "x".repeat(10_000));

        assertThat(repository.findFiles("run-1").getFirst().lastError()).hasSize(4000);
    }

    @Test
    void refusesToUpdateAFileThatIsNotRecorded() {
        assertThatThrownBy(() -> repository.markFileCompleted("run-1", "missing", 1, 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("updated 0 rows");
    }

    @Test
    void findsTheLatestUnfinishedRunForTheSameSourceAndDestination() {
        repository.insertRun(run("old-failed", RunRecord.Status.FAILED, Instant.parse("2026-11-01T00:00:00Z")));
        repository.insertRun(run("new-failed", RunRecord.Status.FAILED, Instant.parse("2026-11-10T00:00:00Z")));
        repository.insertRun(run("newest-done", RunRecord.Status.SUCCEEDED, Instant.parse("2026-11-12T00:00:00Z")));

        assertThat(repository.findLatestUnfinishedRun(SOURCE, DESTINATION)).map(RunRecord::runId)
                .contains("new-failed");
        assertThat(repository.findLatestUnfinishedRun("hdfs://nn/other", DESTINATION)).isEmpty();
        assertThat(repository.findLatestRunForDestination(DESTINATION)).map(RunRecord::runId)
                .contains("newest-done");
    }

    @Test
    void supersedesEveryRunOwningTheDestination() {
        repository.insertRun(run("a", RunRecord.Status.FAILED, Instant.parse("2026-11-01T00:00:00Z")));
        repository.insertRun(run("b", RunRecord.Status.SUCCEEDED, Instant.parse("2026-11-02T00:00:00Z")));

        assertThat(repository.supersedeRuns(DESTINATION)).isEqualTo(2);

        assertThat(repository.findRun("a").orElseThrow().status()).isEqualTo(RunRecord.Status.SUPERSEDED);
        assertThat(repository.findLatestRunForDestination(DESTINATION)).isEmpty();
    }

    @Test
    void recordsTheOutcomeOfARun() {
        repository.insertRun(run("run-1", RunRecord.Status.RUNNING, Instant.parse("2026-11-17T10:00:00Z")));
        Instant finished = Instant.parse("2026-11-17T11:00:00Z");

        repository.updateRunStatus("run-1", RunRecord.Status.FAILED, finished, "2 files failed");

        RunRecord run = repository.findRun("run-1").orElseThrow();
        assertThat(run.status()).isEqualTo(RunRecord.Status.FAILED);
        assertThat(run.finishedAt()).isEqualTo(finished);
        assertThat(run.error()).isEqualTo("2 files failed");
    }

    @Test
    void createsTheControlRowOnTheFirstSaveAndUpdatesItAfterwards() {
        ControlRecord started = control("run-1", RunRecord.Status.RUNNING, 0, null);
        repository.saveControl(started);
        assertThat(repository.findControl(SOURCE, DESTINATION)).contains(started);

        ControlRecord finished = control("run-1", RunRecord.Status.SUCCEEDED, 3,
                Instant.parse("2026-11-17T11:00:00Z"));
        repository.saveControl(finished);

        assertThat(repository.findControl(SOURCE, DESTINATION)).contains(finished);
        assertThat(store.jdbc().sql("SELECT COUNT(*) FROM migration_control").query(Long.class).single())
                .isEqualTo(1);
        assertThat(repository.findControl(SOURCE, "abfss://d@a.dfs.core.windows.net/elsewhere")).isEmpty();
    }

    @Test
    void retriesTransientFailures() {
        AtomicInteger failuresLeft = new AtomicInteger(2);
        SqlExecutor flaky = new FailingExecutor(store.executor(), () -> failuresLeft.getAndDecrement() > 0);

        repository(flaky).insertRun(run("run-1", RunRecord.Status.RUNNING, Instant.parse("2026-11-17T10:00:00Z")));

        assertThat(repository.findRun("run-1")).isPresent();
    }

    @Test
    void givesUpAfterTheConfiguredAttempts() {
        SqlExecutor broken = new FailingExecutor(store.executor(), () -> true);

        assertThatThrownBy(() -> repository(broken).findRun("run-1"))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("State store operation failed after 5 attempts: read run run-1 (Spark SQL failed)")
                .rootCause().hasMessage("ConcurrentAppendException: files were added by a concurrent update");
    }

    @Test
    void shipsDeltaDdlForAllThreeTablesWithRowLevelConcurrency() throws IOException {
        String ddl = new ClassPathResource("db/databricks/migration-state.sql")
                .getContentAsString(StandardCharsets.UTF_8);

        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS ${control_table}",
                "CREATE TABLE IF NOT EXISTS ${run_table}", "CREATE TABLE IF NOT EXISTS ${file_table}");
        assertThat(ddl.split("USING DELTA", -1)).hasSize(4);
        assertThat(ddl.split("'delta.enableRowTracking' = 'true'", -1)).hasSize(4);
    }

    @Test
    void runsEveryDdlStatementSeparately() {
        RecordingExecutor recording = new RecordingExecutor();

        repository(recording).createTablesIfMissing();

        assertThat(recording.statements).hasSize(3).allSatisfy(statement ->
                assertThat(statement).startsWith("CREATE TABLE IF NOT EXISTS migration_").endsWith("'true')"));
    }

    private static RunRecord run(String runId, RunRecord.Status status, Instant startedAt) {
        return new RunRecord(runId, status, "orders-src", "orders-adls", SOURCE, DESTINATION, 42L,
                "{\"type\":\"struct\"}", "{\"type\":\"struct\"}", 2, 3, 300, startedAt, null, null);
    }

    private static FileRecord pending(String runId, String fileKey) {
        return new FileRecord(runId, fileKey, "hdfs://nn/orders/data/" + fileKey + ".parquet", 0, "region=EU",
                "o/" + fileKey + ".parquet", 100, FileRecord.Status.PENDING, null, null, 0, null,
                Instant.parse("2026-11-17T10:00:00Z"));
    }

    private static ControlRecord control(String runId, RunRecord.Status status, long completedFiles,
                                         Instant finishedAt) {
        return new ControlRecord("orders-src", "orders-adls", SOURCE, DESTINATION, status, runId, 2, 42L,
                Instant.parse("2026-11-16T08:00:00Z"), 3, "0:[1000: region: identity(2)]", 2, 3, 300, 1, 4096,
                completedFiles == 3 ? 2 : 0, completedFiles, 0, completedFiles * 100, 0,
                Instant.parse("2026-11-17T10:00:00Z"), finishedAt, null);
    }

    /** Fails while the condition holds, the way a conflicting Delta commit does, then delegates. */
    private record FailingExecutor(SqlExecutor delegate, java.util.function.BooleanSupplier failNow)
            implements SqlExecutor {

        @Override
        public long update(String sql, Map<String, ?> params) {
            failIfNeeded();
            return delegate.update(sql, params);
        }

        @Override
        public List<Map<String, Object>> query(String sql, Map<String, ?> params) {
            failIfNeeded();
            return delegate.query(sql, params);
        }

        private void failIfNeeded() {
            if (failNow.getAsBoolean()) {
                throw new SqlExecutionException("Spark SQL failed",
                        new IllegalStateException("ConcurrentAppendException: files were added by a concurrent update"));
            }
        }
    }

    private static final class RecordingExecutor implements SqlExecutor {

        private final List<String> statements = new java.util.ArrayList<>();

        @Override
        public long update(String sql, Map<String, ?> params) {
            statements.add(sql);
            return 0;
        }

        @Override
        public List<Map<String, Object>> query(String sql, Map<String, ?> params) {
            return List.of();
        }
    }
}
