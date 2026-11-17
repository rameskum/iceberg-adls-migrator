package com.tds.pde.migration.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.testing.H2StateStore;
import com.tds.pde.migration.testing.TestProperties;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DelegatingDataSource;

class MigrationStateRepositoryTest {

    private final H2StateStore store = new H2StateStore();
    private final MigrationStateRepository repository = repository(store.jdbc());

    private static MigrationStateRepository repository(JdbcClient jdbc) {
        return new MigrationStateRepository(jdbc, TestProperties.defaults(), Duration.ofMillis(1));
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
        RunRecord run = new RunRecord("run-1", RunRecord.Status.RUNNING, "src", "dst", "hdfs://t", "abfss://d", null,
                "{}", "{}", 0, 0, 0, Instant.parse("2026-11-17T10:00:00Z"), null, null);

        repository.insertRun(run);

        assertThat(repository.findRun("run-1").orElseThrow().snapshotId()).isNull();
    }

    @Test
    void insertsManyFilesAcrossSeveralStatements() {
        List<FileRecord> files = IntStream.range(0, 47).mapToObj(i -> pending("run-1", "key-" + i)).toList();

        repository.insertFiles(files);

        assertThat(repository.findFiles("run-1")).containsExactlyInAnyOrderElementsOf(files);
    }

    @Test
    void tracksAFileThroughItsLifecycle() {
        repository.insertFiles(List.of(pending("run-1", "key-1")));

        repository.markFileInProgress("run-1", "key-1");
        repository.markFileFailed("run-1", "key-1", "boom");
        FileRecord failed = repository.findFiles("run-1").getFirst();
        assertThat(failed.status()).isEqualTo(FileRecord.Status.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(failed.lastError()).isEqualTo("boom");

        repository.markFileInProgress("run-1", "key-1");
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

        assertThat(repository.findLatestUnfinishedRun("hdfs://nn/orders", "abfss://d@a.dfs.core.windows.net/o"))
                .map(RunRecord::runId).contains("new-failed");
        assertThat(repository.findLatestUnfinishedRun("hdfs://nn/other", "abfss://d@a.dfs.core.windows.net/o"))
                .isEmpty();
        assertThat(repository.findLatestRunForDestination("abfss://d@a.dfs.core.windows.net/o"))
                .map(RunRecord::runId).contains("newest-done");
    }

    @Test
    void supersedesEveryRunOwningTheDestination() {
        repository.insertRun(run("a", RunRecord.Status.FAILED, Instant.parse("2026-11-01T00:00:00Z")));
        repository.insertRun(run("b", RunRecord.Status.SUCCEEDED, Instant.parse("2026-11-02T00:00:00Z")));

        assertThat(repository.supersedeRuns("abfss://d@a.dfs.core.windows.net/o")).isEqualTo(2);

        assertThat(repository.findRun("a").orElseThrow().status()).isEqualTo(RunRecord.Status.SUPERSEDED);
        assertThat(repository.findLatestRunForDestination("abfss://d@a.dfs.core.windows.net/o")).isEmpty();
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
    void retriesTransientFailures() {
        AtomicInteger failuresLeft = new AtomicInteger(2);
        MigrationStateRepository flaky = repository(JdbcClient.create(new DelegatingDataSource(store.dataSource()) {
            @Override
            public Connection getConnection() throws SQLException {
                if (failuresLeft.getAndDecrement() > 0) {
                    throw new SQLException("warehouse starting");
                }
                return super.getConnection();
            }
        }));

        flaky.insertRun(run("run-1", RunRecord.Status.RUNNING, Instant.parse("2026-11-17T10:00:00Z")));

        assertThat(repository.findRun("run-1")).isPresent();
    }

    @Test
    void givesUpAfterTheConfiguredAttempts() {
        MigrationStateRepository broken = repository(JdbcClient.create(new DelegatingDataSource(store.dataSource()) {
            @Override
            public Connection getConnection() throws SQLException {
                throw new SQLException("warehouse unreachable");
            }
        }));

        assertThatThrownBy(() -> broken.findRun("run-1"))
                .isInstanceOf(StateStoreException.class)
                .hasMessage("State store operation failed after 3 attempts: read run run-1")
                .rootCause().hasMessage("warehouse unreachable");
    }

    @Test
    void shipsDatabricksDdlForBothTables() throws IOException {
        String ddl = new ClassPathResource("db/databricks/migration-state.sql")
                .getContentAsString(StandardCharsets.UTF_8);

        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS ${run_table}", "CREATE TABLE IF NOT EXISTS ${file_table}",
                "USING DELTA");
        // Every column the repository writes must exist in the shipped DDL.
        for (String column : List.of("run_id", "source_schema_json", "output_schema_json", "total_records",
                "file_key", "physical_records", "records_deleted", "attempts", "last_error", "updated_at")) {
            assertThat(ddl).contains(column);
        }
    }

    private static RunRecord run(String runId, RunRecord.Status status, Instant startedAt) {
        return new RunRecord(runId, status, "orders-src", "orders-adls", "hdfs://nn/orders",
                "abfss://d@a.dfs.core.windows.net/o", 42L, "{\"type\":\"struct\"}", "{\"type\":\"struct\"}",
                2, 3, 300, startedAt, null, null);
    }

    private static FileRecord pending(String runId, String fileKey) {
        return new FileRecord(runId, fileKey, "hdfs://nn/orders/data/" + fileKey + ".parquet", 0, "region=EU",
                "o/" + fileKey + ".parquet", 100, FileRecord.Status.PENDING, null, null, 0, null,
                Instant.parse("2026-11-17T10:00:00Z").truncatedTo(ChronoUnit.MICROS));
    }
}
