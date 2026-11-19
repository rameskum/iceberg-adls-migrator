package com.tds.pde.migration.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the reflection bridge against stand-in classes with Spark's API shape (in {@code org.apache.spark.sql}
 * under the test sources), as it would find the Databricks Runtime's classes on the system class path.
 */
class SparkSqlExecutorTest {

    private final SparkSqlExecutor executor = new SparkSqlExecutor(() -> getClass().getClassLoader());

    @BeforeEach
    void reset() {
        SparkSession.statements.clear();
        SparkSession.parameters.clear();
        SparkSession.views.clear();
        SparkSession.contextLoaders.clear();
        SparkSession.nextResult = List.of();
    }

    @Test
    void passesNamedParametersAndReturnsRowsKeyedByColumn() {
        SparkSession.nextResult = List.of(
                new Row(new String[] {"run_id", "total_files"}, "run-1", 3L),
                new Row(new String[] {"run_id", "total_files"}, "run-2", null));

        List<Map<String, Object>> rows = executor.query("SELECT run_id, total_files FROM runs WHERE status = :status",
                Map.of("status", "RUNNING"));

        assertThat(SparkSession.statements).containsExactly("SELECT run_id, total_files FROM runs WHERE status = :status");
        assertThat(SparkSession.parameters).containsExactly(Map.of("status", "RUNNING"));
        assertThat(rows).hasSize(2);
        assertThat(rows.getFirst()).containsEntry("run_id", "run-1").containsEntry("total_files", 3L);
        assertThat(rows.getFirst().get("RUN_ID")).as("column names match case-insensitively").isEqualTo("run-1");
        assertThat(rows.get(1)).containsEntry("total_files", null);
    }

    @Test
    void returnsArrayColumnsAsJavaLists() {
        SparkSession.nextResult = List.of(new Row(new String[] {"clusteringColumns", "partitionColumns"},
                scala.collection.Seq.of(List.of("run_id", "source_path")), scala.collection.Seq.of(List.of())));

        Map<String, Object> detail = executor.query("DESCRIBE DETAIL migration_file", Map.of()).getFirst();

        assertThat(detail.get("clusteringColumns")).isEqualTo(List.of("run_id", "source_path"));
        assertThat(detail.get("partitionColumns")).isEqualTo(List.of());
    }

    @Test
    void reportsTheRowsDeltaSaysAStatementAffected() {
        SparkSession.nextResult = List.of(new Row(new String[] {"num_affected_rows"}, 1L));

        assertThat(executor.update("UPDATE runs SET status = :status", Map.of("status", "FAILED"))).isEqualTo(1);
    }

    @Test
    void reportsUnknownWhenTheEngineDoesNotSayHowManyRowsChanged() {
        assertThat(executor.update("CREATE TABLE IF NOT EXISTS runs (run_id STRING)", Map.of())).isEqualTo(-1);
    }

    @Test
    void exposesTypedRowsAsATemporaryViewForTheDurationOfTheBody() {
        List<Object[]> rows = List.<Object[]>of(new Object[] {"run-1", 3L}, new Object[] {"run-2", null});

        List<String> seen = executor.withRows("run_id STRING, total_files BIGINT", rows, view -> {
            assertThat(SparkSession.views).containsOnlyKeys(view);
            List<Row> viewRows = SparkSession.views.get(view);
            assertThat(viewRows).hasSize(2);
            assertThat(viewRows.getFirst().schema().fieldNames()).containsExactly("run_id", "total_files");
            assertThat(viewRows.get(1).get(1)).isNull();
            return List.of(view);
        });

        assertThat(seen.getFirst()).matches("migration_rows_[0-9a-f]{32}");
        assertThat(SparkSession.views).as("dropped afterwards").isEmpty();
    }

    @Test
    void dropsTheViewWhenTheBodyFails() {
        assertThatThrownBy(() -> executor.withRows("run_id STRING", List.<Object[]>of(new Object[] {"run-1"}),
                view -> {
                    throw new SqlExecutionException("merge conflict", null, true);
                }))
                .hasMessage("merge conflict");

        assertThat(SparkSession.views).isEmpty();
    }

    @Test
    void callsSparkWithSparksLoaderAsTheContextClassLoaderAndRestoresTheApplications() {
        ClassLoader sparkLoader = getClass().getClassLoader();
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader applicationLoader = new URLClassLoader(new URL[0], null);
        Thread.currentThread().setContextClassLoader(applicationLoader);
        try {
            executor.query("SELECT 1", Map.of());

            assertThat(SparkSession.contextLoaders).containsExactly(sparkLoader);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(applicationLoader);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void wrapsSparkErrorsWithTheirMessage() {
        assertThatThrownBy(() -> executor.query("SELECT * FROM FAIL", Map.of()))
                .isInstanceOf(SqlExecutionException.class)
                .hasMessage("Spark SQL failed: [TABLE_OR_VIEW_NOT_FOUND] The table cannot be found");
    }

    @Test
    void marksAStatementSparkCannotAnalyseAsNotRetryable() {
        assertThatThrownBy(() -> executor.update("DELETE FROM FAIL", Map.of()))
                .isInstanceOfSatisfying(SqlExecutionException.class, e -> assertThat(e.isRetryable()).isFalse());
    }

    @Test
    void marksAMissingPermissionAsNotRetryable() {
        assertThatThrownBy(() -> executor.update("DELETE FROM DENIED", Map.of()))
                .isInstanceOfSatisfying(SqlExecutionException.class, e -> assertThat(e.isRetryable()).isFalse());
    }

    @Test
    void marksADeltaCommitConflictAsRetryable() {
        assertThatThrownBy(() -> executor.update("MERGE INTO CONFLICT", Map.of()))
                .isInstanceOfSatisfying(SqlExecutionException.class, e -> assertThat(e.isRetryable()).isTrue());
    }

    @Test
    void marksATransientFailureAsRetryable() {
        assertThatThrownBy(() -> executor.update("MERGE INTO LOST", Map.of()))
                .isInstanceOfSatisfying(SqlExecutionException.class, e -> assertThat(e.isRetryable()).isTrue());
    }

    @Test
    void marksRowsSparkCannotTypeAsNotRetryable() {
        assertThatThrownBy(() -> executor.withRows("id UNKNOWN_TYPE", List.of(), view -> 0))
                .isInstanceOfSatisfying(SqlExecutionException.class, e -> assertThat(e.isRetryable()).isFalse());
    }

    @Test
    void explainsWhenSparkIsNotAvailable() {
        SparkSqlExecutor withoutSpark = new SparkSqlExecutor(() -> new URLClassLoader(new URL[0], null));

        assertThatThrownBy(() -> withoutSpark.query("SELECT 1", Map.of()))
                .as("not retryable")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Spark is not available on the system class path");
    }
}
