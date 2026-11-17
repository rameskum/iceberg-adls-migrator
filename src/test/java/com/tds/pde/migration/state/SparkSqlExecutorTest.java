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
    void reportsTheRowsDeltaSaysAStatementAffected() {
        SparkSession.nextResult = List.of(new Row(new String[] {"num_affected_rows"}, 1L));

        assertThat(executor.update("UPDATE runs SET status = :status", Map.of("status", "FAILED"))).isEqualTo(1);
    }

    @Test
    void reportsUnknownWhenTheEngineDoesNotSayHowManyRowsChanged() {
        assertThat(executor.update("CREATE TABLE IF NOT EXISTS runs (run_id STRING)", Map.of())).isEqualTo(-1);
    }

    @Test
    void wrapsSparkErrorsWithTheirMessage() {
        assertThatThrownBy(() -> executor.query("SELECT * FROM FAIL", Map.of()))
                .isInstanceOf(SqlExecutionException.class)
                .hasMessage("Spark SQL failed: [TABLE_OR_VIEW_NOT_FOUND] The table cannot be found");
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
