package com.tds.pde.migration.state;

import static org.assertj.core.api.Assertions.assertThat;

import com.tds.pde.migration.testing.TestProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/** The SQL the repository sends, against an executor that records it. */
class MigrationStateRepositoryTest {

    private final RecordingSql sql = new RecordingSql();
    private final MigrationStateRepository repository =
            new MigrationStateRepository(sql, TestProperties.defaults(), Duration.ZERO);

    @Test
    void createsTheFileTableClusteredByRunAndTheRunTableByTarget() {
        sql.details.put("migration_run", List.of("target_location"));
        sql.details.put("migration_file", List.of("run_id"));

        repository.createTablesIfMissing();

        assertThat(sql.statements).hasSize(5);
        assertThat(sql.statements.get(0)).startsWith("CREATE TABLE IF NOT EXISTS migration_control ")
                .doesNotContain("CLUSTER BY");
        assertThat(sql.statements.get(1)).startsWith("CREATE TABLE IF NOT EXISTS migration_run ")
                .contains(" USING DELTA CLUSTER BY (target_location) COMMENT ");
        assertThat(sql.statements.get(2)).isEqualTo("DESCRIBE DETAIL migration_run");
        assertThat(sql.statements.get(3)).startsWith("CREATE TABLE IF NOT EXISTS migration_file ")
                .contains(" USING DELTA CLUSTER BY (run_id) COMMENT ");
        assertThat(sql.statements.get(4)).as("already clustered: no metadata commit")
                .isEqualTo("DESCRIBE DETAIL migration_file");
    }

    @Test
    void clustersExistingUnclusteredTables() {
        sql.details.put("migration_run", List.of());
        sql.details.put("migration_file", List.of());

        repository.createTablesIfMissing();

        assertThat(sql.statements).filteredOn(statement -> !statement.startsWith("CREATE TABLE")).containsExactly(
                "DESCRIBE DETAIL migration_run",
                "ALTER TABLE migration_run CLUSTER BY (target_location)",
                "DESCRIBE DETAIL migration_file",
                "ALTER TABLE migration_file CLUSTER BY (run_id)");
    }

    @Test
    void reclustersExistingTablesClusteredOnOtherColumns() {
        sql.details.put("migration_run", List.of("target_location"));
        sql.details.put("migration_file", List.of("run_id", "source_path"));

        repository.createTablesIfMissing();

        assertThat(sql.statements).filteredOn(statement -> !statement.startsWith("CREATE TABLE")).containsExactly(
                "DESCRIBE DETAIL migration_run",
                "DESCRIBE DETAIL migration_file",
                "ALTER TABLE migration_file CLUSTER BY (run_id)");
    }

    @Test
    void deletesTheFileRowsOfTheTargetsSupersededRuns() {
        sql.queryResult = List.of(Map.of("run_id", "run-1"), Map.of("run_id", "run-2"));

        repository.deleteSupersededFiles("abfss://data@acct.dfs.core.windows.net/sales/orders");

        assertThat(sql.statements).containsExactly(
                "SELECT run_id FROM migration_run WHERE target_location = :target AND status = 'SUPERSEDED'",
                "MERGE INTO migration_file t USING rows_1 s ON t.run_id = s.run_id WHEN MATCHED THEN DELETE");
        assertThat(sql.parameters.getFirst())
                .isEqualTo(Map.of("target", "abfss://data@acct.dfs.core.windows.net/sales/orders"));
        assertThat(sql.schemas).containsExactly("run_id STRING");
        assertThat(sql.rows.getFirst()).extracting(Arrays::asList)
                .containsExactly(List.of("run-1"), List.of("run-2"));
    }

    @Test
    void deletesNothingWhenNoRunOfTheTargetWasSuperseded() {
        repository.deleteSupersededFiles("abfss://data@acct.dfs.core.windows.net/sales/orders");

        assertThat(sql.statements).singleElement().asString().startsWith("SELECT run_id FROM migration_run ");
    }

    private static final class RecordingSql implements SqlExecutor {

        final List<String> statements = new ArrayList<>();
        final List<Map<String, ?>> parameters = new ArrayList<>();
        final List<String> schemas = new ArrayList<>();
        final List<List<Object[]>> rows = new ArrayList<>();
        List<Map<String, Object>> queryResult = List.of();
        /** clusteringColumns that DESCRIBE DETAIL reports, by table. */
        final Map<String, List<String>> details = new HashMap<>();

        @Override
        public long update(String statement, Map<String, ?> params) {
            statements.add(statement);
            parameters.add(params);
            return -1;
        }

        @Override
        public List<Map<String, Object>> query(String statement, Map<String, ?> params) {
            statements.add(statement);
            parameters.add(params);
            if (statement.startsWith("DESCRIBE DETAIL ")) {
                return List.of(Map.of("clusteringColumns",
                        details.get(statement.substring("DESCRIBE DETAIL ".length()))));
            }
            return queryResult;
        }

        @Override
        public <T> T withRows(String schema, List<Object[]> values, Function<String, T> body) {
            schemas.add(schema);
            rows.add(values);
            return body.apply("rows_" + rows.size());
        }
    }
}
