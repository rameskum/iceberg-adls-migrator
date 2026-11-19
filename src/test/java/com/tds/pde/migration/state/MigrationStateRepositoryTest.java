package com.tds.pde.migration.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import com.tds.pde.migration.testing.LogCapture;
import com.tds.pde.migration.testing.TestProperties;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** The SQL the repository sends, against an executor that records it. */
class MigrationStateRepositoryTest {

    private static final Instant STARTED = Instant.parse("2026-11-19T10:00:00Z");
    private static final Instant HEARTBEAT = Instant.parse("2026-11-19T10:05:00.123456Z");

    private final RecordingSql sql = new RecordingSql();
    private final MigrationStateRepository repository =
            new MigrationStateRepository(sql, TestProperties.defaults(), Duration.ZERO);

    @Test
    void createsTheTablesChecksTheirColumnsAndClustersTheFileTableByRun() {
        sql.details.put("migration_file", List.of("run_id"));

        repository.createTablesIfMissing();

        assertThat(sql.statements).hasSize(5);
        assertThat(sql.statements.get(0)).startsWith("CREATE TABLE IF NOT EXISTS migration_control ")
                .contains("target_dataset_id STRING NOT NULL", "heartbeat_at TIMESTAMP NOT NULL",
                        "completed_files BIGINT NOT NULL", "bytes_written BIGINT NOT NULL")
                .doesNotContain("CLUSTER BY");
        assertThat(sql.statements.get(1)).isEqualTo("SHOW COLUMNS IN migration_control");
        assertThat(sql.statements.get(2)).startsWith("CREATE TABLE IF NOT EXISTS migration_file ")
                .contains(" USING DELTA CLUSTER BY (run_id) COMMENT ");
        assertThat(sql.statements.get(3)).isEqualTo("SHOW COLUMNS IN migration_file");
        assertThat(sql.statements.get(4)).as("already clustered: no metadata commit")
                .isEqualTo("DESCRIBE DETAIL migration_file");
    }

    @Test
    void refusesATableCreatedByAnEarlierVersionWithOtherColumns() {
        sql.columns.put("migration_control", List.of("source_dataset_id", "target_dataset_id", "status",
                "last_run_id", "snapshot_id", "total_files", "total_records"));

        assertThatThrownBy(repository::createTablesIfMissing)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("State table migration_control has the columns [last_run_id, ")
                .hasMessageContaining("this version needs [bytes_written, completed_files, ")
                .hasMessageContaining("Drop it, or point the setting at a new table");
    }

    @Test
    void clustersAnExistingUnclusteredFileTable() {
        sql.details.put("migration_file", List.of());

        repository.createTablesIfMissing();

        assertThat(sql.statements).filteredOn(statement -> !statement.startsWith("CREATE TABLE")
                && !statement.startsWith("SHOW COLUMNS")).containsExactly(
                "DESCRIBE DETAIL migration_file",
                "ALTER TABLE migration_file CLUSTER BY (run_id)");
    }

    @Test
    void carriesOnUnclusteredWhenTheFileTableCannotBeAltered() {
        sql.details.put("migration_file", List.of());
        sql.failingStatement = "ALTER TABLE migration_file CLUSTER BY (run_id)";

        try (LogCapture logs = new LogCapture(MigrationStateRepository.class.getName())) {
            repository.createTablesIfMissing();

            assertThat(sql.statements).as("tried once, not retried")
                    .containsOnlyOnce("ALTER TABLE migration_file CLUSTER BY (run_id)");
            assertThat(logs.messagesAt(Level.WARN)).singleElement().asString()
                    .contains("ALTER TABLE migration_file CLUSTER BY (run_id)", "table=migration_file",
                            "clusterBy=[run_id]", "[PERMISSION_DENIED]");
            assertThat(logs.messages()).last().asString().startsWith("State tables ready:");
        }
    }

    @Test
    void insertsTheControlRowOnlyWhenTheMigrationHasNone() {
        repository.insertControl(control("run-1"));

        assertThat(sql.statements).containsExactly("MERGE INTO migration_control t USING rows_1 s ON "
                + "t.target_dataset_id = s.target_dataset_id WHEN NOT MATCHED THEN INSERT *");
        assertThat(sql.schemas.getFirst()).startsWith("target_dataset_id STRING, source_dataset_id STRING, ")
                .contains("heartbeat_at TIMESTAMP").endsWith("bytes_written BIGINT, error STRING");
    }

    @Test
    void replacesTheControlRowOnlyWhileItIsAsItWasRead() {
        ControlRecord expected = control("run-1");

        repository.replaceControl(expected, control("run-2"));

        assertThat(sql.statements).singleElement().asString()
                .startsWith("UPDATE migration_control SET ")
                .contains("run_id = CAST(:set_run_id AS STRING)", "heartbeat_at = CAST(:set_heartbeat_at AS TIMESTAMP)",
                        "finished_at = CAST(:set_finished_at AS TIMESTAMP)", "error = CAST(:set_error AS STRING)")
                .endsWith(" WHERE target_dataset_id = :target AND run_id = CAST(:was_run_id AS STRING)"
                        + " AND status = CAST(:was_status AS STRING)"
                        + " AND heartbeat_at = CAST(:was_heartbeat_at AS TIMESTAMP)");
        assertThat(sql.parameters.getFirst())
                .containsEntry("target", "target-1")
                .containsEntry("was_run_id", "run-1")
                .containsEntry("was_status", "RUNNING")
                .containsEntry("was_heartbeat_at", Timestamp.from(HEARTBEAT))
                .containsEntry("set_run_id", "run-2")
                .containsEntry("set_finished_at", null)
                .hasSize(22);
    }

    @Test
    void renewsTheHeartbeatAndProgressOnlyWhileTheRunOwnsTheRow() {
        sql.affectedRows = 0;
        Instant now = Instant.parse("2026-11-19T10:06:00Z");

        long updated = repository.renewControl("target-1", "run-1", now, new ControlRecord.Progress(2, 1, 30, 20, 4096));

        assertThat(updated).isZero();
        assertThat(sql.statements).singleElement().asString()
                .startsWith("UPDATE migration_control SET ")
                .contains("heartbeat_at = CAST(:set_heartbeat_at AS TIMESTAMP)",
                        "completed_files = CAST(:set_completed_files AS BIGINT)")
                .doesNotContain("status")
                .endsWith(" WHERE target_dataset_id = :target AND run_id = CAST(:was_run_id AS STRING)");
        assertThat(sql.parameters.getFirst()).isEqualTo(Map.of("target", "target-1", "was_run_id", "run-1",
                "set_heartbeat_at", Timestamp.from(now), "set_completed_files", 2L, "set_failed_files", 1L,
                "set_records_read", 30L, "set_records_written", 20L, "set_bytes_written", 4096L));
    }

    @Test
    void finishesTheRunOnlyWhileItOwnsTheRow() {
        Instant now = Instant.parse("2026-11-19T11:00:00Z");

        repository.finishControl("target-1", "run-1", ControlRecord.Status.FAILED, now, "1 of 3 files failed",
                ControlRecord.Progress.NONE);

        assertThat(sql.statements).singleElement().asString()
                .contains("status = CAST(:set_status AS STRING)", "finished_at = CAST(:set_finished_at AS TIMESTAMP)")
                .endsWith(" WHERE target_dataset_id = :target AND run_id = CAST(:was_run_id AS STRING)");
        assertThat(sql.parameters.getFirst()).containsEntry("set_status", "FAILED")
                .containsEntry("set_error", "1 of 3 files failed")
                .containsEntry("set_finished_at", Timestamp.from(now))
                .containsEntry("set_heartbeat_at", Timestamp.from(now));
    }

    @Test
    void readsTheControlRowsOfATarget() {
        Map<String, Object> row = new HashMap<>();
        row.put("target_dataset_id", "target-1");
        row.put("source_dataset_id", "source-1");
        row.put("source_location", "hdfs://nn/orders");
        row.put("target_location", "abfss://data@acct.dfs.core.windows.net/orders");
        row.put("run_id", "run-1");
        row.put("status", "RUNNING");
        row.put("snapshot_id", null);
        row.put("started_at", Timestamp.from(STARTED));
        row.put("heartbeat_at", Timestamp.from(HEARTBEAT));
        row.put("finished_at", null);
        row.put("total_files", 3L);
        row.put("total_records", 9L);
        row.put("completed_files", 1L);
        row.put("failed_files", 0L);
        row.put("records_read", 3L);
        row.put("records_written", 2L);
        row.put("bytes_written", 100L);
        row.put("error", null);
        sql.queryResult = List.of(row);

        assertThat(repository.findControls("target-1")).containsExactly(new ControlRecord("target-1", "source-1",
                "hdfs://nn/orders", "abfss://data@acct.dfs.core.windows.net/orders", "run-1",
                ControlRecord.Status.RUNNING, null, STARTED, HEARTBEAT, null, 3, 9,
                new ControlRecord.Progress(1, 0, 3, 2, 100), null));
        assertThat(sql.statements.getFirst()).startsWith("SELECT target_dataset_id, source_dataset_id, ")
                .endsWith(" FROM migration_control WHERE target_dataset_id = :target");
    }

    @Test
    void deletesTheControlRowOfARunThatLostTheRaceToInsertIt() {
        repository.deleteControl("target-1", "run-2");

        assertThat(sql.statements).containsExactly(
                "DELETE FROM migration_control WHERE target_dataset_id = :target AND run_id = :run_id");
        assertThat(sql.parameters.getFirst()).isEqualTo(Map.of("target", "target-1", "run_id", "run-2"));
    }

    @Test
    void deletesTheFileRowsOfARun() {
        repository.deleteFiles("run-1");

        assertThat(sql.statements).containsExactly("DELETE FROM migration_file WHERE run_id = :run_id");
        assertThat(sql.parameters.getFirst()).isEqualTo(Map.of("run_id", "run-1"));
    }

    @Test
    void retriesAConditionalUpdateThatConflicted() {
        sql.failuresBeforeSuccess = 2;

        repository.renewControl("target-1", "run-1", HEARTBEAT, ControlRecord.Progress.NONE);

        assertThat(sql.statements).hasSize(3).allSatisfy(statement -> assertThat(statement)
                .startsWith("UPDATE migration_control SET "));
    }

    private static ControlRecord control(String runId) {
        return new ControlRecord("target-1", "source-1", "hdfs://nn/orders",
                "abfss://data@acct.dfs.core.windows.net/orders", runId, ControlRecord.Status.RUNNING, 42L, STARTED,
                HEARTBEAT, null, 3, 9, ControlRecord.Progress.NONE, null);
    }

    private static final class RecordingSql implements SqlExecutor {

        final List<String> statements = new ArrayList<>();
        final List<Map<String, Object>> parameters = new ArrayList<>();
        final List<String> schemas = new ArrayList<>();
        final List<List<Object[]>> rows = new ArrayList<>();
        List<Map<String, Object>> queryResult = List.of();
        /** clusteringColumns that DESCRIBE DETAIL reports, by table. */
        final Map<String, List<String>> details = new HashMap<>();
        /** Columns that SHOW COLUMNS reports, by table; by default the table's own. */
        final Map<String, List<String>> columns = new HashMap<>();
        @Nullable String failingStatement;
        long affectedRows = -1;
        int failuresBeforeSuccess;

        @Override
        public long update(String statement, Map<String, ?> params) {
            statements.add(statement);
            parameters.add(new HashMap<>(params));
            if (statement.equals(failingStatement)) {
                throw new SqlExecutionException("[PERMISSION_DENIED] User does not have MANAGE on table", null);
            }
            if (failuresBeforeSuccess > 0) {
                failuresBeforeSuccess--;
                throw new SqlExecutionException("[DELTA_CONCURRENT_APPEND] ConcurrentAppendException", null);
            }
            return affectedRows;
        }

        @Override
        public List<Map<String, Object>> query(String statement, Map<String, ?> params) {
            statements.add(statement);
            parameters.add(new HashMap<>(params));
            if (statement.startsWith("DESCRIBE DETAIL ")) {
                return List.of(Map.of("clusteringColumns",
                        details.get(statement.substring("DESCRIBE DETAIL ".length()))));
            }
            if (statement.startsWith("SHOW COLUMNS IN ")) {
                String table = statement.substring("SHOW COLUMNS IN ".length());
                List<String> names = columns.getOrDefault(table, table.equals("migration_control")
                        ? StateTable.control(table).columnNames() : StateTable.file(table).columnNames());
                return names.stream().<Map<String, Object>>map(name -> Map.of("col_name", name)).toList();
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
