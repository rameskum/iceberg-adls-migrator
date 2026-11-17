package com.tds.pde.migration.state;

import com.tds.pde.migration.config.MigrationProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Repository;

/**
 * The control, run and file state tables (Unity Catalog Delta tables, shared by every migration job).
 *
 * <p>Uses only {@code CREATE TABLE IF NOT EXISTS}, {@code INSERT}, {@code UPDATE} and {@code SELECT} with named
 * parameters, so the same statements run through Spark in production and on H2 in tests. Every call is retried
 * with jittered backoff, which also absorbs Delta's concurrent-modification conflicts between parallel jobs,
 * before it fails with {@link StateStoreException}.
 */
@Slf4j
@Repository
public class MigrationStateRepository {

    private static final int FILE_ROWS_PER_INSERT = 100;
    private static final int MAX_ERROR_LENGTH = 4000;
    private static final String DDL = "db/databricks/migration-state.sql";

    private static final List<String> CONTROL_COLUMNS = List.of("source_dataset_id", "destination_dataset_id",
            "source_table_location", "destination_location", "status", "last_run_id", "format_version", "snapshot_id",
            "snapshot_timestamp", "schema_id", "partition_specs", "total_partitions", "total_files", "total_records",
            "total_delete_files", "total_bytes", "completed_partitions", "completed_files", "failed_files",
            "records_written", "records_deleted", "started_at", "finished_at", "error", "updated_at");
    private static final List<String> RUN_COLUMNS = List.of("run_id", "status", "source_dataset_id",
            "destination_dataset_id", "source_table_location", "destination_location", "snapshot_id",
            "source_schema_json", "output_schema_json", "total_partitions", "total_files", "total_records",
            "started_at", "finished_at", "error", "updated_at");
    private static final List<String> FILE_COLUMNS = List.of("run_id", "file_key", "source_path", "spec_id",
            "partition_path", "destination_path", "physical_records", "status", "records_written", "records_deleted",
            "attempts", "last_error", "updated_at");

    private final SqlExecutor sql;
    private final String controlTable;
    private final String runTable;
    private final String fileTable;
    private final int attempts;
    private final Duration initialBackoff;

    @Autowired
    public MigrationStateRepository(SqlExecutor sql, MigrationProperties properties) {
        this(sql, properties, Duration.ofSeconds(1));
    }

    MigrationStateRepository(SqlExecutor sql, MigrationProperties properties, Duration initialBackoff) {
        this.sql = sql;
        this.controlTable = properties.state().controlTable();
        this.runTable = properties.state().runTable();
        this.fileTable = properties.state().fileTable();
        this.attempts = properties.state().writeRetries();
        this.initialBackoff = initialBackoff;
    }

    /** Runs the bundled Delta DDL; every statement is {@code CREATE TABLE IF NOT EXISTS}. */
    public void createTablesIfMissing() {
        String script;
        try {
            script = new ClassPathResource(DDL).getContentAsString(StandardCharsets.UTF_8)
                    .replace("${control_table}", controlTable)
                    .replace("${run_table}", runTable)
                    .replace("${file_table}", fileTable);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + DDL, e);
        }
        // Statements end with ';' at the end of a line, so a ';' inside a comment string does not split one.
        Arrays.stream(script.replaceAll("(?m)^\\s*--.*$", "").split(";\\s*(\\R|$)"))
                .map(String::strip)
                .filter(statement -> !statement.isEmpty())
                .forEach(statement -> retrying("create state tables", () -> sql.update(statement, Map.of())));
        log.info("State tables ready: controlTable={} runTable={} fileTable={}", controlTable, runTable, fileTable);
    }

    // ---- control table ----------------------------------------------------------------------------------------

    /** Writes the control row of this source and destination, creating it on the first run. */
    public void saveControl(ControlRecord control) {
        Map<String, Object> params = controlParams(control);
        String assignments = String.join(", ", CONTROL_COLUMNS.stream()
                .filter(column -> !column.equals("source_table_location") && !column.equals("destination_location"))
                .map(column -> column + " = :" + column)
                .toList());
        long updated = retrying("update control row", () -> sql.update("UPDATE " + controlTable + " SET "
                + assignments + " WHERE source_table_location = :source_table_location"
                + " AND destination_location = :destination_location", params));
        // The run lock guarantees one writer per destination, so update-then-insert cannot race.
        if (updated == 0 || updated < 0 && findControl(control.sourceTableLocation(),
                control.destinationLocation()).isEmpty()) {
            retrying("insert control row", () -> sql.update("INSERT INTO " + controlTable + " ("
                    + String.join(", ", CONTROL_COLUMNS) + ") VALUES (:" + String.join(", :", CONTROL_COLUMNS) + ")",
                    params));
        }
    }

    public Optional<ControlRecord> findControl(String sourceTableLocation, String destinationLocation) {
        return retrying("read control row", () -> sql.query("SELECT " + String.join(", ", CONTROL_COLUMNS)
                        + " FROM " + controlTable + " WHERE source_table_location = :source"
                        + " AND destination_location = :destination",
                Map.of("source", sourceTableLocation, "destination", destinationLocation)))
                .stream().findFirst().map(MigrationStateRepository::control);
    }

    // ---- run table --------------------------------------------------------------------------------------------

    public void insertRun(RunRecord run) {
        Map<String, Object> params = new HashMap<>();
        params.put("run_id", run.runId());
        params.put("status", run.status().name());
        params.put("source_dataset_id", run.sourceDatasetId());
        params.put("destination_dataset_id", run.destinationDatasetId());
        params.put("source_table_location", run.sourceTableLocation());
        params.put("destination_location", run.destinationLocation());
        params.put("snapshot_id", run.snapshotId());
        params.put("source_schema_json", run.sourceSchemaJson());
        params.put("output_schema_json", run.outputSchemaJson());
        params.put("total_partitions", run.totalPartitions());
        params.put("total_files", run.totalFiles());
        params.put("total_records", run.totalRecords());
        params.put("started_at", timestamp(run.startedAt()));
        params.put("finished_at", timestamp(run.finishedAt()));
        params.put("error", truncate(run.error()));
        params.put("updated_at", timestamp(Instant.now()));
        retrying("insert run " + run.runId(), () -> sql.update("INSERT INTO " + runTable + " ("
                + String.join(", ", RUN_COLUMNS) + ") VALUES (:" + String.join(", :", RUN_COLUMNS) + ")", params));
    }

    public Optional<RunRecord> findRun(String runId) {
        return firstRun("read run " + runId, "WHERE run_id = :run_id", Map.of("run_id", runId));
    }

    /** The most recently started RUNNING or FAILED run copying this source to this destination. */
    public Optional<RunRecord> findLatestUnfinishedRun(String sourceTableLocation, String destinationLocation) {
        return firstRun("find unfinished run", "WHERE source_table_location = :source"
                        + " AND destination_location = :destination AND status IN ('RUNNING', 'FAILED')"
                        + " ORDER BY started_at DESC LIMIT 1",
                Map.of("source", sourceTableLocation, "destination", destinationLocation));
    }

    /** The most recently started run that still owns output at this destination. */
    public Optional<RunRecord> findLatestRunForDestination(String destinationLocation) {
        return firstRun("find run for destination", "WHERE destination_location = :destination"
                        + " AND status <> 'SUPERSEDED' ORDER BY started_at DESC LIMIT 1",
                Map.of("destination", destinationLocation));
    }

    public void updateRunStatus(String runId, RunRecord.Status status, @Nullable Instant finishedAt,
                                @Nullable String error) {
        Map<String, Object> params = new HashMap<>();
        params.put("status", status.name());
        params.put("finished_at", timestamp(finishedAt));
        params.put("error", truncate(error));
        params.put("updated_at", timestamp(Instant.now()));
        params.put("run_id", runId);
        updateOne("update run " + runId, "UPDATE " + runTable + " SET status = :status,"
                + " finished_at = :finished_at, error = :error, updated_at = :updated_at WHERE run_id = :run_id", params);
    }

    /** Marks every other run that owns output at the destination as superseded by a clean start. */
    public long supersedeRuns(String destinationLocation) {
        return retrying("supersede runs", () -> sql.update("UPDATE " + runTable + " SET status = 'SUPERSEDED',"
                        + " updated_at = :updated_at WHERE destination_location = :destination"
                        + " AND status <> 'SUPERSEDED'",
                Map.of("updated_at", timestamp(Instant.now()), "destination", destinationLocation)));
    }

    // ---- file table -------------------------------------------------------------------------------------------

    /** Inserts in multi-row statements: every statement is a Delta commit, so fewer is faster. */
    public void insertFiles(List<FileRecord> files) {
        for (int start = 0; start < files.size(); start += FILE_ROWS_PER_INSERT) {
            List<FileRecord> chunk = files.subList(start, Math.min(files.size(), start + FILE_ROWS_PER_INSERT));
            Map<String, Object> params = new HashMap<>();
            List<String> rows = new ArrayList<>(chunk.size());
            for (int i = 0; i < chunk.size(); i++) {
                FileRecord file = chunk.get(i);
                Map<String, Object> values = new HashMap<>();
                values.put("run_id", file.runId());
                values.put("file_key", file.fileKey());
                values.put("source_path", file.sourcePath());
                values.put("spec_id", file.specId());
                values.put("partition_path", file.partitionPath());
                values.put("destination_path", file.destinationPath());
                values.put("physical_records", file.physicalRecords());
                values.put("status", file.status().name());
                values.put("records_written", file.recordsWritten());
                values.put("records_deleted", file.recordsDeleted());
                values.put("attempts", file.attempts());
                values.put("last_error", truncate(file.lastError()));
                values.put("updated_at", timestamp(file.updatedAt()));
                String prefix = "f" + i + "_";
                values.forEach((column, value) -> params.put(prefix + column, value));
                rows.add("(:" + prefix + String.join(", :" + prefix, FILE_COLUMNS) + ")");
            }
            String statement = "INSERT INTO " + fileTable + " (" + String.join(", ", FILE_COLUMNS) + ") VALUES "
                    + String.join(", ", rows);
            retrying("insert " + chunk.size() + " file rows", () -> sql.update(statement, params));
        }
    }

    public List<FileRecord> findFiles(String runId) {
        return retrying("read files of run " + runId, () -> sql.query("SELECT " + String.join(", ", FILE_COLUMNS)
                + " FROM " + fileTable + " WHERE run_id = :run_id", Map.of("run_id", runId)))
                .stream().map(MigrationStateRepository::file).toList();
    }

    public void markFileCompleted(String runId, String fileKey, long recordsWritten, long recordsDeleted) {
        updateOne("mark file completed", "UPDATE " + fileTable + " SET status = 'COMPLETED',"
                        + " records_written = :written, records_deleted = :deleted, attempts = attempts + 1,"
                        + " last_error = NULL, updated_at = :updated_at WHERE run_id = :run_id AND file_key = :file_key",
                Map.of("written", recordsWritten, "deleted", recordsDeleted, "updated_at", timestamp(Instant.now()),
                        "run_id", runId, "file_key", fileKey));
    }

    public void markFileFailed(String runId, String fileKey, String error) {
        updateOne("mark file failed", "UPDATE " + fileTable + " SET status = 'FAILED', attempts = attempts + 1,"
                        + " last_error = :error, updated_at = :updated_at WHERE run_id = :run_id AND file_key = :file_key",
                Map.of("error", truncate(error), "updated_at", timestamp(Instant.now()), "run_id", runId,
                        "file_key", fileKey));
    }

    // ---- helpers ----------------------------------------------------------------------------------------------

    private Optional<RunRecord> firstRun(String operation, String condition, Map<String, ?> params) {
        return retrying(operation, () -> sql.query("SELECT " + String.join(", ", RUN_COLUMNS) + " FROM "
                + runTable + " " + condition, params)).stream().findFirst().map(MigrationStateRepository::run);
    }

    private void updateOne(String operation, String statement, Map<String, ?> params) {
        long updated = retrying(operation, () -> sql.update(statement, params));
        if (updated >= 0 && updated != 1) {
            throw new IllegalStateException(operation + " updated " + updated + " rows instead of 1");
        }
    }

    private <T> T retrying(String operation, Supplier<T> call) {
        Duration backoff = initialBackoff;
        for (int attempt = 1; ; attempt++) {
            try {
                return call.get();
            } catch (SqlExecutionException e) {
                if (attempt >= attempts) {
                    throw new StateStoreException("State store operation failed after " + attempt + " attempts: "
                            + operation + " (" + e.getMessage() + ")", e);
                }
                // Jitter keeps parallel jobs that conflicted on a Delta commit from retrying in lockstep.
                Duration wait = backoff.plusMillis(ThreadLocalRandom.current().nextLong(backoff.toMillis() + 1));
                log.warn("State store operation failed, retrying: operation={} attempt={}/{} retryIn={} reason={}",
                        operation, attempt, attempts, wait, e.getMessage());
                sleep(wait);
                backoff = backoff.multipliedBy(2);
            }
        }
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StateStoreException("Interrupted while waiting to retry", e);
        }
    }

    private static Map<String, Object> controlParams(ControlRecord control) {
        Map<String, Object> params = new HashMap<>();
        params.put("source_dataset_id", control.sourceDatasetId());
        params.put("destination_dataset_id", control.destinationDatasetId());
        params.put("source_table_location", control.sourceTableLocation());
        params.put("destination_location", control.destinationLocation());
        params.put("status", control.status().name());
        params.put("last_run_id", control.lastRunId());
        params.put("format_version", control.formatVersion());
        params.put("snapshot_id", control.snapshotId());
        params.put("snapshot_timestamp", timestamp(control.snapshotTimestamp()));
        params.put("schema_id", control.schemaId());
        params.put("partition_specs", control.partitionSpecs());
        params.put("total_partitions", control.totalPartitions());
        params.put("total_files", control.totalFiles());
        params.put("total_records", control.totalRecords());
        params.put("total_delete_files", control.totalDeleteFiles());
        params.put("total_bytes", control.totalBytes());
        params.put("completed_partitions", control.completedPartitions());
        params.put("completed_files", control.completedFiles());
        params.put("failed_files", control.failedFiles());
        params.put("records_written", control.recordsWritten());
        params.put("records_deleted", control.recordsDeleted());
        params.put("started_at", timestamp(control.startedAt()));
        params.put("finished_at", timestamp(control.finishedAt()));
        params.put("error", truncate(control.error()));
        params.put("updated_at", timestamp(Instant.now()));
        return params;
    }

    private static ControlRecord control(Map<String, Object> row) {
        return new ControlRecord((String) row.get("source_dataset_id"), (String) row.get("destination_dataset_id"),
                (String) row.get("source_table_location"), (String) row.get("destination_location"),
                RunRecord.Status.valueOf((String) row.get("status")), (String) row.get("last_run_id"),
                integer(row.get("format_version")), nullableLong(row.get("snapshot_id")),
                instant(row.get("snapshot_timestamp")), nullableInteger(row.get("schema_id")),
                (String) row.get("partition_specs"), number(row.get("total_partitions")),
                number(row.get("total_files")), number(row.get("total_records")),
                number(row.get("total_delete_files")), number(row.get("total_bytes")),
                number(row.get("completed_partitions")), number(row.get("completed_files")),
                number(row.get("failed_files")), number(row.get("records_written")),
                number(row.get("records_deleted")), instant(row.get("started_at")),
                instant(row.get("finished_at")), (String) row.get("error"));
    }

    private static RunRecord run(Map<String, Object> row) {
        return new RunRecord((String) row.get("run_id"), RunRecord.Status.valueOf((String) row.get("status")),
                (String) row.get("source_dataset_id"), (String) row.get("destination_dataset_id"),
                (String) row.get("source_table_location"), (String) row.get("destination_location"),
                nullableLong(row.get("snapshot_id")), text(row.get("source_schema_json")),
                text(row.get("output_schema_json")), number(row.get("total_partitions")),
                number(row.get("total_files")), number(row.get("total_records")), instant(row.get("started_at")),
                instant(row.get("finished_at")), (String) row.get("error"));
    }

    private static FileRecord file(Map<String, Object> row) {
        return new FileRecord((String) row.get("run_id"), (String) row.get("file_key"),
                (String) row.get("source_path"), integer(row.get("spec_id")), (String) row.get("partition_path"),
                (String) row.get("destination_path"), number(row.get("physical_records")),
                FileRecord.Status.valueOf((String) row.get("status")), nullableLong(row.get("records_written")),
                nullableLong(row.get("records_deleted")), integer(row.get("attempts")),
                (String) row.get("last_error"), instant(row.get("updated_at")));
    }

    private static long number(Object value) {
        return ((Number) value).longValue();
    }

    private static int integer(Object value) {
        return ((Number) value).intValue();
    }

    private static @Nullable Long nullableLong(@Nullable Object value) {
        return value == null ? null : ((Number) value).longValue();
    }

    private static @Nullable Integer nullableInteger(@Nullable Object value) {
        return value == null ? null : ((Number) value).intValue();
    }

    /** Large text may come back as a character LOB from JDBC engines. */
    private static String text(Object value) {
        if (value instanceof java.sql.Clob clob) {
            try {
                return clob.getSubString(1, Math.toIntExact(clob.length()));
            } catch (java.sql.SQLException e) {
                throw new SqlExecutionException("Cannot read text column", e);
            }
        }
        return (String) value;
    }

    /** Engines return TIMESTAMP as java.sql.Timestamp, Instant or a local date-time in UTC, depending on settings. */
    private static @Nullable Instant instant(@Nullable Object value) {
        return switch (value) {
            case null -> null;
            case Timestamp timestamp -> timestamp.toInstant();
            case Instant instant -> instant;
            case OffsetDateTime dateTime -> dateTime.toInstant();
            case LocalDateTime dateTime -> dateTime.toInstant(ZoneOffset.UTC);
            default -> throw new IllegalStateException("Unexpected timestamp value " + value.getClass().getName());
        };
    }

    private static @Nullable Timestamp timestamp(@Nullable Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static @Nullable String truncate(@Nullable String text) {
        return text == null || text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH);
    }
}
