package com.tds.pde.migration.state;

import com.tds.pde.migration.config.MigrationProperties;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * The control, run and file state tables (Unity Catalog Delta tables, shared by every migration job).
 *
 * <p>Every write is a {@code MERGE INTO} whose source is the rows to write, typed by the table's {@link StateTable}
 * definition, so a write never depends on whether an earlier attempt landed: inserts skip rows already present and
 * upserts converge. Reads are {@code SELECT}s with named parameters. Every call is retried with jittered backoff,
 * which also absorbs Delta's concurrent-modification conflicts between parallel jobs, before it fails with
 * {@link StateStoreException}.
 */
@Slf4j
@Repository
public class MigrationStateRepository {

    /** Rows per MERGE source: bounded so the rows, which travel inside the query plan, stay small. */
    private static final int FILE_ROWS_PER_MERGE = 5_000;
    private static final int MAX_ERROR_LENGTH = 4000;

    private static final String CONTROL_KEY =
            "t.source_dataset_id = s.source_dataset_id AND t.target_dataset_id = s.target_dataset_id";
    private static final String RUN_KEY = "t.run_id = s.run_id";
    private static final String FILE_KEY = "t.run_id = s.run_id AND t.source_path = s.source_path";

    private final SqlExecutor sql;
    private final StateTable controlTable;
    private final StateTable runTable;
    private final StateTable fileTable;
    private final int attempts;
    private final Duration initialBackoff;

    @Autowired
    public MigrationStateRepository(SqlExecutor sql, MigrationProperties properties) {
        this(sql, properties, Duration.ofSeconds(1));
    }

    MigrationStateRepository(SqlExecutor sql, MigrationProperties properties, Duration initialBackoff) {
        this.sql = sql;
        this.controlTable = StateTable.control(properties.state().controlTable());
        this.runTable = StateTable.run(properties.state().runTable());
        this.fileTable = StateTable.file(properties.state().fileTable());
        this.attempts = properties.state().writeRetries();
        this.initialBackoff = initialBackoff;
    }

    public void createTablesIfMissing() {
        for (StateTable table : List.of(controlTable, runTable, fileTable)) {
            retrying("create state table " + table.name(), () -> sql.update(table.createStatement(), Map.of()));
        }
        log.info("State tables ready: controlTable={} runTable={} fileTable={}", controlTable.name(),
                runTable.name(), fileTable.name());
    }

    // ---- control table ----------------------------------------------------------------------------------------

    /** Writes the control row of this table migration, creating it on the first run. */
    public void saveControl(ControlRecord control) {
        merge("save control row", controlTable, List.of(controlRow(control)), CONTROL_KEY,
                "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
    }

    public Optional<ControlRecord> findControl(String sourceDatasetId, String targetDatasetId) {
        return select("read control row", controlTable, "WHERE source_dataset_id = :source"
                        + " AND target_dataset_id = :target",
                Map.of("source", sourceDatasetId, "target", targetDatasetId))
                .stream().findFirst().map(MigrationStateRepository::control);
    }

    // ---- run table --------------------------------------------------------------------------------------------

    public void insertRun(RunRecord run) {
        Map<String, Object> row = new HashMap<>();
        row.put("run_id", run.runId());
        row.put("status", run.status().name());
        row.put("source_dataset_id", run.sourceDatasetId());
        row.put("target_dataset_id", run.targetDatasetId());
        row.put("source_location", run.sourceLocation());
        row.put("target_location", run.targetLocation());
        row.put("snapshot_id", run.snapshotId());
        row.put("source_schema_json", run.sourceSchemaJson());
        row.put("output_schema_json", run.outputSchemaJson());
        row.put("started_at", timestamp(run.startedAt()));
        row.put("finished_at", timestamp(run.finishedAt()));
        row.put("error", truncate(run.error()));
        merge("insert run " + run.runId(), runTable, List.of(row), RUN_KEY, "WHEN NOT MATCHED THEN INSERT *");
    }

    public Optional<RunRecord> findRun(String runId) {
        return firstRun("read run " + runId, "WHERE run_id = :run_id", Map.of("run_id", runId));
    }

    /** The most recently started RUNNING or FAILED run copying this source to this target. */
    public Optional<RunRecord> findLatestUnfinishedRun(String sourceLocation, String targetLocation) {
        return firstRun("find unfinished run", "WHERE source_location = :source"
                        + " AND target_location = :target AND status IN ('RUNNING', 'FAILED')"
                        + " ORDER BY started_at DESC LIMIT 1",
                Map.of("source", sourceLocation, "target", targetLocation));
    }

    /** The most recently started run that still owns output at this target. */
    public Optional<RunRecord> findLatestRunForTarget(String targetLocation) {
        return firstRun("find run for target", "WHERE target_location = :target"
                        + " AND status <> 'SUPERSEDED' ORDER BY started_at DESC LIMIT 1",
                Map.of("target", targetLocation));
    }

    public void updateRunStatus(String runId, RunRecord.Status status, @Nullable Instant finishedAt,
                                @Nullable String error) {
        Map<String, Object> row = new HashMap<>();
        row.put("run_id", runId);
        row.put("status", status.name());
        row.put("finished_at", timestamp(finishedAt));
        row.put("error", truncate(error));
        mergeOne("update run " + runId, runTable, row, RUN_KEY, "WHEN MATCHED THEN UPDATE SET status = s.status,"
                + " finished_at = s.finished_at, error = s.error");
    }

    /** Marks every other run that owns output at the target as superseded by a clean start. */
    public long supersedeRuns(String targetLocation) {
        return merge("supersede runs", runTable, List.of(Map.of("target_location", targetLocation)),
                "t.target_location = s.target_location AND t.status <> 'SUPERSEDED'",
                "WHEN MATCHED THEN UPDATE SET status = 'SUPERSEDED'");
    }

    /**
     * Deletes the file rows of every superseded run of the target: their output is gone and they cannot be resumed,
     * so the rows only slow down the shared file table. The run rows stay as the target's history.
     *
     * @return file rows deleted when the engine reports it, otherwise -1
     */
    public long deleteSupersededFiles(String targetLocation) {
        List<Map<String, ?>> runs = select("find superseded runs", runTable, List.of("run_id"),
                "WHERE target_location = :target AND status = 'SUPERSEDED'", Map.of("target", targetLocation))
                .stream().<Map<String, ?>>map(row -> Map.of("run_id", row.get("run_id"))).toList();
        long deleted = 0;
        for (int start = 0; start < runs.size(); start += FILE_ROWS_PER_MERGE) {
            List<Map<String, ?>> chunk = runs.subList(start, Math.min(runs.size(), start + FILE_ROWS_PER_MERGE));
            long affected = merge("delete file rows of " + chunk.size() + " superseded runs", fileTable, chunk,
                    "t.run_id = s.run_id", "WHEN MATCHED THEN DELETE");
            deleted = affected < 0 || deleted < 0 ? -1 : deleted + affected;
        }
        return deleted;
    }

    // ---- file table -------------------------------------------------------------------------------------------

    public void insertFiles(List<FileRecord> files) {
        for (int start = 0; start < files.size(); start += FILE_ROWS_PER_MERGE) {
            List<FileRecord> chunk = files.subList(start, Math.min(files.size(), start + FILE_ROWS_PER_MERGE));
            List<Map<String, ?>> rows = new ArrayList<>(chunk.size());
            for (FileRecord file : chunk) {
                Map<String, Object> row = new HashMap<>();
                row.put("run_id", file.runId());
                row.put("source_path", file.sourcePath());
                row.put("target_path", file.targetPath());
                row.put("physical_records", file.physicalRecords());
                row.put("status", file.status().name());
                row.put("records_written", file.recordsWritten());
                row.put("error", truncate(file.error()));
                rows.add(row);
            }
            merge("insert " + chunk.size() + " file rows", fileTable, rows, FILE_KEY,
                    "WHEN NOT MATCHED THEN INSERT *");
        }
    }

    public List<FileRecord> findFiles(String runId) {
        return select("read files of run " + runId, fileTable, "WHERE run_id = :run_id", Map.of("run_id", runId))
                .stream().map(MigrationStateRepository::file).toList();
    }

    /** Writes the outcomes of files of one run; each must match an existing file row. */
    public void recordFileOutcomes(String runId, List<FileOutcome> outcomes) {
        for (int start = 0; start < outcomes.size(); start += FILE_ROWS_PER_MERGE) {
            List<FileOutcome> chunk = outcomes.subList(start, Math.min(outcomes.size(), start + FILE_ROWS_PER_MERGE));
            List<Map<String, ?>> rows = new ArrayList<>(chunk.size());
            for (FileOutcome outcome : chunk) {
                Map<String, Object> row = new HashMap<>();
                row.put("run_id", runId);
                row.put("source_path", outcome.sourcePath());
                row.put("status", outcome.status().name());
                row.put("records_written", outcome.recordsWritten());
                row.put("error", truncate(outcome.error()));
                rows.add(row);
            }
            String operation = "record " + chunk.size() + " file outcomes";
            long updated = merge(operation, fileTable, rows, FILE_KEY, "WHEN MATCHED THEN UPDATE SET"
                    + " status = s.status, records_written = s.records_written, error = s.error");
            if (updated >= 0 && updated != chunk.size()) {
                throw new IllegalStateException(operation + " updated " + updated + " rows instead of "
                        + chunk.size());
            }
        }
    }

    // ---- helpers ----------------------------------------------------------------------------------------------

    private Optional<RunRecord> firstRun(String operation, String condition, Map<String, ?> params) {
        return select(operation, runTable, condition, params).stream().findFirst()
                .map(MigrationStateRepository::run);
    }

    private List<Map<String, Object>> select(String operation, StateTable table, String condition,
                                             Map<String, ?> params) {
        return select(operation, table, table.columnNames(), condition, params);
    }

    private List<Map<String, Object>> select(String operation, StateTable table, List<String> columns,
                                             String condition, Map<String, ?> params) {
        return retrying(operation, () -> sql.query("SELECT " + String.join(", ", columns) + " FROM "
                + table.name() + " " + condition, params));
    }

    /**
     * Merges {@code rows}, which all hold the same columns, into {@code table} as source {@code s} against target
     * {@code t}.
     *
     * @return rows affected when the engine reports it, otherwise -1
     */
    private long merge(String operation, StateTable table, List<? extends Map<String, ?>> rows, String on,
                       String clauses) {
        String schema = table.rowSchema(rows.getFirst());
        List<String> columns = table.columnNames().stream().filter(rows.getFirst()::containsKey).toList();
        List<Object[]> values = rows.stream().map(row -> columns.stream().map(row::get).toArray()).toList();
        return retrying(operation, () -> sql.withRows(schema, values, source -> sql.update("MERGE INTO "
                + table.name() + " t USING " + source + " s ON " + on + " " + clauses, Map.of())));
    }

    /** Merges one row that must match exactly one existing row. */
    private void mergeOne(String operation, StateTable table, Map<String, ?> row, String on, String clauses) {
        long updated = merge(operation, table, List.of(row), on, clauses);
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

    private static Map<String, Object> controlRow(ControlRecord control) {
        Map<String, Object> row = new HashMap<>();
        row.put("source_dataset_id", control.sourceDatasetId());
        row.put("target_dataset_id", control.targetDatasetId());
        row.put("status", control.status().name());
        row.put("last_run_id", control.lastRunId());
        row.put("snapshot_id", control.snapshotId());
        row.put("total_files", control.totalFiles());
        row.put("total_records", control.totalRecords());
        return row;
    }

    private static ControlRecord control(Map<String, Object> row) {
        return new ControlRecord((String) row.get("source_dataset_id"), (String) row.get("target_dataset_id"),
                RunRecord.Status.valueOf((String) row.get("status")), (String) row.get("last_run_id"),
                nullableLong(row.get("snapshot_id")), number(row.get("total_files")),
                number(row.get("total_records")));
    }

    private static RunRecord run(Map<String, Object> row) {
        return new RunRecord((String) row.get("run_id"), RunRecord.Status.valueOf((String) row.get("status")),
                (String) row.get("source_dataset_id"), (String) row.get("target_dataset_id"),
                (String) row.get("source_location"), (String) row.get("target_location"),
                nullableLong(row.get("snapshot_id")), (String) row.get("source_schema_json"),
                (String) row.get("output_schema_json"), instant(row.get("started_at")),
                instant(row.get("finished_at")), (String) row.get("error"));
    }

    private static FileRecord file(Map<String, Object> row) {
        return new FileRecord((String) row.get("run_id"), (String) row.get("source_path"),
                (String) row.get("target_path"), number(row.get("physical_records")),
                FileRecord.Status.valueOf((String) row.get("status")), nullableLong(row.get("records_written")),
                (String) row.get("error"));
    }

    private static long number(Object value) {
        return ((Number) value).longValue();
    }

    private static @Nullable Long nullableLong(@Nullable Object value) {
        return value == null ? null : ((Number) value).longValue();
    }

    /** Spark returns TIMESTAMP as java.sql.Timestamp, or as Instant when its Java 8 time API is enabled. */
    private static @Nullable Instant instant(@Nullable Object value) {
        return switch (value) {
            case null -> null;
            case Timestamp timestamp -> timestamp.toInstant();
            case Instant instant -> instant;
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
