package com.tds.pde.migration.state;

import com.tds.pde.migration.config.MigrationProperties;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * The control and file state tables (Unity Catalog Delta tables, shared by every migration job).
 *
 * <p>The control row of a migration is changed only by conditional {@code UPDATE}s naming the run that owns it, or
 * the row state it was read in, so a run that lost the row changes nothing. Rows are inserted with a
 * {@code MERGE INTO} whose source is the rows to write, typed by the table's {@link StateTable} definition, so an
 * insert never depends on whether an earlier attempt landed. Every call is retried with jittered backoff, which also
 * absorbs Delta's concurrent-modification conflicts between parallel jobs, before it fails with
 * {@link StateStoreException}; a retried conditional update is evaluated again against the row as it now is.
 */
@Slf4j
@Repository
public class MigrationStateRepository {

    /** Rows per MERGE source: bounded so the rows, which travel inside the query plan, stay small. */
    private static final int FILE_ROWS_PER_MERGE = 5_000;
    private static final int MAX_ERROR_LENGTH = 4000;

    private static final String FILE_KEY = "t.run_id = s.run_id AND t.source_path = s.source_path";

    private final SqlExecutor sql;
    private final StateTable controlTable;
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
        this.fileTable = StateTable.file(properties.state().fileTable());
        this.attempts = properties.state().writeRetries();
        this.initialBackoff = initialBackoff;
    }

    /**
     * Creates the tables that do not exist yet, and checks that existing ones have this version's columns.
     *
     * @throws IllegalStateException when a table was created with other columns, e.g. by an earlier version
     */
    public void createTablesIfMissing() {
        for (StateTable table : List.of(controlTable, fileTable)) {
            retrying("create state table " + table.name(), () -> sql.update(table.createStatement(), Map.of()));
            requireColumns(table);
            if (!table.clusterBy().isEmpty()) {
                cluster(table);
            }
        }
        log.info("State tables ready: controlTable={} fileTable={}", controlTable.name(), fileTable.name());
    }

    private void requireColumns(StateTable table) {
        Set<String> actual = retrying("read columns of state table " + table.name(),
                () -> sql.query("SHOW COLUMNS IN " + table.name(), Map.of())).stream()
                .map(row -> ((String) row.get("col_name")).toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> expected = new TreeSet<>(table.columnNames());
        if (!actual.equals(expected)) {
            throw new IllegalStateException("State table " + table.name() + " has the columns " + actual
                    + " but this version needs " + expected + "; it was created by an earlier version. Drop it, or "
                    + "point the setting at a new table, and run again.");
        }
    }

    /**
     * Brings a table created before its clustering was defined to that clustering. A table already clustered on
     * exactly those columns is left alone: altering it would be a Delta metadata commit, which conflicts with
     * parallel jobs' merges. Clustering only speeds the table up, so a failed alter, typically because the job's
     * identity does not own the table, is tried once and leaves the table as it is.
     */
    private void cluster(StateTable table) {
        Object current = retrying("read clustering of state table " + table.name(),
                () -> sql.query("DESCRIBE DETAIL " + table.name(), Map.of())).getFirst().get("clusteringColumns");
        if (table.clusterBy().equals(current)) {
            return;
        }
        String alter = "ALTER TABLE " + table.name() + " CLUSTER BY (" + String.join(", ", table.clusterBy()) + ")";
        try {
            sql.update(alter, Map.of());
        } catch (SqlExecutionException e) {
            log.warn("Cannot cluster state table, continuing unclustered; to cluster it, run as its owner: {}"
                            + " table={} clusterBy={} currentClusterBy={} reason={}", alter, table.name(),
                    table.clusterBy(), current, e.getMessage());
            return;
        }
        log.info("State table clustered: table={} clusterBy={} previousClusterBy={}", table.name(),
                table.clusterBy(), current);
    }

    // ---- control table ----------------------------------------------------------------------------------------

    /**
     * Every control row of the target dataset: none before its first run, otherwise one. Two rows only exist
     * while two first runs race to insert the row.
     */
    public List<ControlRecord> findControls(String targetDatasetId) {
        return retrying("read control row", () -> sql.query("SELECT " + String.join(", ", controlTable.columnNames())
                + " FROM " + controlTable.name() + " WHERE target_dataset_id = :target",
                Map.of("target", targetDatasetId))).stream().map(MigrationStateRepository::control).toList();
    }

    /** Inserts the control row of a migration that has none yet; does nothing when one exists. */
    public void insertControl(ControlRecord control) {
        merge("insert control row", controlTable, List.of(controlRow(control)),
                "t.target_dataset_id = s.target_dataset_id", "WHEN NOT MATCHED THEN INSERT *");
    }

    /** Replaces the control row with {@code replacement} if it is still exactly as {@code expected}. */
    public void replaceControl(ControlRecord expected, ControlRecord replacement) {
        Map<String, Object> where = new LinkedHashMap<>();
        where.put("run_id", expected.runId());
        where.put("status", expected.status().name());
        where.put("heartbeat_at", timestamp(expected.heartbeatAt()));
        updateControl("replace control row of run " + expected.runId(), expected.targetDatasetId(),
                controlRow(replacement), where);
    }

    /** Deletes the control row of the run, which lost the race to insert the migration's row. */
    public void deleteControl(String targetDatasetId, String runId) {
        retrying("delete control row of run " + runId, () -> sql.update("DELETE FROM " + controlTable.name()
                + " WHERE target_dataset_id = :target AND run_id = :run_id",
                Map.of("target", targetDatasetId, "run_id", runId)));
    }

    /**
     * Renews the heartbeat of the run and writes its progress, if the run still owns the control row.
     *
     * @return rows updated when the engine reports it, otherwise -1; 0 means the run no longer owns the row
     */
    public long renewControl(String targetDatasetId, String runId, Instant heartbeatAt,
                             ControlRecord.Progress progress) {
        Map<String, Object> set = progressRow(progress);
        set.put("heartbeat_at", timestamp(heartbeatAt));
        return updateControl("renew control row of run " + runId, targetDatasetId, set, Map.of("run_id", runId));
    }

    /**
     * Records how the run ended, if it still owns the control row.
     *
     * @return rows updated when the engine reports it, otherwise -1; 0 means the run no longer owns the row
     */
    public long finishControl(String targetDatasetId, String runId, ControlRecord.Status status, Instant finishedAt,
                              @Nullable String error, ControlRecord.Progress progress) {
        Map<String, Object> set = progressRow(progress);
        set.put("status", status.name());
        set.put("heartbeat_at", timestamp(finishedAt));
        set.put("finished_at", timestamp(finishedAt));
        set.put("error", truncate(error));
        return updateControl("finish control row of run " + runId, targetDatasetId, set, Map.of("run_id", runId));
    }

    /**
     * {@code UPDATE}s the target dataset's control row where it still holds the {@code where} values. Values are
     * bound as parameters and cast to their column's type, so a {@code null} is typed too.
     */
    private long updateControl(String operation, String targetDatasetId, Map<String, ?> set, Map<String, ?> where) {
        Map<String, Object> params = new HashMap<>();
        List<String> assignments = new ArrayList<>();
        set.forEach((column, value) -> {
            assignments.add(column + " = CAST(:set_" + column + " AS " + controlTable.columnType(column) + ")");
            params.put("set_" + column, value);
        });
        List<String> conditions = new ArrayList<>(List.of("target_dataset_id = :target"));
        params.put("target", targetDatasetId);
        where.forEach((column, value) -> {
            conditions.add(column + " = CAST(:was_" + column + " AS " + controlTable.columnType(column) + ")");
            params.put("was_" + column, value);
        });
        String statement = "UPDATE " + controlTable.name() + " SET " + String.join(", ", assignments) + " WHERE "
                + String.join(" AND ", conditions);
        return retrying(operation, () -> sql.update(statement, params));
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
        return retrying("read files of run " + runId, () -> sql.query("SELECT "
                + String.join(", ", fileTable.columnNames()) + " FROM " + fileTable.name()
                + " WHERE run_id = :run_id", Map.of("run_id", runId))).stream()
                .map(MigrationStateRepository::file).toList();
    }

    /**
     * Deletes the file rows of a run whose output was deleted or replaced.
     *
     * @return rows deleted when the engine reports it, otherwise -1
     */
    public long deleteFiles(String runId) {
        return retrying("delete file rows of run " + runId, () -> sql.update("DELETE FROM " + fileTable.name()
                + " WHERE run_id = :run_id", Map.of("run_id", runId)));
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
        row.put("target_dataset_id", control.targetDatasetId());
        row.put("source_dataset_id", control.sourceDatasetId());
        row.put("source_location", control.sourceLocation());
        row.put("target_location", control.targetLocation());
        row.put("run_id", control.runId());
        row.put("status", control.status().name());
        row.put("snapshot_id", control.snapshotId());
        row.put("started_at", timestamp(control.startedAt()));
        row.put("heartbeat_at", timestamp(control.heartbeatAt()));
        row.put("finished_at", timestamp(control.finishedAt()));
        row.put("total_files", control.totalFiles());
        row.put("total_records", control.totalRecords());
        row.put("error", truncate(control.error()));
        row.putAll(progressRow(control.progress()));
        return row;
    }

    private static Map<String, Object> progressRow(ControlRecord.Progress progress) {
        Map<String, Object> row = new HashMap<>();
        row.put("completed_files", progress.completedFiles());
        row.put("failed_files", progress.failedFiles());
        row.put("records_read", progress.recordsRead());
        row.put("records_written", progress.recordsWritten());
        row.put("bytes_written", progress.bytesWritten());
        return row;
    }

    private static ControlRecord control(Map<String, Object> row) {
        return new ControlRecord((String) row.get("target_dataset_id"), (String) row.get("source_dataset_id"),
                (String) row.get("source_location"), (String) row.get("target_location"),
                (String) row.get("run_id"), ControlRecord.Status.valueOf((String) row.get("status")),
                nullableLong(row.get("snapshot_id")), instant(row.get("started_at")), instant(row.get("heartbeat_at")),
                instant(row.get("finished_at")), number(row.get("total_files")), number(row.get("total_records")),
                new ControlRecord.Progress(number(row.get("completed_files")), number(row.get("failed_files")),
                        number(row.get("records_read")), number(row.get("records_written")),
                        number(row.get("bytes_written"))),
                (String) row.get("error"));
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
