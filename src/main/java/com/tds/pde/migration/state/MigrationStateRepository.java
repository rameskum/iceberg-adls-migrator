package com.tds.pde.migration.state;

import com.tds.pde.migration.config.MigrationProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Run and file state in two JDBC tables (Delta tables on Databricks SQL in production).
 *
 * <p>Uses only plain {@code INSERT}, {@code UPDATE} and {@code SELECT}, so the same statements run on Databricks
 * SQL and on H2 in tests. Every call is retried before it fails with {@link StateStoreException}.
 */
@Slf4j
@Repository
public class MigrationStateRepository {

    /** Databricks SQL accepts at most 256 parameters per statement. */
    private static final int MAX_PARAMETERS = 250;
    private static final int MAX_ERROR_LENGTH = 4000;
    private static final String DDL = "db/databricks/migration-state.sql";

    private static final String RUN_COLUMNS = "run_id, status, source_dataset_id, destination_dataset_id, "
            + "source_table_location, destination_location, snapshot_id, source_schema_json, output_schema_json, "
            + "total_partitions, total_files, total_records, started_at, finished_at, error, updated_at";
    private static final String FILE_COLUMNS = "run_id, file_key, source_path, spec_id, partition_path, "
            + "destination_path, physical_records, status, records_written, records_deleted, attempts, last_error, "
            + "updated_at";
    private static final int FILE_COLUMN_COUNT = FILE_COLUMNS.split(",").length;

    private final JdbcClient jdbc;
    private final String runTable;
    private final String fileTable;
    private final int attempts;
    private final Duration initialBackoff;

    @Autowired
    public MigrationStateRepository(JdbcClient jdbc, MigrationProperties properties) {
        this(jdbc, properties, Duration.ofSeconds(1));
    }

    MigrationStateRepository(JdbcClient jdbc, MigrationProperties properties, Duration initialBackoff) {
        this.jdbc = jdbc;
        this.runTable = properties.state().runTable();
        this.fileTable = properties.state().fileTable();
        this.attempts = properties.state().writeRetries();
        this.initialBackoff = initialBackoff;
    }

    /** Runs the bundled Databricks DDL; every statement is {@code CREATE TABLE IF NOT EXISTS}. */
    public void createTablesIfMissing() {
        String script;
        try {
            script = new ClassPathResource(DDL).getContentAsString(StandardCharsets.UTF_8)
                    .replace("${run_table}", runTable)
                    .replace("${file_table}", fileTable);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + DDL, e);
        }
        Arrays.stream(script.split(";"))
                .map(statement -> statement.replaceAll("(?m)^\\s*--.*$", "").strip())
                .filter(statement -> !statement.isEmpty())
                .forEach(statement -> retrying("create state tables", () -> jdbc.sql(statement).update()));
        log.info("State tables ready: runTable={} fileTable={}", runTable, fileTable);
    }

    public void insertRun(RunRecord run) {
        retrying("insert run " + run.runId(), () -> jdbc
                .sql("INSERT INTO " + runTable + " (" + RUN_COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .params(run.runId(), run.status().name(), run.sourceDatasetId(), run.destinationDatasetId(),
                        run.sourceTableLocation(), run.destinationLocation(), bigint(run.snapshotId()),
                        run.sourceSchemaJson(), run.outputSchemaJson(), run.totalPartitions(), run.totalFiles(),
                        run.totalRecords(), timestamp(run.startedAt()), timestamp(run.finishedAt()),
                        text(run.error()), timestamp(Instant.now()))
                .update());
    }

    /** Inserts in multi-row statements, keeping round trips to a warehouse few. */
    public void insertFiles(List<FileRecord> files) {
        int rowsPerStatement = MAX_PARAMETERS / FILE_COLUMN_COUNT;
        String row = "(" + String.join(", ", java.util.Collections.nCopies(FILE_COLUMN_COUNT, "?")) + ")";
        for (int start = 0; start < files.size(); start += rowsPerStatement) {
            List<FileRecord> chunk = files.subList(start, Math.min(files.size(), start + rowsPerStatement));
            List<Object> params = new ArrayList<>(chunk.size() * FILE_COLUMN_COUNT);
            for (FileRecord file : chunk) {
                params.addAll(Arrays.asList(file.runId(), file.fileKey(), file.sourcePath(), file.specId(),
                        file.partitionPath(), file.destinationPath(), file.physicalRecords(), file.status().name(),
                        bigint(file.recordsWritten()), bigint(file.recordsDeleted()), file.attempts(), text(file.lastError()),
                        timestamp(file.updatedAt())));
            }
            String sql = "INSERT INTO " + fileTable + " (" + FILE_COLUMNS + ") VALUES "
                    + String.join(", ", java.util.Collections.nCopies(chunk.size(), row));
            retrying("insert " + chunk.size() + " file rows", () -> jdbc.sql(sql).params(params).update());
        }
    }

    public Optional<RunRecord> findRun(String runId) {
        return retrying("read run " + runId, () -> jdbc
                .sql("SELECT " + RUN_COLUMNS + " FROM " + runTable + " WHERE run_id = ?")
                .param(runId)
                .query(MigrationStateRepository::run)
                .optional());
    }

    /** The most recently started RUNNING or FAILED run copying this source to this destination. */
    public Optional<RunRecord> findLatestUnfinishedRun(String sourceTableLocation, String destinationLocation) {
        return retrying("find unfinished run", () -> jdbc
                .sql("SELECT " + RUN_COLUMNS + " FROM " + runTable
                        + " WHERE source_table_location = ? AND destination_location = ?"
                        + " AND status IN ('RUNNING', 'FAILED') ORDER BY started_at DESC LIMIT 1")
                .params(sourceTableLocation, destinationLocation)
                .query(MigrationStateRepository::run)
                .optional());
    }

    /** The most recently started run that still owns output at this destination. */
    public Optional<RunRecord> findLatestRunForDestination(String destinationLocation) {
        return retrying("find run for destination", () -> jdbc
                .sql("SELECT " + RUN_COLUMNS + " FROM " + runTable
                        + " WHERE destination_location = ? AND status <> 'SUPERSEDED' ORDER BY started_at DESC LIMIT 1")
                .param(destinationLocation)
                .query(MigrationStateRepository::run)
                .optional());
    }

    public List<FileRecord> findFiles(String runId) {
        return retrying("read files of run " + runId, () -> jdbc
                .sql("SELECT " + FILE_COLUMNS + " FROM " + fileTable + " WHERE run_id = ?")
                .param(runId)
                .query(MigrationStateRepository::file)
                .list());
    }

    public void updateRunStatus(String runId, RunRecord.Status status, @Nullable Instant finishedAt,
                                @Nullable String error) {
        updateOne("update run " + runId, "UPDATE " + runTable
                        + " SET status = ?, finished_at = ?, error = ?, updated_at = ? WHERE run_id = ?",
                status.name(), timestamp(finishedAt), text(error), timestamp(Instant.now()), runId);
    }

    /** Marks every other run that owns output at the destination as superseded by a clean start. */
    public int supersedeRuns(String destinationLocation) {
        return retrying("supersede runs", () -> jdbc
                .sql("UPDATE " + runTable + " SET status = 'SUPERSEDED', updated_at = ?"
                        + " WHERE destination_location = ? AND status <> 'SUPERSEDED'")
                .params(timestamp(Instant.now()), destinationLocation)
                .update());
    }

    public void markFileInProgress(String runId, String fileKey) {
        updateOne("mark file in progress", "UPDATE " + fileTable
                        + " SET status = 'IN_PROGRESS', attempts = attempts + 1, updated_at = ?"
                        + " WHERE run_id = ? AND file_key = ?",
                timestamp(Instant.now()), runId, fileKey);
    }

    public void markFileCompleted(String runId, String fileKey, long recordsWritten, long recordsDeleted) {
        updateOne("mark file completed", "UPDATE " + fileTable
                        + " SET status = 'COMPLETED', records_written = ?, records_deleted = ?, last_error = NULL,"
                        + " updated_at = ? WHERE run_id = ? AND file_key = ?",
                recordsWritten, recordsDeleted, timestamp(Instant.now()), runId, fileKey);
    }

    public void markFileFailed(String runId, String fileKey, String error) {
        updateOne("mark file failed", "UPDATE " + fileTable
                        + " SET status = 'FAILED', last_error = ?, updated_at = ? WHERE run_id = ? AND file_key = ?",
                text(error), timestamp(Instant.now()), runId, fileKey);
    }

    private void updateOne(String operation, String sql, Object... params) {
        int updated = retrying(operation, () -> jdbc.sql(sql).params(params).update());
        if (updated != 1) {
            throw new IllegalStateException(operation + " updated " + updated + " rows instead of 1");
        }
    }

    private <T> T retrying(String operation, Supplier<T> call) {
        Duration backoff = initialBackoff;
        for (int attempt = 1; ; attempt++) {
            try {
                return call.get();
            } catch (DataAccessException e) {
                if (attempt >= attempts) {
                    throw new StateStoreException("State store operation failed after " + attempt + " attempts: "
                            + operation, e);
                }
                log.warn("State store operation failed, retrying: operation={} attempt={}/{} retryIn={} reason={}",
                        operation, attempt, attempts, backoff, e.getMessage());
                sleep(backoff);
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

    private static RunRecord run(ResultSet rs, int row) throws SQLException {
        return new RunRecord(rs.getString("run_id"), RunRecord.Status.valueOf(rs.getString("status")),
                rs.getString("source_dataset_id"), rs.getString("destination_dataset_id"),
                rs.getString("source_table_location"), rs.getString("destination_location"),
                nullableLong(rs, "snapshot_id"), rs.getString("source_schema_json"),
                rs.getString("output_schema_json"), rs.getLong("total_partitions"), rs.getLong("total_files"),
                rs.getLong("total_records"), instant(rs.getTimestamp("started_at")),
                instant(rs.getTimestamp("finished_at")), rs.getString("error"));
    }

    private static FileRecord file(ResultSet rs, int row) throws SQLException {
        return new FileRecord(rs.getString("run_id"), rs.getString("file_key"), rs.getString("source_path"),
                rs.getInt("spec_id"), rs.getString("partition_path"), rs.getString("destination_path"),
                rs.getLong("physical_records"), FileRecord.Status.valueOf(rs.getString("status")),
                nullableLong(rs, "records_written"), nullableLong(rs, "records_deleted"), rs.getInt("attempts"),
                rs.getString("last_error"), instant(rs.getTimestamp("updated_at")));
    }

    private static @Nullable Long nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    // Nullable values carry an explicit SQL type: drivers such as Databricks' cannot infer one for NULL.
    private static SqlParameterValue timestamp(@Nullable Instant instant) {
        return new SqlParameterValue(Types.TIMESTAMP, instant == null ? null : Timestamp.from(instant));
    }

    private static SqlParameterValue bigint(@Nullable Long value) {
        return new SqlParameterValue(Types.BIGINT, value);
    }

    private static @Nullable Instant instant(@Nullable Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private static SqlParameterValue text(@Nullable String text) {
        return new SqlParameterValue(Types.VARCHAR,
                text == null || text.length() <= MAX_ERROR_LENGTH ? text : text.substring(0, MAX_ERROR_LENGTH));
    }
}
