package com.tds.pde.migration.state;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

/**
 * A state table's definition: the single source of its columns, for the {@code CREATE TABLE} statement and for the
 * Spark schema of the rows merged into it.
 *
 * <p>Deletion vectors and row tracking give Delta row-level concurrency, so parallel jobs merging different rows
 * of the same table do not conflict. The file table grows with every migration, so it is clustered (liquid
 * clustering) by the column its reads and merges select on, {@code run_id}: a merge or read then touches the files
 * holding that run's rows, not the whole table.
 *
 * @param clusterBy liquid clustering columns; empty for none
 */
record StateTable(String name, String comment, List<Column> columns, List<String> clusterBy) {

    record Column(String name, String type, boolean nullable, @Nullable String comment) {
    }

    static StateTable control(String name) {
        return new StateTable(name, "One row per table migration, keyed by target dataset: its latest run, the "
                + "claim that keeps two runs from writing the target at once, and the run's progress",
                List.of(
                        required("target_dataset_id", "STRING", "The table migration key"),
                        required("source_dataset_id", "STRING"),
                        required("source_location", "STRING", "Iceberg table location"),
                        required("target_location", "STRING", "abfss:// folder the Parquet files are written to"),
                        required("run_id", "STRING", "Latest run, e.g. 20261117-103000-ab12cd; its files are in "
                                + "the file table"),
                        required("status", "STRING", "RUNNING, COMPLETED or FAILED"),
                        optional("snapshot_id", "BIGINT", "Source snapshot, NULL when the table was never written"),
                        required("started_at", "TIMESTAMP"),
                        required("heartbeat_at", "TIMESTAMP", "Renewed while the run is live; a RUNNING row whose "
                                + "heartbeat is older than the heartbeat timeout is a failed run"),
                        optional("finished_at", "TIMESTAMP"),
                        required("total_files", "BIGINT", "Source data files at the snapshot"),
                        required("completed_files", "BIGINT", "Files published"),
                        required("failed_files", "BIGINT"),
                        required("total_records", "BIGINT", "Physical source rows before deletes"),
                        required("records_read", "BIGINT", "Physical source rows of the published files"),
                        required("records_written", "BIGINT", "Live rows published; records_read minus this were "
                                + "deleted"),
                        required("bytes_written", "BIGINT", "Size of the published target files"),
                        optional("error", "STRING", "Why the run failed")),
                List.of());
    }

    static StateTable file(String name) {
        return new StateTable(name, "One row per source data file of a migration run", List.of(
                required("run_id", "STRING"),
                required("source_path", "STRING"),
                required("target_path", "STRING", "Blob name within the target container"),
                required("physical_records", "BIGINT", "Source rows before deletes"),
                required("status", "STRING", "PENDING, COMPLETED or FAILED"),
                optional("records_written", "BIGINT", "Live rows published; physical_records minus this were deleted"),
                optional("error", "STRING", "Why the file last failed")),
                List.of("run_id"));
    }

    List<String> columnNames() {
        return columns.stream().map(Column::name).toList();
    }

    String columnType(String columnName) {
        return columns.stream().filter(column -> column.name().equals(columnName)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No column " + columnName + " in " + name))
                .type();
    }

    String createStatement() {
        String columnDefinitions = columns.stream()
                .map(column -> column.name() + " " + column.type() + (column.nullable() ? "" : " NOT NULL")
                        + (column.comment() == null ? "" : " COMMENT " + quote(column.comment())))
                .collect(Collectors.joining(",\n    ", "(\n    ", "\n)"));
        String clustering = clusterBy.isEmpty() ? "" : " CLUSTER BY (" + String.join(", ", clusterBy) + ")";
        return "CREATE TABLE IF NOT EXISTS " + name + " " + columnDefinitions + " USING DELTA" + clustering
                + " COMMENT " + quote(comment) + " TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',"
                + " 'delta.enableRowTracking' = 'true')";
    }

    /**
     * The Spark schema, as DDL, of rows holding the given subset of this table's columns, in table order. Every
     * field is nullable: the table's own constraints are enforced when the rows are merged.
     */
    String rowSchema(Map<String, ?> row) {
        if (!columnNames().containsAll(row.keySet())) {
            throw new IllegalArgumentException("Columns " + row.keySet() + " are not all in " + name);
        }
        return columns.stream()
                .filter(column -> row.containsKey(column.name()))
                .map(column -> column.name() + " " + column.type())
                .collect(Collectors.joining(", "));
    }

    private static Column required(String name, String type) {
        return new Column(name, type, false, null);
    }

    private static Column required(String name, String type, String comment) {
        return new Column(name, type, false, comment);
    }

    private static Column optional(String name, String type) {
        return new Column(name, type, true, null);
    }

    private static Column optional(String name, String type, String comment) {
        return new Column(name, type, true, comment);
    }

    private static String quote(String text) {
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }
}
