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
 * of the same table do not conflict.
 */
record StateTable(String name, String comment, List<Column> columns) {

    record Column(String name, String type, boolean nullable, @Nullable String comment) {
    }

    static StateTable control(String name) {
        return new StateTable(name, "One row per table migration (source dataset to target dataset)",
                List.of(
                        required("source_dataset_id", "STRING", "With target_dataset_id, the table migration key"),
                        required("target_dataset_id", "STRING"),
                        required("status", "STRING", "Status of the latest run: RUNNING, SUCCEEDED or FAILED"),
                        required("last_run_id", "STRING", "Latest run, in the run table"),
                        optional("snapshot_id", "BIGINT", "Pinned snapshot, NULL when the table was never written"),
                        required("total_files", "BIGINT", "Source data files at the snapshot"),
                        required("total_records", "BIGINT", "Physical source rows before deletes")));
    }

    static StateTable run(String name) {
        return new StateTable(name, "One row per migration run: what a resume must reproduce, and its outcome",
                List.of(
                        required("run_id", "STRING", "Run identifier, e.g. 20261117-103000-ab12cd"),
                        required("status", "STRING", "RUNNING, SUCCEEDED, FAILED or SUPERSEDED"),
                        required("source_dataset_id", "STRING", "The table migration, in the control table"),
                        required("target_dataset_id", "STRING"),
                        required("source_location", "STRING", "Iceberg table location"),
                        required("target_location", "STRING", "abfss:// folder the Parquet files are written to"),
                        optional("snapshot_id", "BIGINT", "Pinned snapshot, NULL when the table was never written"),
                        required("source_schema_json", "STRING", "Iceberg schema the source rows are read into"),
                        required("output_schema_json", "STRING", "Iceberg schema of every target file"),
                        required("started_at", "TIMESTAMP"),
                        optional("finished_at", "TIMESTAMP"),
                        optional("error", "STRING")));
    }

    static StateTable file(String name) {
        return new StateTable(name, "One row per source data file of a migration run", List.of(
                required("run_id", "STRING"),
                required("source_path", "STRING"),
                required("target_path", "STRING", "Blob name within the target container"),
                required("physical_records", "BIGINT", "Source rows before deletes"),
                required("status", "STRING", "PENDING, COMPLETED or FAILED"),
                optional("records_written", "BIGINT", "Live rows published; physical_records minus this were deleted"),
                optional("error", "STRING", "Why the file last failed")));
    }

    List<String> columnNames() {
        return columns.stream().map(Column::name).toList();
    }

    String createStatement() {
        String columnDefinitions = columns.stream()
                .map(column -> column.name() + " " + column.type() + (column.nullable() ? "" : " NOT NULL")
                        + (column.comment() == null ? "" : " COMMENT " + quote(column.comment())))
                .collect(Collectors.joining(",\n    ", "(\n    ", "\n)"));
        return "CREATE TABLE IF NOT EXISTS " + name + " " + columnDefinitions + " USING DELTA COMMENT "
                + quote(comment) + " TBLPROPERTIES ('delta.enableDeletionVectors' = 'true',"
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
