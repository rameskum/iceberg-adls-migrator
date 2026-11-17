-- Migration state tables: Unity Catalog Delta tables shared by every migration job. ${control_table}, ${run_table}
-- and ${file_table} are replaced with migration.state.control-table, run-table and file-table. The app runs this
-- script through Spark at every startup; every statement is CREATE TABLE IF NOT EXISTS.
-- Deletion vectors and row tracking give row-level concurrency, so parallel jobs updating different rows of the
-- same table do not conflict.

CREATE TABLE IF NOT EXISTS ${control_table} (
    source_dataset_id      STRING    NOT NULL,
    target_dataset_id STRING    NOT NULL,
    source_table_location  STRING    NOT NULL COMMENT 'Identifies the table migration, with target_location',
    target_location   STRING    NOT NULL,
    status                 STRING    NOT NULL COMMENT 'RUNNING, SUCCEEDED or FAILED',
    last_run_id            STRING    NOT NULL,
    format_version         INT       NOT NULL COMMENT 'Iceberg table format version',
    snapshot_id            BIGINT             COMMENT 'Pinned snapshot, NULL when the table was never written',
    snapshot_timestamp     TIMESTAMP,
    schema_id              INT,
    partition_specs        STRING    NOT NULL COMMENT 'Partition specs in use, e.g. 0:[region: identity(2)]',
    total_partitions       BIGINT    NOT NULL,
    total_files            BIGINT    NOT NULL,
    total_records          BIGINT    NOT NULL COMMENT 'Physical source rows before deletes',
    total_delete_files     BIGINT    NOT NULL,
    total_bytes            BIGINT    NOT NULL COMMENT 'Size of the source data files',
    completed_partitions   BIGINT    NOT NULL,
    completed_files        BIGINT    NOT NULL,
    failed_files           BIGINT    NOT NULL,
    records_written        BIGINT    NOT NULL,
    records_deleted        BIGINT    NOT NULL,
    started_at             TIMESTAMP NOT NULL,
    finished_at            TIMESTAMP,
    error                  STRING,
    updated_at             TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'One row per table migration (source table to target), with the latest run'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableRowTracking' = 'true');

CREATE TABLE IF NOT EXISTS ${run_table} (
    run_id                 STRING    NOT NULL COMMENT 'Run identifier, e.g. 20261117-103000-ab12cd',
    status                 STRING    NOT NULL COMMENT 'RUNNING, SUCCEEDED, FAILED or SUPERSEDED',
    source_dataset_id      STRING    NOT NULL,
    target_dataset_id STRING    NOT NULL,
    source_table_location  STRING    NOT NULL,
    target_location   STRING    NOT NULL,
    snapshot_id            BIGINT             COMMENT 'Pinned source snapshot, NULL when the table was never written',
    source_schema_json     STRING    NOT NULL COMMENT 'Iceberg schema the source rows are read into',
    output_schema_json     STRING    NOT NULL COMMENT 'Iceberg schema of every target file',
    total_partitions       BIGINT    NOT NULL,
    total_files            BIGINT    NOT NULL,
    total_records          BIGINT    NOT NULL COMMENT 'Physical source rows before deletes',
    started_at             TIMESTAMP NOT NULL,
    finished_at            TIMESTAMP,
    error                  STRING,
    updated_at             TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'One row per migration run'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableRowTracking' = 'true');

CREATE TABLE IF NOT EXISTS ${file_table} (
    run_id           STRING    NOT NULL,
    file_key         STRING    NOT NULL COMMENT 'SHA-256 of source_path',
    source_path      STRING    NOT NULL,
    spec_id          INT       NOT NULL,
    partition_path   STRING    NOT NULL COMMENT 'Hive-style source partition, empty when unpartitioned',
    target_path STRING    NOT NULL COMMENT 'Blob name within the target container',
    physical_records BIGINT    NOT NULL COMMENT 'Source rows before deletes',
    status           STRING    NOT NULL COMMENT 'PENDING, COMPLETED or FAILED',
    records_written  BIGINT,
    records_deleted  BIGINT,
    attempts         INT       NOT NULL,
    last_error       STRING,
    updated_at       TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'One row per source data file of a migration run'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', 'delta.enableRowTracking' = 'true');
