-- Migration state tables for Databricks SQL (Delta). ${run_table} and ${file_table} are replaced with
-- migration.state.run-table and migration.state.file-table. Run by the app when migration.state.create-tables=true,
-- or reviewed and applied by hand.

CREATE TABLE IF NOT EXISTS ${run_table} (
    run_id                 STRING    NOT NULL COMMENT 'Run identifier, e.g. 20261117-103000-ab12cd',
    status                 STRING    NOT NULL COMMENT 'RUNNING, SUCCEEDED, FAILED or SUPERSEDED',
    source_dataset_id      STRING    NOT NULL,
    destination_dataset_id STRING    NOT NULL,
    source_table_location  STRING    NOT NULL,
    destination_location   STRING    NOT NULL,
    snapshot_id            BIGINT             COMMENT 'Pinned source snapshot; NULL when the table was never written',
    source_schema_json     STRING    NOT NULL COMMENT 'Iceberg schema the source rows are read into',
    output_schema_json     STRING    NOT NULL COMMENT 'Iceberg schema of every destination file',
    total_partitions       BIGINT    NOT NULL,
    total_files            BIGINT    NOT NULL,
    total_records          BIGINT    NOT NULL COMMENT 'Physical source rows before deletes',
    started_at             TIMESTAMP NOT NULL,
    finished_at            TIMESTAMP,
    error                  STRING,
    updated_at             TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'One row per Iceberg-to-ADLS migration run';

CREATE TABLE IF NOT EXISTS ${file_table} (
    run_id           STRING    NOT NULL,
    file_key         STRING    NOT NULL COMMENT 'SHA-256 of source_path',
    source_path      STRING    NOT NULL,
    spec_id          INT       NOT NULL,
    partition_path   STRING    NOT NULL COMMENT 'Hive-style source partition; empty when unpartitioned',
    destination_path STRING    NOT NULL COMMENT 'Blob name within the destination container',
    physical_records BIGINT    NOT NULL COMMENT 'Source rows before deletes',
    status           STRING    NOT NULL COMMENT 'PENDING, IN_PROGRESS, COMPLETED or FAILED',
    records_written  BIGINT,
    records_deleted  BIGINT,
    attempts         INT       NOT NULL,
    last_error       STRING,
    updated_at       TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'One row per source data file of a migration run';
