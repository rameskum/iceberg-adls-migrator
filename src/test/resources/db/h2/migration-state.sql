-- H2 equivalent of db/databricks/migration-state.sql for tests.

CREATE TABLE IF NOT EXISTS ${run_table} (
    run_id                 VARCHAR(64)              NOT NULL PRIMARY KEY,
    status                 VARCHAR(16)              NOT NULL,
    source_dataset_id      VARCHAR(256)             NOT NULL,
    destination_dataset_id VARCHAR(256)             NOT NULL,
    source_table_location  VARCHAR(4000)            NOT NULL,
    destination_location   VARCHAR(4000)            NOT NULL,
    snapshot_id            BIGINT,
    source_schema_json     CHARACTER LARGE OBJECT   NOT NULL,
    output_schema_json     CHARACTER LARGE OBJECT   NOT NULL,
    total_partitions       BIGINT                   NOT NULL,
    total_files            BIGINT                   NOT NULL,
    total_records          BIGINT                   NOT NULL,
    started_at             TIMESTAMP(6)             NOT NULL,
    finished_at            TIMESTAMP(6),
    error                  VARCHAR(4000),
    updated_at             TIMESTAMP(6)             NOT NULL
);

CREATE TABLE IF NOT EXISTS ${file_table} (
    run_id           VARCHAR(64)   NOT NULL,
    file_key         VARCHAR(64)   NOT NULL,
    source_path      VARCHAR(4000) NOT NULL,
    spec_id          INT           NOT NULL,
    partition_path   VARCHAR(4000) NOT NULL,
    destination_path VARCHAR(1024) NOT NULL,
    physical_records BIGINT        NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    records_written  BIGINT,
    records_deleted  BIGINT,
    attempts         INT           NOT NULL,
    last_error       VARCHAR(4000),
    updated_at       TIMESTAMP(6)  NOT NULL,
    PRIMARY KEY (run_id, file_key)
);
