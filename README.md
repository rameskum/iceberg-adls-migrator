# iceberg-adls-migrator

Copies an Apache Iceberg table stored on HDFS into standalone Parquet files on Azure Blob Storage (ADLS Gen2
account without hierarchical namespace). The target is plain Parquet: no Iceberg metadata is created.

- One source data file becomes exactly one target Parquet file.
- The output holds the **logical** table data: position deletes, equality deletes and deletion vectors are applied.
- Several files are copied at the same time; within a file, rows are streamed one at a time, so memory stays
  bounded however large the table is.
- A file becomes visible at its target only once it is complete and verified, so a failed or interrupted
  write can never be mistaken for a finished file.
- Every table migration, run and file is tracked in Delta tables written through the cluster's own Spark session,
  and runs can be resumed.

It runs on Databricks (DBR 18, Java 21) through the team's isolated launcher, and reads HDFS with the plain
Iceberg and Hadoop Java clients, not with Spark.

## How it works

```
Databricks job ──app_args──▶ team launcher (isolated class loader) ──▶ MigrationApplication.main
  1. Look up the source and target datasets (dataset API; a JSON stub for now)
  2. Take the target's run lock (a renewing blob lease)
  3. Choose the run: resume a given run, auto-resume an unfinished one, clean and start over, or start new
  4. Pin a snapshot and plan its data files from Iceberg metadata (one task per file, with its delete files)
  5. For each partition, for each file:
       read live rows (deletes applied) ─▶ Iceberg Parquet writer ─▶ staged blocks (invisible)
       close the writer, verify row counts, commit the block list with metadata (atomic) ─▶ record COMPLETED
  6. Record the run as SUCCEEDED, or FAILED and throw MigrationFailedException
```

| Component | Responsibility |
|---|---|
| `MigrationApplication` | Entry point; closes the Spring context and rethrows failures; never calls `System.exit` |
| `MigrationOrchestrator` | Run lifecycle: datasets, lock, run choice, partition loop, final status and summary |
| `RunStarter` | New run, resume, auto-resume or clean; pins snapshot, schemas and file list in the state store |
| `IcebergMetadataReader` | Loads the table by path, pins the snapshot, plans files, groups them by partition, validates |
| `TargetSchemaValidator` | Checks the target schema against the Iceberg schema; decides string conversions |
| `FileRunner` | Copies up to `parallel-files` files at once: lock check, copy, count, record the outcome; failure policy |
| `FileOutcomeRecorder` | Writes file outcomes to the file table in batches |
| `DataFileProcessor` | Copies one source file to one target file; independent of runs, progress and state |
| `IcebergDataReader` | Streams the live rows of one data file, the way Iceberg's generic reader does |
| `ParquetDataWriter` | Writes Parquet with Iceberg's writer (exact type mapping), including empty files |
| `AzureBlobStore` | Staged block upload and atomic commit, metadata, listing, deletes, lease lock |
| `MigrationStateRepository` | Control, run and file tables, with retries |
| `StateTable` | Each state table's columns: the source of its DDL and of the schema of rows merged into it |
| `SparkSqlExecutor` | Runs the state SQL through the cluster's Spark session; turns rows into a typed temporary view to merge from |
| `MigrationProgressTracker` | Record, file and partition progress, committed and in flight |

## Build

Requires JDK 21. Maven is provided by the wrapper.

```bash
./mvnw package
```

This builds and tests everything and produces `target/iceberg-adls-migrator-<version>-dist.zip`, which contains
the thin application jar and every runtime dependency in one flat `lib/` folder. The build fails if any class
exists in two jars, because the launcher loads the jars in no particular order.

## Deploy on Databricks

**Cluster:** DBR 18 (JDK 21), classic compute in *Dedicated* access mode, with network access to the HDFS
NameNode and DataNodes and to the storage account.

**Files:** unzip the dist into the folder the launcher reads as `libDir`.

**Launcher change.** The launcher currently calls `main` with an empty array. Pass the `app_args` job parameter
through instead; a missing widget keeps today's behaviour for other apps:

```scala
val args = scala.util.Try(dbutils.widgets.get("app_args")).toOption
  .filter(_.trim.nonEmpty).map(_.trim.split("\\s+")).getOrElse(Array.empty[String])
mainMethod.invoke(null, args)
```

A failed migration throws `MigrationFailedException` out of `main`. Through `Method.invoke` it arrives as the
cause of an `InvocationTargetException`, which fails the task.

**Job parameters:**

| Parameter | Value |
|---|---|
| `main_class_name` | `com.tds.pde.migration.MigrationApplication` |
| `app_args` | The per-run inputs and the environment config, space-separated (see below) |

For example:

```text
--spring.config.additional-location=/Volumes/<catalog>/<schema>/<volume>/iceberg-adls-migrator/application.yaml
--migration.source.dataset-id=0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01
--migration.target.dataset-id=5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02
--migration.run-id=20261117-103000-ab12cd
```

Any configuration property can be set this way for one run, e.g. `--migration.failure.on-file-failure=CONTINUE`.
Parameters are checked strictly before anything runs: dataset ids must be UUIDs, an unknown `--migration.*` name
(such as a misspelled `--migration.run-ld`) fails startup, and so does any parameter not starting with
`--migration.`, `--spring.` or `--logging.`.

**Secrets**, if any, come from environment variables set on the cluster with secret references
(`{{secrets/<scope>/<key>}}`), never from the YAML file. The state tables need none: they are written through the
cluster's Spark session as the job's identity.

**Before the first run:**
- create the target container (the app never creates it);
- grant the identity *Storage Blob Data Contributor* on it;
- grant the job's identity `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, and `SELECT` and `MODIFY` on the schema
  holding the state tables. The app creates them at startup from their definitions in
  [`StateTable`](src/main/java/com/tds/pde/migration/state/StateTable.java).

## Configuration

Defaults live in `src/main/resources/application.yaml`; environment values go in the file passed with
`--spring.config.additional-location`. [`config/application-example.yaml`](config/application-example.yaml) is a
complete example. Invalid values stop the app before any data is touched.

| Property | Default | Meaning |
|---|---|---|
| `migration.run-id` | empty | Resume this run. Empty: auto-resume an unfinished run, or start a new one |
| `migration.source.dataset-id` | required | Source dataset (format `ICEBERG`, location = table path on HDFS) |
| `migration.source.snapshot-id` | current | Migrate this snapshot instead of the current one |
| `migration.source.hadoop."[key]"` | none | Extra Hadoop settings, e.g. `"[fs.defaultFS]": hdfs://nn:8020`; quote dotted keys |
| `migration.target.dataset-id` | required | Target dataset (format `PARQUET`, location = `abfss://...`) |
| `migration.target.clean` | `false` | Delete everything under the target and start a fresh run |
| `migration.target.block-size` | `8MB` | Size of each staged block; also the upload memory per file |
| `migration.datasets.stub-directory` | required | Folder of `<dataset-id>.json` descriptors |
| `migration.azure.auth.mode` | `MANAGED_IDENTITY` | Or `CONNECTION_STRING` (local runs and tests only) |
| `migration.azure.auth.client-id` | empty | Client id of a user-assigned managed identity |
| `migration.azure.auth.connection-string` | empty | Required for `CONNECTION_STRING` mode |
| `migration.parquet.row-group-size` | `128MB` | Parquet row group size; the main writer memory |
| `migration.parquet.page-size` | `1MB` | Parquet page size |
| `migration.parquet.compression` | `ZSTD` | `ZSTD`, `SNAPPY`, `GZIP` or `UNCOMPRESSED` |
| `migration.progress.log-every-records` | `100000` | Log record progress at least this often (in records) |
| `migration.progress.log-every` | `30s` | ...and at least this often (in time) |
| `migration.processing.parallel-files` | `4` | Files copied at the same time (1 to 64); see memory below |
| `migration.failure.on-file-failure` | `FAIL_FAST` | Or `CONTINUE` with the remaining files |
| `migration.state.control-table` | required | e.g. `main.migration.migration_control` |
| `migration.state.run-table` | required | e.g. `main.migration.migration_run` |
| `migration.state.file-table` | required | e.g. `main.migration.migration_file` |
| `migration.state.write-retries` | `5` | Attempts per state table call before the run stops |
| `migration.state.outcome-batch-size` | `50` | File outcomes written to the file table in one commit |
| `migration.state.outcome-flush-interval` | `30s` | The first outcome after this long since the last write is written at once, with any waiting |

**Memory and parallel files.** Each file being copied holds about one source row group (as written in the
source table), one target row group (`migration.parquet.row-group-size`) and one upload block
(`migration.target.block-size`). With the defaults that is up to about 300 MB per file for large source files, so
`parallel-files: 4` needs roughly 1.2 GB of driver heap at peak; small files need far less. More parallel files help
until HDFS, the network or Azure is saturated.

**Authentication.** The `TokenCredential` bean in `AzureConfig` is the single place to plug in a different
sign-in mechanism. The default is the cluster's managed identity.

**Dataset descriptors.** Until the dataset API is wired in, `StubDatasetClient` serves recorded responses:

```json
{
  "datasetId": "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02",
  "format": "PARQUET",
  "location": "abfss://data@salesacct.dfs.core.windows.net/sales/orders",
  "schema": { "fields": [
    { "name": "order_id", "type": "bigint", "nullable": false },
    { "name": "amount", "type": "decimal(18,2)" }
  ] }
}
```

Types are Spark SQL type strings. `schema` is optional; for the source it is ignored, because the Iceberg metadata
is authoritative.

## State tables

Three Delta tables, shared by every migration job and created at startup with `CREATE TABLE IF NOT EXISTS`. Each
fact is stored once; the rest is a join away.

| Table | One row per | Columns |
|---|---|---|
| control (`migration_control`) | table migration, keyed by `source_dataset_id` and `target_dataset_id` | `status` and `last_run_id` of the latest run; the source table at its pinned snapshot: `snapshot_id`, `total_files`, `total_records` |
| run (`migration_run`) | run | `run_id`, `status` (`RUNNING`, `SUCCEEDED`, `FAILED`, `SUPERSEDED`), the migration's `source_dataset_id` and `target_dataset_id`, the resolved `source_location` and `target_location`, and what a resume must reproduce: `snapshot_id`, `source_schema_json`, `output_schema_json`; `started_at`, `finished_at`, `error` |
| file (`migration_file`) | source data file of a run | `run_id`, `source_path`, `target_path`, `physical_records`, `status` (`PENDING`, `COMPLETED`, `FAILED`), `records_written` (the other physical records were deleted), `error` |

A run belongs to the migration with its dataset pair, and the control row points at the latest run. The control row
is written when a run starts and when it ends; progress while a run is going comes from its file rows, which are
updated as each file completes:

```sql
SELECT c.source_dataset_id, c.target_dataset_id, c.status, c.last_run_id, c.snapshot_id,
       count_if(f.status = 'COMPLETED') AS completed_files, count_if(f.status = 'FAILED') AS failed_files,
       c.total_files, coalesce(sum(f.records_written), 0) AS records_written, c.total_records,
       r.started_at, r.finished_at, r.error
FROM main.migration.migration_control c
JOIN main.migration.migration_run r ON r.run_id = c.last_run_id
LEFT JOIN main.migration.migration_file f ON f.run_id = r.run_id
GROUP BY ALL;
```

Every run of one migration, oldest first:

```sql
SELECT * FROM main.migration.migration_run
WHERE source_dataset_id = :source AND target_dataset_id = :target
ORDER BY started_at;
```

**Writes.** Every write is a `MERGE INTO`. The rows to write become a DataFrame typed by the table's `StateTable`
definition, registered as a temporary view and merged on the table's key:

| Write | Merge |
|---|---|
| Control row | `WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *` |
| New run, its file rows | `WHEN NOT MATCHED THEN INSERT *`, so a retry after a commit that did land adds nothing |
| Run status, file outcome, superseding runs | `WHEN MATCHED THEN UPDATE SET` the changed columns; a run or file outcome must match exactly one row |

The statements run through the cluster's `SparkSession`, reached by reflection because the launcher's isolated class
loader hides the runtime's classes; each call switches the thread's context class loader to Spark's, because Spark and
Hadoop load their own classes through it. Every statement is a Delta commit, so a file costs one write: its outcome.
Parallel jobs migrating *different* tables share the tables safely: deletion vectors and row tracking give row-level
concurrency, and the rare conflicting commit is retried with jittered backoff.

## Runs and resume

What happens depends on `migration.run-id` and `migration.target.clean`:

| `run-id` | `clean` | Behaviour |
|---|---|---|
| set | `false` | Resume that run. It must copy the same source to the same target and not be superseded |
| empty | `false` | Resume the latest RUNNING or FAILED run for this source and target; otherwise start a new run, which requires an empty target |
| empty | `true` | Delete everything under the target prefix (refused at a container root), mark earlier runs `SUPERSEDED`, start a new run |
| set | `true` | Refused |

So a Databricks task retry or *Repair run*, which re-sends the same parameters, simply continues where the run
stopped.

**A run pins** its snapshot, read schema, output schema and file list. A resume plans the pinned snapshot again
and refuses to continue if any of these no longer match (for example, the target schema was edited, or
snapshot expiry removed a file).

**What counts as done.** Every target blob is committed together with its metadata (`migration_run_id`,
`source_file`, `source_snapshot_id`, `physical_records`, `records_written`). On resume, a blob carrying this run's
markers is a finished file even if recording it in the state store failed; it is recorded and not written again.

**One run per target.** A run holds a 60-second blob lease on `<prefix>/_migration/lock`, renewed every
20 seconds. A second run fails at once. If a run dies, its lease lapses within a minute. The lease is checked before
each file and again immediately before each file is published, so a run that loses it publishes nothing more and
stops without touching the run's state, which then belongs to whichever run took the lease over. Setting the
Databricks job's *Maximum concurrent runs* to 1 adds a second guard.

## Progress reporting

Records, files and partitions are tracked independently, with `percentage = total == 0 ? 100.0 : completed * 100.0 / total`:

```text
Migration overview: runId=20261117-204240-0fef7d resumed=false table=hdfs://nn/warehouse/orders snapshotId=825090155793393105
  Total partitions: 3
  Total data files: 4
  Total records: 1000
  Delete files: 0
  Target: abfss://data@salesacct.dfs.core.windows.net/sales/orders
Partition processing started: partition=region=EU specId=0 files=2 physicalRecords=500
Source file processing started: source=hdfs://... target=sales/orders/region=EU__... physicalRecords=250 deleteFiles=0
Record progress: 500/1000 (50%) committed=250 inFlight=250 written=500
Source file completed: source=hdfs://... target=... recordsWritten=250 recordsDeleted=0 durationMs=61
File progress: 2/4 (50%)
Partition completed: partition=region=EU files=2
Partition progress: 2/3 (66.6667%)
Migration completed: runId=20261117-204240-0fef7d
  Records: 1000/1000 (100%) written=1000 deleted=0
  Files: 4/4 (100%) failed=0
  Partitions: 3/3 (100%)
```

- **Record totals are physical rows** (before deletes), the only total Iceberg metadata gives exactly without
  scanning the table. Rows removed by deletes count as processed and are reported as `deleted`, so a completed
  run always reaches 100%.
- **Committed and in flight.** Rows of the files being written are reported as `inFlight`, summed over the files
  copied in parallel; they become `committed` only when their file is published, and are dropped if it fails.
  Files count when published; partitions count when all their files are. With files copied in parallel, the log
  lines of different files interleave; the thread name (`file-worker-N`) tells them apart.
- **The file table trails the log slightly:** outcomes are written in batches (see
  `migration.state.outcome-batch-size`), so a finished file can show `PENDING` there until its batch is written.
- For JSON logs set `logging.structured.format.console=ecs` (or `logstash`).

## Failure handling

- **A failed file publishes nothing.** Its staged blocks are never committed; Azure discards them after 7 days.
- **Every file is verified before publishing:** all physical rows recorded in Iceberg metadata must have been read,
  and the closed Parquet file must hold exactly the rows written.
- **`FAIL_FAST`** (default) starts no more files after the first failed one; files already being copied in
  parallel finish and count. **`CONTINUE`** processes the rest. Either way the run ends `FAILED`, lists the failed
  files (source, target, reason) and throws `MigrationFailedException` with the run id to resume.
- **Checked before anything is written:** non-Parquet data files, unreadable delete files, format versions above 3,
  schema mismatches, a non-empty target for a new run, another active run.
- **State table failures** are retried with jittered backoff (about 1s, then 2s, ...); if they persist the run stops,
  and the next resume recognises published files by their blob metadata. The same covers outcomes that were still
  waiting for their batch when a run stopped or crashed: nothing published is copied twice.

## Output

- **One flat folder per target**, with the partition in the file name, for example
  `sales/orders/region=EU.event_ts_day=2026-01-15__00000-5-a1b2c3d4-0001-00001-9f3e1c2a.parquet`. Names are
  deterministic and contain a hash of the source location, so source files with the same name in different folders
  never collide. Names never start with `_` or `.`, which Spark would skip.
- **Schema.** Every file uses the table schema: the current schema when migrating the current snapshot (as a
  plain read does), or the snapshot's own schema when `snapshot-id` is set (as time travel does). Older files are
  projected onto it: added columns are null, renamed columns appear under their current names.
- **Types** are written exactly as Iceberg writes them, with Parquet field ids. Databricks cannot read Parquet
  `UUID` or `TIME` columns; the app warns about them, and converts a top-level `uuid` or `time` column to string
  only when the target schema declares it as `string`.
- A source file whose rows are all deleted still produces a valid, empty Parquet file.

## Tests

```bash
./mvnw verify
```

- **Unit tests** for configuration, schema matching, file naming, progress, state store, run choice.
- **End-to-end tests** run the whole application against real Iceberg tables: deletes of every kind, schema
  changes, empty and never-written tables, fail-fast, continue, resume, recovery of completions the state store
  missed, lock conflicts and loss, clean.
- **HDFS:** one end-to-end test reads the source from an in-process HDFS (`MiniDFSCluster`).
- **Bounded memory:** `BoundedMemoryTest` (tag `memory`) runs in its own JVM with `-Xmx128m` and migrates two
  source files, each larger than the heap, at the same time.
- **Azure:** the Blob adapter tests and one end-to-end test need a real Blob API. They run when
  `AZURE_STORAGE_TEST_CONNECTION_STRING` is set, for example against Azurite:

  ```bash
  npx azurite --inMemoryPersistence --skipApiVersionCheck --loose --blobHost 127.0.0.1 &
  export AZURE_STORAGE_TEST_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<Azurite's well-known key>;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
  ./mvnw verify
  ```

  Azurite's account key is published in its documentation.

Spark and Delta do not run in the tests: the orchestration tests keep state in an in-memory stand-in for the
repository, and the Spark bridge is tested against stand-in classes with Spark's API shape. The packaged app was run
by hand through a replica of the launcher on a JVM holding open-source Spark 4.0.1 and Delta 4.0.0, covering a new
run, a refused rerun, a clean run with a failed file, and its resume.

## Not yet verified outside tests

- Managed identity sign-in, and the state tables on Databricks' own Spark and Delta with Unity Catalog, including
  row-level concurrency between parallel jobs: these need the Databricks workspace.
- A production HDFS cluster (tests use an in-process one).
- Equality deletes are loaded into memory per data file; memory then grows with the size of the equality delete
  files, not the data files. Tables here are append-only, so none are expected.
- The best `parallel-files` for a real cluster: it depends on HDFS, network and Azure throughput, so measure it
  with the `durationMs` of each file and the run's total time.
