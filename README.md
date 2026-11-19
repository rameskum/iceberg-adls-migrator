# iceberg-adls-migrator

Copies an Apache Iceberg table stored on HDFS into standalone Parquet files on Azure Blob Storage (ADLS Gen2
account without hierarchical namespace). The target is plain Parquet: no Iceberg metadata is created.

- One source data file becomes exactly one target Parquet file.
- The output holds the **logical** table data: position deletes, equality deletes and deletion vectors are applied.
- Several files are copied at the same time; within a file, rows are streamed one at a time, so memory stays
  bounded however large the table is.
- A file becomes visible at its target only once it is complete and verified, so a failed or interrupted
  write can never be mistaken for a finished file.
- Every table migration and file is tracked in Delta tables written through the cluster's own Spark session. One
  control row per migration keeps two runs from writing the same target and shows the run's progress.
- A run that does not complete is not resumed: the next run cleans the target and copies everything again, from
  the beginning, under a new run id.

It runs on Databricks (DBR 18, Java 21) through the team's isolated launcher, and reads HDFS with the plain
Iceberg and Hadoop Java clients, not with Spark.

## How it works

```
Databricks job ──app_args──▶ team launcher (isolated class loader) ──▶ MigrationApplication.main
  1. Look up the source and target datasets (dataset API; a JSON stub for now)
  2. Read the migration's control row: refuse after a run that did not complete unless clean is set,
     and refuse while that run's heartbeat is fresh
  3. Pin a snapshot and plan its data files from Iceberg metadata (one task per file, with its delete files)
  4. Claim the control row for a new run id (a conditional write, read back); from now on renew its
     heartbeat and write the progress with every Progress: line
  5. With clean, delete the target's files; record the run's files as PENDING
  6. For each partition, for each file:
       read live rows (deletes applied) ─▶ Iceberg Parquet writer ─▶ staged blocks (invisible)
       (staging overlaps with writing: one block is uploaded while the next one fills)
       close the writer, verify row counts, check the claim,
       commit the block list with metadata (atomic) ─▶ record COMPLETED
  7. Mark the control row COMPLETED, or FAILED and throw MigrationFailedException
```

| Component | Responsibility |
|---|---|
| `MigrationApplication` | Entry point; closes the Spring context and rethrows failures; never calls `System.exit` |
| `MigrationOrchestrator` | Run lifecycle: datasets, run start, heartbeat, file copying, final status and summary |
| `RunStarter` | Decides whether a run may start, plans it, claims the control row; cleans the target and records the run's files |
| `RunClaim` | The run's ownership of the control row: renews the heartbeat and progress, is checked before each target delete of a clean, each file and each publish, records how the run ended |
| `IcebergMetadataReader` | Loads the table by path, pins the snapshot, plans files, groups them by partition, validates |
| `TargetSchemaValidator` | Checks the target schema against the Iceberg schema; decides string conversions |
| `FileRunner` | Copies up to `parallel-files` files at once: claim check, copy, count, record the outcome; failure policy |
| `FileOutcomeRecorder` | Writes file outcomes to the file table in batches |
| `DataFileProcessor` | Copies one source file to one target file; independent of runs, progress and state |
| `IcebergDataReader` | Streams the live rows of one data file, the way Iceberg's generic reader does |
| `ParquetDataWriter` | Writes Parquet with Iceberg's writer (exact type mapping), including empty files |
| `AzureBlobStore` | Staged block upload (one block staged in the background while the next fills) and atomic commit, metadata, listing, deletes |
| `MigrationStateRepository` | Control and file tables, with retries |
| `StateTable` | Each state table's columns: the source of its DDL and of the schema of rows merged into it |
| `SparkSqlExecutor` | Runs the state SQL through the cluster's Spark session; turns rows into a typed temporary view to merge from |
| `MigrationProgressTracker` | Record, byte, file and partition progress, committed and in flight; logs the `Progress:` heartbeat on a timer and hands its progress to the run's claim |

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
```

Any configuration property can be set this way for one run, e.g. `--migration.failure.on-file-failure=CONTINUE`.
Parameters are checked strictly before anything runs: dataset ids must be UUIDs, an unknown `--migration.*` name
(such as a misspelled `--migration.target.claen`) fails startup, and so does any parameter not starting with
`--migration.`, `--spring.` or `--logging.`.

**Secrets**, if any, come from environment variables set on the cluster with secret references
(`{{secrets/<scope>/<key>}}`), never from the YAML file or a job parameter: job parameters show in the job's settings
and run history, so a secret one, such as `--migration.azure.auth.connection-string`, fails startup and names the
environment variable to use instead (`MIGRATION_AZURE_AUTH_CONNECTIONSTRING`). The state tables need none: they are
written through the cluster's Spark session as the job's identity.

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
| `migration.source.dataset-id` | required | Source dataset (format `ICEBERG`, location = table path on HDFS) |
| `migration.source.snapshot-id` | current | Migrate this snapshot instead of the current one |
| `migration.source.hadoop."[key]"` | none | Extra Hadoop settings, e.g. `"[fs.defaultFS]": hdfs://nn:8020`; quote dotted keys |
| `migration.target.dataset-id` | required | Target dataset (format `PARQUET`, location = `abfss://...`) |
| `migration.target.clean` | `false` | Delete everything under the target and start again from the beginning; needed after a run that did not complete, see [Runs](#runs) |
| `migration.target.block-size` | `8MB` | Size of each staged block; each file in flight holds up to two (one filling, one being uploaded) |
| `migration.datasets.stub-directory` | required | Folder of `<dataset-id>.json` descriptors |
| `migration.azure.auth.mode` | `MANAGED_IDENTITY` | Or `CONNECTION_STRING` (local runs and tests only) |
| `migration.azure.auth.client-id` | empty | Client id of a user-assigned managed identity |
| `migration.azure.auth.connection-string` | empty | Required for `CONNECTION_STRING` mode; a secret, so only from the environment variable `MIGRATION_AZURE_AUTH_CONNECTIONSTRING` |
| `migration.parquet.row-group-size` | `32MB` | Parquet row group size; the main writer memory per file in flight, see below |
| `migration.parquet.page-size` | `1MB` | Parquet page size |
| `migration.parquet.compression` | `ZSTD` | `ZSTD`, `SNAPPY`, `GZIP` or `UNCOMPRESSED` |
| `migration.progress.log-every` | `30s` | How often the `Progress:` heartbeat is logged, and the run's heartbeat and progress written to its control row |
| `migration.processing.parallel-files` | `4` | Files copied at the same time (1 to 64); see memory below |
| `migration.failure.on-file-failure` | `FAIL_FAST` | Or `CONTINUE` with the remaining files |
| `migration.state.control-table` | required | e.g. `main.migration.migration_control` |
| `migration.state.file-table` | required | e.g. `main.migration.migration_file` |
| `migration.state.write-retries` | `5` | Attempts per state table call, read or write, before the run stops; only conflicts and transient errors are retried |
| `migration.state.outcome-batch-size` | `50` | File outcomes written to the file table in one commit |
| `migration.state.outcome-flush-interval` | `30s` | The first outcome after this long since the last write is written at once, with any waiting |
| `migration.state.heartbeat-timeout` | `5m` | A RUNNING run whose heartbeat is older than this has died; until then `clean` is refused. At least 4 x `log-every` |

**Memory and parallel files.** Each file being copied holds about one source row group (as written in the
source table), one target row group (`migration.parquet.row-group-size`) and up to two upload blocks
(`migration.target.block-size`, one filling while the other is uploaded). With the defaults that is 32 MB + 16 MB
plus the source row group, so with a 128 MB source row group about 180 MB per file and `parallel-files: 4` needs
roughly 0.75 GB of driver heap at peak; small files need far less. Size the driver heap for
`parallel-files` x (source row group + `row-group-size` + 2 x `block-size`).

The default row group is 32 MB rather than the 128 MB Parquet writers often use, so that several files fit the
heap at once; Spark and Databricks read 32 MB row groups without trouble. Raise `row-group-size` if the heap
allows and readers benefit from fewer, larger row groups, or lower it (with `parallel-files`) on a small driver.
More parallel files help until HDFS, the network or Azure is saturated.

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

Two Delta tables, shared by every migration job and created at startup with `CREATE TABLE IF NOT EXISTS`. A table
that exists with other columns, such as one created by an earlier version, stops the app at startup with a message
to drop it (or point the setting at a new table); the earlier version's run table is no longer used.

| Table | One row per | Columns |
|---|---|---|
| control (`migration_control`) | table migration, keyed by `target_dataset_id` | the latest run: `run_id`, `status` (`RUNNING`, `COMPLETED`, `FAILED`), `started_at`, `heartbeat_at`, `finished_at`, `error`; what it copies: `source_dataset_id`, `source_location`, `target_location`, `snapshot_id`; its progress: `completed_files`, `failed_files` and `total_files`, `records_read`, `records_written` and `total_records`, `bytes_written` |
| file (`migration_file`) | source data file of a run | `run_id`, `source_path`, `target_path`, `physical_records`, `status` (`PENDING`, `COMPLETED`, `FAILED`), `records_written` (the other physical records were deleted), `error` |

Record counts are physical source rows (before deletes): `records_read` of the published files, of which
`records_written` were live and the rest deleted, out of `total_records`. The counters are those of the latest
`Progress:` line, so they trail the log by at most `migration.progress.log-every`; the end of the run writes the
final ones.

**Clustering.** The file table grows with every migration, so it uses liquid clustering on the column its reads
and merges select on: `CLUSTER BY (run_id)`. A file outcome merge then touches only the files holding that run's
rows, which keeps it fast and keeps parallel jobs from conflicting as the table grows. Liquid clustering reorganises
data when `OPTIMIZE` runs, which predictive optimization does for Unity Catalog managed tables; otherwise schedule
`OPTIMIZE` on the table. At startup the app reads its `clusteringColumns` from `DESCRIBE DETAIL` and, when they
differ, as for a table created before clustering existed, runs `ALTER TABLE <table> CLUSTER BY (run_id)` and logs it.
A table already clustered right is left alone, so it gets no metadata commit that could conflict with parallel jobs'
merges. Altering needs ownership of the table. When the alter fails, for example because the job's identity does not
own the table, it is not retried: the app logs a warning with the statement to run and carries on with the table
unclustered, until its owner runs `ALTER TABLE <file-table> CLUSTER BY (run_id)` once by hand.

**Retention.** The file table holds the files of each migration's latest run: a new run deletes the file rows of
the run it replaces, whose output it has just deleted, or found already gone.

Every migration, its latest run and how far it got:

```sql
SELECT target_dataset_id, source_dataset_id, run_id, status, started_at, heartbeat_at, finished_at,
       completed_files, failed_files, total_files, records_read, records_written, total_records, bytes_written,
       error
FROM main.migration.migration_control;
```

The files of a run, for example the failed ones:

```sql
SELECT source_path, target_path, error FROM main.migration.migration_file
WHERE run_id = :run_id AND status = 'FAILED';
```

**Writes.** File rows are written with `MERGE INTO`: the rows to write become a DataFrame typed by the table's
`StateTable` definition, registered as a temporary view and merged on the table's key. The control row is changed
with conditional `UPDATE`s whose values are bound as parameters:

| Write | Statement |
|---|---|
| First run of a migration claims its row | `MERGE ... WHEN NOT MATCHED THEN INSERT *` on `target_dataset_id` |
| A later run claims the row | `UPDATE` every column `WHERE` the row still holds the `run_id`, `status` and `heartbeat_at` it was read with |
| Heartbeat and progress; how the run ended | `UPDATE` those columns `WHERE run_id` is the run's own |
| New run's file rows | `WHEN NOT MATCHED THEN INSERT *`, so a retry after a commit that did land adds nothing |
| File outcomes | `WHEN MATCHED THEN UPDATE SET` the changed columns; each must match exactly one row |
| File rows of the replaced run | `DELETE ... WHERE run_id` |

The statements run through the cluster's `SparkSession`, reached by reflection because the launcher's isolated class
loader hides the runtime's classes; each call switches the thread's context class loader to Spark's, because Spark and
Hadoop load their own classes through it. Every statement is a Delta commit; file outcomes are written in batches
(`migration.state.outcome-batch-size`, 50 by default), so a file costs a fraction of a write.
Parallel jobs migrating *different* tables share the tables safely: deletion vectors and row tracking give row-level
concurrency, and the rare conflicting commit is retried with jittered backoff. A retried conditional `UPDATE` is
evaluated again against the row as it now is.

## Runs

Every run starts from the beginning under a new run id; nothing is resumed. Whether a run may start depends on the
latest run in the migration's control row and on `migration.target.clean`:

| Latest run | `clean=false` | `clean=true` |
|---|---|---|
| none, or `COMPLETED` | Start, if the target is empty; otherwise refused | Delete the target's files, start |
| `FAILED`, or `RUNNING` with a heartbeat older than `heartbeat-timeout` (it died) | Refused | Delete the target's files, start |
| `RUNNING` with a fresh heartbeat | Refused | Refused: it may still be writing |

A refusal writes nothing and names the run and what to set. Cleaning deletes everything under the target prefix,
and is refused at a container root. A Databricks task retry or *Repair run* re-sends the same parameters, so after a
failure it is refused until it is rerun with `--migration.target.clean=true`.

**Targets must not overlap.** A target that is the same folder as another migration's target, or lies within it or
contains it, is refused: a clean of either would delete the other's files. The control table holds every
migration's target location, so this is checked before the run claims its migration, and again before it cleans or
writes the target, which also catches two such runs that started at the same moment.

**One run per migration.** A run claims the control row before it writes anything: the first run of a migration
inserts the row, a later run updates it only while it is still exactly as read. Of two runs racing, Delta commits one
write and the other, retried after the conflict, no longer applies. The run then reads the row back and only goes on
when it is the migration's single row and names its run id. Row-level concurrency does not treat two inserts of a
new key as a conflict, so two first runs can both insert; only one of them can then see a single row, the other
withdraws its row, and if both see two rows both stop.

**The heartbeat.** While a run is live, every `Progress:` line (`migration.progress.log-every`, 30s) renews
`heartbeat_at` and writes the progress, if the row still names the run. A run whose row names another run id has
lost the migration, to a run that cleaned the target after its heartbeat went stale; so has a run that could not
renew its heartbeat for half of `heartbeat-timeout`, as another run may clean the target from the full timeout on.
The claim is checked before each blob a clean deletes, before each file starts and again immediately before each file
is published, so such a run deletes and publishes nothing more and stops without writing the migration's state. Setting the Databricks job's *Maximum
concurrent runs* to 1 adds a second guard.

**Publishing a file is atomic.** Every target blob is committed in one Put Block List together with its metadata
(`migration_run_id`, `source_file`, `source_partition`, `source_snapshot_id`, `physical_records`, `records_written`),
under a deterministic name, so a partial file is never visible. Each staged block is sent with its Content-MD5, so
the service rejects a block whose bytes changed in transit, and the file fails.

## Progress reporting

Records, bytes, files and partitions are tracked independently, with `percentage = total == 0 ? 100.0 : completed * 100.0 / total`:

```text
Migration overview: runId=20261117-204240-0fef7d table=hdfs://nn/warehouse/orders snapshotId=825090155793393105
  Total partitions: 3
  Total data files: 4
  Total records: 1000
  Total bytes: 20971520
  Delete files: 0
  Target: abfss://data@salesacct.dfs.core.windows.net/sales/orders
Partition processing started: partition=region=EU specId=0 files=2 physicalRecords=500
Source file processing started: source=hdfs://... target=sales/orders/region=EU__... physicalRecords=250 deleteFiles=0
Source file completed: source=hdfs://... target=... recordsWritten=250 recordsDeleted=0 sourceBytes=10485760 targetBytes=6291456 durationMs=61 readMiBPerSecond=163.9
Progress: files=2/4 (50%) remaining=2 failed=0 inFlight=2 records=750/1000 (75%) written=750 deleted=0 bytesRead=15MiB/20MiB (75%) bytesWritten=9.5MiB rate=12.5MiB/s (5m) elapsed=0:00:30 eta=0:00:10
Partition completed: partition=region=EU files=2
Partition progress: 2/3 (66.6667%)
Migration completed: runId=20261117-204240-0fef7d
  Records: 1000/1000 (100%) written=1000 deleted=0
  Files: 4/4 (100%) failed=0
  Partitions: 3/3 (100%)
  Bytes: read=20MiB/20MiB written=12.5MiB elapsed=0:00:41
```

**The `Progress:` heartbeat.** A timer logs one line every `migration.progress.log-every` (30s), whether or not any
file reports progress, so a stalled file shows as numbers that stop moving, with `rate=0B/s` and `eta=unknown`:

| Field | Meaning |
|---|---|
| `files=done/total (%)` | Files published |
| `remaining` | Files not yet published or failed: the ones this run still has to copy |
| `failed`, `inFlight` | Files that failed in this run; files being copied right now |
| `records=read/total (%)`, `written`, `deleted` | Physical source rows read so far (published files plus rows of the files in flight), the live rows among them, and the rows removed by deletes |
| `bytesRead=read/total (%)` | Source data file sizes from Iceberg metadata. A file in flight counts for the share of its rows read, so this is an estimate until the file is published |
| `bytesWritten` | Size of the target files written so far. Row groups are buffered before they are written, so a file in flight shows what was flushed so far |
| `rate` | Source bytes read per second over the last 5 minutes of this run (since the start, in the first 5 minutes) |
| `elapsed` | Time since this run started |
| `eta` | Source bytes still to copy, divided by the rate; `unknown` while the rate is 0. Files that failed are not left to copy, while the rows a failed file read before failing are in the rate |

Each completed file also logs `Source file completed` with its source and target sizes and read speed.

- **Record totals are physical rows** (before deletes), the only total Iceberg metadata gives exactly without
  scanning the table. Rows removed by deletes count as processed and are reported as `deleted`, so a completed
  run always reaches 100%.
- **Committed and in flight.** Rows of the files being written are reported as `inFlight`, summed over the files
  copied in parallel; they become `committed` only when their file is published, and are dropped if it fails.
  Files count when published; partitions count when all their files are. With files copied in parallel, the log
  lines of different files interleave; the thread name (`file-worker-N`) tells them apart.
- **The control row carries the same counts**, written with every `Progress:` line: see [State tables](#state-tables).
- **The file table trails the log slightly:** outcomes are written in batches (see
  `migration.state.outcome-batch-size`), so a finished file can show `PENDING` there until its batch is written.
- For JSON logs set `logging.structured.format.console=ecs` (or `logstash`).

## Failure handling

- **A failed file publishes nothing.** Its staged blocks are never committed; Azure discards them after 7 days.
- **Every file is verified before publishing:** all physical rows recorded in Iceberg metadata must have been read,
  and the closed Parquet file must hold exactly the rows written.
- **`FAIL_FAST`** (default) starts no more files after the first failed one; files already being copied in
  parallel finish and count. **`CONTINUE`** processes the rest. Either way the run ends `FAILED`, lists the failed
  files (source, target, reason) and throws `MigrationFailedException`; the next run needs
  `migration.target.clean=true` and copies everything again.
- **Checked before anything is written:** a run that did not complete, another live run, non-Parquet data files,
  unreadable delete files, format versions above 3, schema mismatches, a non-empty target without `clean`, a target
  overlapping another migration's, two source files that would get the same target name.
- **State table failures** are retried with jittered backoff (about 1s, then 2s, ...) when they are Delta commit
  conflicts or transient; if they persist the run stops and is recorded as `FAILED` where it still can be. An error
  that retrying cannot change, such as a missing table or permission or a statement Spark rejects, stops it at once.
  A run that crashed, or could not record its end, stays `RUNNING` and counts as failed once its heartbeat is older
  than `migration.state.heartbeat-timeout`. A heartbeat renewal that keeps failing is logged and tried again with the
  next `Progress:` line; after half the timeout without one, the run stops publishing.

## Output

- **One flat folder per target**, with the partition in the file name, for example
  `sales/orders/region=EU.event_ts_day=2026-01-15__00000-5-a1b2c3d4-0001-00001-9f3e1c2a.parquet`. Names are
  deterministic and contain a hash of the source location, so source files with the same name in different folders
  do not collide; should two source files still get the same name, the run fails before writing anything. Names
  never start with `_` or `.`, which Spark would skip.
- **Schema.** Every file uses the table schema: the current schema when migrating the current snapshot (as a
  plain read does), or the snapshot's own schema when `snapshot-id` is set (as time travel does). Older files are
  projected onto it: added columns are null, renamed columns appear under their current names.
- **Equality deletes keep working after their key column is dropped.** A delete file written against a column
  that was dropped later still applies, whether the run reads the current snapshot or an older one: the column is
  looked up in the table's schema history, and is read only to apply the delete, never written to the output.
- **Types** are written exactly as Iceberg writes them, with Parquet field ids. Databricks cannot read Parquet
  `UUID` or `TIME` columns; the app warns about them, and converts a top-level `uuid` or `time` column to string
  only when the target schema declares it as `string`.
- A source file whose rows are all deleted still produces a valid, empty Parquet file.

## Tests

```bash
./mvnw verify
```

- **Unit tests** for configuration, schema matching, file naming, progress, state store SQL, run start rules,
  claims racing for the control row (including two first runs whose inserts both commit), and the run's heartbeat.
- **End-to-end tests** run the whole application against real Iceberg tables: deletes of every kind, schema
  changes, empty and never-written tables, fail-fast, continue, a failed run blocking the next start until clean,
  clean refused under a live run and allowed once its heartbeat is stale, progress counters on the control row, a
  run that loses the migration (between files, and while a file is being written) publishing nothing more.
- **HDFS:** one end-to-end test reads the source from an in-process HDFS (`MiniDFSCluster`).
- **Bounded memory:** `BoundedMemoryTest` (tag `memory`) runs in its own JVM with `-Xmx128m` and the serial
  collector, and migrates two source files, each larger than the heap, at the same time.
- **Async block staging:** `AzureStagedUploadTest` drives the upload against a stand-in blob service whose Put Block
  calls it can hold, to show that writing continues meanwhile, that at most one block is in flight, and that a
  failed block fails the upload.
- **Azure:** the Blob adapter tests and one end-to-end test need a real Blob API. They run when
  `AZURE_STORAGE_TEST_CONNECTION_STRING` is set, for example against Azurite, whose development account the Azure
  SDK knows as `UseDevelopmentStorage=true`:

  ```bash
  npx azurite --inMemoryPersistence --skipApiVersionCheck --loose --blobHost 127.0.0.1 &
  export AZURE_STORAGE_TEST_CONNECTION_STRING="UseDevelopmentStorage=true"
  ./mvnw verify
  ```

- **CI** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs `./mvnw verify` on JDK 21 for every pull
  request and every push to `master`, with Azurite, so the Azure tests run too.

The Maven wrapper keeps its launcher jar in `.mvn/wrapper`, so it downloads Maven through Java, with no `curl` or
`wget`, and checks the download's SHA-256.

Spark and Delta do not run in the tests: the orchestration tests keep state in an in-memory stand-in for the
repository, and the Spark bridge is tested against stand-in classes with Spark's API shape. The repository's
statements were run by hand through `SparkSqlExecutor` on open-source Spark 4.0.1 and Delta 4.0.0: creating and
checking the tables, two first runs inserting the control row at once and two runs claiming it at once (in both,
Delta failed one commit as concurrent, and its retry no longer applied), renewals by the owner and by a run that
lost the row, finishing, and refusing an earlier version's control table.

## Not yet verified outside tests

- Managed identity sign-in, and the state tables on Databricks' own Spark and Delta with Unity Catalog, including
  row-level concurrency between parallel jobs and between two first runs inserting the same control row: these
  need the Databricks workspace.
- Which state table errors Databricks reports as commit conflicts, analysis or permission errors: the retry
  decision is tested against stand-ins with the shape of Spark's and Delta's exceptions.
- A production HDFS cluster (tests use an in-process one).
- Equality deletes are loaded into memory per data file; memory then grows with the size of the equality delete
  files, not the data files. Tables here are append-only, so none are expected.
- The best `parallel-files` for a real cluster: it depends on HDFS, network and Azure throughput, so measure it
  with the `durationMs` of each file and the run's total time.
