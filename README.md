# iceberg-adls-migrator

Copies an Apache Iceberg table stored on HDFS into standalone Parquet files on Azure Blob Storage (ADLS Gen2
account without hierarchical namespace). The destination is plain Parquet: no Iceberg metadata is created.

- One source data file becomes exactly one destination Parquet file.
- The output holds the **logical** table data: position deletes, equality deletes and deletion vectors are applied.
- Rows are streamed one at a time; memory stays bounded however large the table is.
- A file becomes visible at its destination only once it is complete and verified, so a failed or interrupted
  write can never be mistaken for a finished file.
- Runs are recorded per file in a state store and can be resumed.

It runs on Databricks (DBR 18, Java 21) through the team's isolated launcher, and reads HDFS with the plain
Iceberg and Hadoop Java clients, not with Spark.

## How it works

```
Databricks job ──app_args──▶ team launcher (isolated class loader) ──▶ MigrationApplication.main
  1. Look up the source and destination datasets (dataset API; a JSON stub for now)
  2. Take the destination's run lock (a renewing blob lease)
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
| `DestinationSchemaValidator` | Checks the destination schema against the Iceberg schema; decides string conversions |
| `PartitionProcessor` | Per-file checkpoint: lock check, in progress, process, completed or failed, failure policy |
| `DataFileProcessor` | Copies one source file to one destination file; independent of runs, progress and state |
| `IcebergDataReader` | Streams the live rows of one data file, the way Iceberg's generic reader does |
| `ParquetDataWriter` | Writes Parquet with Iceberg's writer (exact type mapping), including empty files |
| `AzureBlobStore` | Staged block upload and atomic commit, metadata, listing, deletes, lease lock |
| `MigrationStateRepository` | Run and file state over JDBC (Databricks SQL), with retries |
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
| `app_args` | `--spring.config.additional-location=/Volumes/<catalog>/<schema>/<volume>/iceberg-adls-migrator/application.yaml` plus any overrides, e.g. `--migration.run-id=...` |

**Secrets** come from environment variables set on the cluster with secret references, never from the YAML file,
for example `DATABRICKS_SQL_TOKEN={{secrets/<scope>/<key>}}`.

**Before the first run:**
- create the destination container (the app never creates it);
- grant the identity *Storage Blob Data Contributor* on it;
- create the state tables from [`src/main/resources/db/databricks/migration-state.sql`](src/main/resources/db/databricks/migration-state.sql),
  or run once with `migration.state.create-tables=true`.

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
| `migration.destination.dataset-id` | required | Destination dataset (format `PARQUET`, location = `abfss://...`) |
| `migration.destination.clean` | `false` | Delete everything under the destination and start a fresh run |
| `migration.destination.block-size` | `8MB` | Size of each staged block; also the upload memory per file |
| `migration.datasets.stub-directory` | required | Folder of `<dataset-id>.json` descriptors |
| `migration.azure.auth.mode` | `MANAGED_IDENTITY` | Or `CONNECTION_STRING` (local runs and tests only) |
| `migration.azure.auth.client-id` | empty | Client id of a user-assigned managed identity |
| `migration.azure.auth.connection-string` | empty | Required for `CONNECTION_STRING` mode |
| `migration.parquet.row-group-size` | `128MB` | Parquet row group size; the main writer memory |
| `migration.parquet.page-size` | `1MB` | Parquet page size |
| `migration.parquet.compression` | `ZSTD` | `ZSTD`, `SNAPPY`, `GZIP` or `UNCOMPRESSED` |
| `migration.progress.log-every-records` | `100000` | Log record progress at least this often (in records) |
| `migration.progress.log-every` | `30s` | ...and at least this often (in time) |
| `migration.failure.on-file-failure` | `FAIL_FAST` | Or `CONTINUE` with the remaining files |
| `migration.state.run-table` | required | e.g. `main.migration.migration_run` |
| `migration.state.file-table` | required | e.g. `main.migration.migration_file` |
| `migration.state.create-tables` | `false` | Run the bundled Delta DDL at startup |
| `migration.state.write-retries` | `3` | Attempts per state store call before the run stops |
| `spring.datasource.url`, `.password` | none | Databricks SQL warehouse JDBC URL and token |

**Authentication.** The `TokenCredential` bean in `AzureConfig` is the single place to plug in a different
sign-in mechanism. The default is the cluster's managed identity.

**Dataset descriptors.** Until the dataset API is wired in, `StubDatasetClient` serves recorded responses:

```json
{
  "datasetId": "orders-adls",
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

## Runs and resume

What happens depends on `migration.run-id` and `migration.destination.clean`:

| `run-id` | `clean` | Behaviour |
|---|---|---|
| set | `false` | Resume that run. It must copy the same source to the same destination and not be superseded |
| empty | `false` | Resume the latest RUNNING or FAILED run for this source and destination; otherwise start a new run, which requires an empty destination |
| empty | `true` | Delete everything under the destination prefix (refused at a container root), mark earlier runs `SUPERSEDED`, start a new run |
| set | `true` | Refused |

So a Databricks task retry or *Repair run*, which re-sends the same parameters, simply continues where the run
stopped.

**A run pins** its snapshot, read schema, output schema and file list. A resume plans the pinned snapshot again
and refuses to continue if any of these no longer match (for example, the destination schema was edited, or
snapshot expiry removed a file).

**What counts as done.** Every destination blob is committed together with its metadata (`migration_run_id`,
`source_file`, `source_snapshot_id`, `physical_records`, `records_written`). On resume, a blob carrying this run's
markers is a finished file even if recording it in the state store failed; it is recorded and not written again.

**One run per destination.** A run holds a 60-second blob lease on `<prefix>/_migration/lock`, renewed every
20 seconds. A second run fails at once. If a run dies, its lease lapses within a minute. A run that loses its lease
stops before the next file.

## Progress reporting

Records, files and partitions are tracked independently, with `percentage = total == 0 ? 100.0 : completed * 100.0 / total`:

```text
Migration overview: runId=20261117-204240-0fef7d resumed=false table=hdfs://nn/warehouse/orders snapshotId=825090155793393105
  Total partitions: 3
  Total data files: 4
  Total records: 1000
  Delete files: 0
  Destination: abfss://data@salesacct.dfs.core.windows.net/sales/orders
Partition processing started: partition=region=EU specId=0 files=2 physicalRecords=500
Source file processing started: source=hdfs://... destination=sales/orders/region=EU__... physicalRecords=250 deleteFiles=0
Record progress: 500/1000 (50%) committed=250 inFlight=250 written=500
Source file completed: source=hdfs://... destination=... recordsWritten=250 recordsDeleted=0 durationMs=61
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
- **Committed and in flight.** Rows of the file being written are reported as `inFlight`; they become
  `committed` only when that file is published, and are dropped if it fails. Files count when published;
  partitions count when all their files are.
- For JSON logs set `logging.structured.format.console=ecs` (or `logstash`).

## Failure handling

- **A failed file publishes nothing.** Its staged blocks are never committed; Azure discards them after 7 days.
- **Every file is verified before publishing:** all physical rows recorded in Iceberg metadata must have been read,
  and the closed Parquet file must hold exactly the rows written.
- **`FAIL_FAST`** (default) stops at the first failed file; **`CONTINUE`** processes the rest. Either way the run
  ends `FAILED`, lists the failed files (source, destination, reason) and throws `MigrationFailedException` with
  the run id to resume.
- **Checked before anything is written:** non-Parquet data files, unreadable delete files, format versions above 3,
  schema mismatches, a non-empty destination for a new run, another active run.
- **State store failures** are retried with backoff (1s, then 2s, ...); if they persist the run stops, and the next
  resume recognises published files by their blob metadata.

## Output

- **One flat folder per destination**, with the partition in the file name, for example
  `sales/orders/region=EU.event_ts_day=2026-01-15__00000-5-a1b2c3d4-0001-00001-9f3e1c2a.parquet`. Names are
  deterministic and contain a hash of the source location, so source files with the same name in different folders
  never collide. Names never start with `_` or `.`, which Spark would skip.
- **Schema.** Every file uses the table schema: the current schema when migrating the current snapshot (as a
  plain read does), or the snapshot's own schema when `snapshot-id` is set (as time travel does). Older files are
  projected onto it: added columns are null, renamed columns appear under their current names.
- **Types** are written exactly as Iceberg writes them, with Parquet field ids. Databricks cannot read Parquet
  `UUID` or `TIME` columns; the app warns about them, and converts a top-level `uuid` or `time` column to string
  only when the destination schema declares it as `string`.
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
- **Bounded memory:** `BoundedMemoryTest` (tag `memory`) runs in its own JVM with `-Xmx128m` and migrates a
  source file larger than the heap.
- **Azure:** the Blob adapter tests and one end-to-end test need a real Blob API. They run when
  `AZURE_STORAGE_TEST_CONNECTION_STRING` is set, for example against Azurite:

  ```bash
  npx azurite --inMemoryPersistence --skipApiVersionCheck --loose --blobHost 127.0.0.1 &
  export AZURE_STORAGE_TEST_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<Azurite's well-known key>;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
  ./mvnw verify
  ```

  Azurite's account key is published in its documentation.

The state store tests run on H2; the SQL is plain `INSERT`, `UPDATE` and `SELECT`, the same statements used on
Databricks SQL.

## Not yet verified outside tests

- Managed identity sign-in, the Databricks SQL JDBC driver and the Delta DDL: these need the Databricks workspace.
- A production HDFS cluster (tests use an in-process one).
- Equality deletes are loaded into memory per data file; memory then grows with the size of the equality delete
  files, not the data files. Tables here are append-only, so none are expected.
- Files are processed one at a time. `DataFileProcessor` is independent of the run, so file-level parallelism can
  be added around it later.
