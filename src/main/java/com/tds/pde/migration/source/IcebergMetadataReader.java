package com.tds.pde.migration.source;

import com.tds.pde.migration.MigrationFailedException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

/**
 * Discovers what to migrate from Iceberg metadata alone: loads the table by path, pins one snapshot and plans
 * its data files with their delete files. No data file is opened and no HDFS directory is listed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IcebergMetadataReader {

    /** Highest format version whose row-level delete semantics this migrator applies and is tested against. */
    static final int MAX_FORMAT_VERSION = 3;

    /** Delete file formats the generic delete loader reads with the modules on the classpath. */
    private static final Set<FileFormat> READABLE_DELETE_FORMATS =
            EnumSet.of(FileFormat.PARQUET, FileFormat.AVRO, FileFormat.PUFFIN);

    private static final Comparator<PartitionKey> PARTITION_ORDER =
            Comparator.comparingInt(PartitionKey::specId).thenComparing(PartitionKey::path);

    private final Configuration hadoopConfiguration;

    /**
     * @param snapshotId snapshot to migrate; {@code null} for the table's current snapshot
     * @throws MigrationFailedException when the table cannot be loaded or contains anything this migrator cannot
     *                                  read faithfully, before any data is written
     */
    public MigrationPlan plan(String tableLocation, @Nullable Long snapshotId) {
        Table table = load(tableLocation);
        int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();
        if (formatVersion > MAX_FORMAT_VERSION) {
            throw new MigrationFailedException("Source table " + tableLocation + " uses format version "
                    + formatVersion + "; at most " + MAX_FORMAT_VERSION + " is supported");
        }

        String nameMappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
        NameMapping nameMapping = nameMappingJson == null ? null : NameMappingParser.fromJson(nameMappingJson);

        Snapshot snapshot = resolveSnapshot(table, snapshotId);
        if (snapshot == null) {
            log.info("Source table identified: table={} formatVersion={} snapshotId=none (table has never been written)",
                    tableLocation, formatVersion);
            return MigrationPlan.empty(new SourceTable(tableLocation, formatVersion, null, table.schema(),
                    table.schema(), table.io(), nameMapping));
        }

        TableScan scan = table.newScan().useSnapshot(snapshot.snapshotId());
        // Like a plain read, the current snapshot is read with the current schema, which includes schema changes
        // made after the last write. Like time travel, an explicitly requested snapshot keeps the schema it was
        // written with.
        Schema schema = snapshotId == null ? table.schema() : scan.schema();
        log.info("Source table and snapshot identified: table={} formatVersion={} snapshotId={} "
                        + "snapshotTimestamp={} operation={} schemaId={} columns={} nameMapping={}",
                tableLocation, formatVersion, snapshot.snapshotId(), Instant.ofEpochMilli(snapshot.timestampMillis()),
                snapshot.operation(), schema.schemaId(), schema.columns().size(), nameMapping != null);

        SourceTable source = new SourceTable(tableLocation, formatVersion, snapshot.snapshotId(), schema,
                table.schema(), table.io(), nameMapping);
        return planFiles(table, scan, source);
    }

    private Table load(String tableLocation) {
        try {
            return new HadoopTables(hadoopConfiguration).load(tableLocation);
        } catch (RuntimeException e) {
            throw new MigrationFailedException("Cannot load Iceberg table at " + tableLocation, e);
        }
    }

    private static @Nullable Snapshot resolveSnapshot(Table table, @Nullable Long snapshotId) {
        if (snapshotId == null) {
            return table.currentSnapshot();
        }
        Snapshot snapshot = table.snapshot(snapshotId);
        if (snapshot == null) {
            throw new MigrationFailedException(
                    "Snapshot " + snapshotId + " does not exist in table " + table.location());
        }
        return snapshot;
    }

    private static MigrationPlan planFiles(Table table, TableScan scan, SourceTable source) {
        String tableLocation = source.location();
        Map<PartitionKey, List<SourceFile>> filesByPartition = new TreeMap<>(PARTITION_ORDER);
        Set<String> problems = new LinkedHashSet<>();
        Set<String> deleteFiles = new HashSet<>();
        long totalRecords = 0;
        long totalFiles = 0;

        // planFiles (unlike planTasks) yields exactly one task per data file, never split.
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                if (file.format() != FileFormat.PARQUET) {
                    problems.add("data file " + file.location() + " has format " + file.format()
                            + "; only PARQUET is supported");
                }
                for (DeleteFile delete : task.deletes()) {
                    if (!READABLE_DELETE_FORMATS.contains(delete.format())) {
                        problems.add("delete file " + delete.location() + " has unsupported format "
                                + delete.format());
                    }
                    deleteFiles.add(delete.location());
                }
                PartitionSpec spec = task.spec();
                filesByPartition
                        .computeIfAbsent(new PartitionKey(spec.specId(), spec.partitionToPath(file.partition())),
                                key -> new ArrayList<>())
                        .add(new SourceFile(task));
                totalRecords += file.recordCount();
                totalFiles++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close the file plan of " + tableLocation, e);
        }

        if (!problems.isEmpty()) {
            throw new MigrationFailedException("Source table " + tableLocation
                    + " contains files this migrator cannot read; nothing was written:\n  "
                    + String.join("\n  ", problems));
        }

        List<PlannedPartition> partitions = new ArrayList<>(filesByPartition.size());
        filesByPartition.forEach((key, files) -> {
            files.sort(Comparator.comparing(SourceFile::location));
            partitions.add(new PlannedPartition(key.specId(), key.path(), files));
        });

        filesByPartition.keySet().stream().map(PartitionKey::specId).distinct().forEach(specId ->
                log.info("Partition spec in use: specId={} fields={}", specId, table.specs().get(specId).fields()));
        if (!deleteFiles.isEmpty()) {
            log.warn("Source snapshot has row-level deletes; they are applied while copying: snapshotId={} "
                    + "deleteFiles={}", source.snapshotId(), deleteFiles.size());
        }

        return new MigrationPlan(source, partitions, totalRecords, totalFiles, deleteFiles.size());
    }

    /** Partitions are told apart by spec as well as values: the same path under two specs is two partitions. */
    private record PartitionKey(int specId, String path) {
    }
}
