package com.tds.pde.migration.processing;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.OutputRecordMapper;
import com.tds.pde.migration.target.ParquetDataWriter;
import com.tds.pde.migration.target.StagedUpload;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.source.SourceRecords;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;

/**
 * Copies one source data file to one target Parquet file: streams its live rows, writes them, verifies the
 * written file, and only then publishes it. Knows nothing about runs, progress totals or state persistence, so
 * it can be wrapped by checkpointing or parallel execution unchanged.
 */
@Slf4j
public class DataFileProcessor {

    /** How often, in rows, in-flight progress is reported. */
    static final int PROGRESS_INTERVAL = 1024;

    private final IcebergDataReader reader;
    private final ParquetDataWriter writer;
    private final BlobStore blobStore;
    private final Runnable beforePublish;

    public DataFileProcessor(IcebergDataReader reader, ParquetDataWriter writer, BlobStore blobStore) {
        this(reader, writer, blobStore, () -> {
        });
    }

    /**
     * @param beforePublish runs immediately before each publish and throws to prevent it, e.g. when the run lost
     *                      its claim on the migration while the file was being written
     */
    public DataFileProcessor(IcebergDataReader reader, ParquetDataWriter writer, BlobStore blobStore,
                             Runnable beforePublish) {
        this.reader = reader;
        this.writer = writer;
        this.blobStore = blobStore;
        this.beforePublish = beforePublish;
    }

    /**
     * Never throws for a failure of this file: the failure is logged with its source and target and returned.
     */
    public FileResult process(RunContext run, FileTask task, FileProgressListener listener) {
        long started = System.nanoTime();
        log.info("Source file processing started: source={} target={} partition={} physicalRecords={} "
                        + "deleteFiles={}", task.source().location(), task.target(),
                task.partitionPath().isEmpty() ? "<unpartitioned>" : task.partitionPath(),
                task.source().recordCount(), task.source().deleteFileCount());

        StagedUpload upload = blobStore.startUpload(task.target());
        long physicalRowsRead = 0;
        long recordsWritten = 0;
        long bytesWritten = 0;
        boolean published = false;
        try {
            FileAppender<Record> appender;
            try (SourceRecords rows = reader.open(run.source(), task.source().task())) {
                appender = writer.open(upload, run.outputSchema().schema(), footerMetadata(run, task));
                try (appender) {
                    OutputRecordMapper mapper = new OutputRecordMapper(rows.schema(), run.outputSchema());
                    for (Record row : rows) {
                        appender.add(mapper.map(row));
                        recordsWritten++;
                        if (recordsWritten % PROGRESS_INTERVAL == 0) {
                            listener.onProgress(rows.physicalRowsRead(), recordsWritten, appender.length());
                        }
                    }
                }
                physicalRowsRead = rows.physicalRowsRead();
                bytesWritten = appender.length();
            }
            // Still in flight: the rows only count once the file is published.
            listener.onProgress(physicalRowsRead, recordsWritten, bytesWritten);
            verify(task, physicalRowsRead, recordsWritten, appender.metrics().recordCount());

            beforePublish.run();
            upload.commit(PublishedFile.metadata(run, task, physicalRowsRead, recordsWritten));
            published = true;

            Duration duration = Duration.ofNanos(System.nanoTime() - started);
            log.info("Source file completed: source={} target={} recordsWritten={} recordsDeleted={} "
                            + "sourceBytes={} targetBytes={} durationMs={} readMiBPerSecond={}",
                    task.source().location(), task.target(), recordsWritten, physicalRowsRead - recordsWritten,
                    task.source().sizeInBytes(), bytesWritten, duration.toMillis(),
                    mebibytesPerSecond(task.source().sizeInBytes(), duration));
            return FileResult.succeeded(task, physicalRowsRead, recordsWritten, bytesWritten, duration);
        } catch (Exception e) {
            Duration duration = Duration.ofNanos(System.nanoTime() - started);
            log.error("Source file failed, nothing was published: source={} target={} "
                            + "recordsWrittenBeforeFailure={} reason={}", task.source().location(), task.target(),
                    recordsWritten, FileResult.describe(e), e);
            return FileResult.failed(task, physicalRowsRead, recordsWritten, duration, e);
        } finally {
            if (!published) {
                upload.abort();
            }
        }
    }

    /** Source bytes per second over the file's whole duration, with one decimal. */
    private static String mebibytesPerSecond(long bytes, Duration duration) {
        double seconds = duration.toNanos() / 1e9;
        return seconds == 0 ? "n/a" : String.format(Locale.ROOT, "%.1f", bytes / (1024.0 * 1024.0) / seconds);
    }

    /**
     * Guards against silent data loss: every physical row must have been read, and the closed writer must hold
     * exactly the rows handed to it.
     */
    private static void verify(FileTask task, long physicalRowsRead, long recordsWritten, long recordsInFile) {
        if (physicalRowsRead != task.source().recordCount()) {
            throw new IllegalStateException("Read " + physicalRowsRead + " rows but the source metadata records "
                    + task.source().recordCount());
        }
        if (recordsInFile != recordsWritten) {
            throw new IllegalStateException("Wrote " + recordsWritten + " rows but the finished file holds "
                    + recordsInFile);
        }
    }

    private static Map<String, String> footerMetadata(RunContext run, FileTask task) {
        return Map.of(
                "migration.run-id", run.runId(),
                "migration.source-file", task.source().location(),
                "migration.source-snapshot-id", String.valueOf(run.source().snapshotId()));
    }
}
