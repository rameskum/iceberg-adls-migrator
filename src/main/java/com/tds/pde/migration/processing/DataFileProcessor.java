package com.tds.pde.migration.processing;

import com.tds.pde.migration.destination.BlobStore;
import com.tds.pde.migration.destination.OutputRecordMapper;
import com.tds.pde.migration.destination.ParquetDataWriter;
import com.tds.pde.migration.destination.StagedUpload;
import com.tds.pde.migration.source.IcebergDataReader;
import com.tds.pde.migration.source.SourceRecords;
import java.time.Duration;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;

/**
 * Copies one source data file to one destination Parquet file: streams its live rows, writes them, verifies the
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

    public DataFileProcessor(IcebergDataReader reader, ParquetDataWriter writer, BlobStore blobStore) {
        this.reader = reader;
        this.writer = writer;
        this.blobStore = blobStore;
    }

    /**
     * Never throws for a failure of this file: the failure is logged with its source and destination and returned.
     */
    public FileResult process(RunContext run, FileTask task, FileProgressListener listener) {
        long started = System.nanoTime();
        log.info("Source file processing started: source={} destination={} partition={} physicalRecords={} "
                        + "deleteFiles={}", task.source().location(), task.destination(),
                task.partitionPath().isEmpty() ? "<unpartitioned>" : task.partitionPath(),
                task.source().recordCount(), task.source().deleteFileCount());

        StagedUpload upload = blobStore.startUpload(task.destination());
        long physicalRowsRead = 0;
        long recordsWritten = 0;
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
                            listener.onProgress(rows.physicalRowsRead(), recordsWritten);
                        }
                    }
                }
                physicalRowsRead = rows.physicalRowsRead();
            }
            // Still in flight: the rows only count once the file is published.
            listener.onProgress(physicalRowsRead, recordsWritten);
            verify(task, physicalRowsRead, recordsWritten, appender.metrics().recordCount());

            upload.commit(PublishedFile.metadata(run, task, physicalRowsRead, recordsWritten));
            published = true;

            Duration duration = Duration.ofNanos(System.nanoTime() - started);
            log.info("Source file completed: source={} destination={} recordsWritten={} recordsDeleted={} "
                            + "durationMs={}", task.source().location(), task.destination(), recordsWritten,
                    physicalRowsRead - recordsWritten, duration.toMillis());
            return FileResult.succeeded(task, physicalRowsRead, recordsWritten, duration);
        } catch (Exception e) {
            Duration duration = Duration.ofNanos(System.nanoTime() - started);
            log.error("Source file failed, nothing was published: source={} destination={} "
                            + "recordsWrittenBeforeFailure={} reason={}", task.source().location(), task.destination(),
                    recordsWritten, FileResult.describe(e), e);
            return FileResult.failed(task, physicalRowsRead, recordsWritten, duration, e);
        } finally {
            if (!published) {
                upload.abort();
            }
        }
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
