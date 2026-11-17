package com.tds.pde.migration;

import com.tds.pde.migration.config.MigrationProperties;
import com.tds.pde.migration.dataset.DatasetClient;
import com.tds.pde.migration.dataset.DatasetDescriptor;
import com.tds.pde.migration.dataset.DatasetFormat;
import com.tds.pde.migration.schema.DestinationSchemaValidator;
import com.tds.pde.migration.schema.OutputSchema;
import com.tds.pde.migration.source.IcebergMetadataReader;
import com.tds.pde.migration.source.MigrationPlan;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Coordinates the migration lifecycle. File processing and state tracking are added in later steps.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MigrationOrchestrator {

    private final MigrationProperties properties;
    private final DatasetClient datasetClient;
    private final IcebergMetadataReader metadataReader;
    private final DestinationSchemaValidator schemaValidator;

    /**
     * @throws MigrationFailedException when the run does not complete successfully, with the cause attached
     */
    public void migrate() {
        log.info("Migration started: sourceDataset={} destinationDataset={} runId={}",
                properties.source().datasetId(),
                properties.destination().datasetId(),
                properties.runId() == null ? "<new or auto-resume>" : properties.runId());
        try {
            run();
        } catch (MigrationFailedException e) {
            log.error("Migration failed: sourceDataset={} destinationDataset={} reason={}",
                    properties.source().datasetId(), properties.destination().datasetId(), e.getMessage(), e);
            throw e;
        } catch (RuntimeException e) {
            log.error("Migration failed: sourceDataset={} destinationDataset={} reason={}",
                    properties.source().datasetId(), properties.destination().datasetId(), e.getMessage(), e);
            throw new MigrationFailedException("Migration failed: " + e.getMessage(), e);
        }
    }

    private void run() {
        DatasetDescriptor source = dataset(properties.source().datasetId(), DatasetFormat.ICEBERG);
        DatasetDescriptor destination = dataset(properties.destination().datasetId(), DatasetFormat.PARQUET);

        MigrationPlan plan = metadataReader.plan(source.location(), properties.source().snapshotId());
        OutputSchema outputSchema = schemaValidator.resolve(plan.schema(), destination.schema());

        log.info("Migration overview: table={} snapshotId={} totalPartitions={} totalDataFiles={} "
                        + "totalRecords={} deleteFiles={} outputColumns={} destination={}",
                plan.tableLocation(), plan.snapshotId(), plan.totalPartitions(), plan.totalFiles(),
                plan.totalRecords(), plan.totalDeleteFiles(), outputSchema.schema().columns().size(),
                destination.location());
    }

    private DatasetDescriptor dataset(String datasetId, DatasetFormat expectedFormat) {
        DatasetDescriptor descriptor = datasetClient.getDataset(datasetId);
        if (descriptor.format() != expectedFormat) {
            throw new MigrationFailedException("Dataset '" + datasetId + "' has format " + descriptor.format()
                    + "; expected " + expectedFormat);
        }
        return descriptor;
    }
}
