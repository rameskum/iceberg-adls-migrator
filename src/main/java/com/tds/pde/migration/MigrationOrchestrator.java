package com.tds.pde.migration;

import com.tds.pde.migration.config.MigrationProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Coordinates the migration lifecycle. Discovery, file processing and state tracking are added in later steps.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MigrationOrchestrator {

    private final MigrationProperties properties;

    public void migrate() {
        log.info("Migration started: sourceDataset={} destinationDataset={} runId={}",
                properties.source().datasetId(),
                properties.destination().datasetId(),
                properties.runId() == null ? "<new or auto-resume>" : properties.runId());
    }
}
