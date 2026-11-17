package com.tds.pde.migration.dataset;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.tds.pde.migration.config.MigrationProperties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Stands in for the dataset API: serves {@code <stub-directory>/<dataset-id>.json}, a recorded API response.
 */
@Slf4j
@Component
public class StubDatasetClient implements DatasetClient {

    // Keeps the id from escaping the stub directory.
    private static final Pattern DATASET_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            // The real API may return more than the migration needs.
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private final Path directory;

    public StubDatasetClient(MigrationProperties properties) {
        this.directory = properties.datasets().stubDirectory();
    }

    @Override
    public DatasetDescriptor getDataset(String datasetId) {
        if (!DATASET_ID.matcher(datasetId).matches()) {
            throw new DatasetLookupException("Invalid dataset id '" + datasetId + "'");
        }
        Path file = directory.resolve(datasetId + ".json");
        if (!Files.isRegularFile(file)) {
            throw new DatasetLookupException("Dataset '" + datasetId + "' not found: no descriptor at " + file);
        }
        DatasetDescriptor descriptor;
        try {
            descriptor = MAPPER.readValue(file.toFile(), DatasetDescriptor.class);
        } catch (IOException | IllegalArgumentException e) {
            throw new DatasetLookupException("Invalid descriptor for dataset '" + datasetId + "' at " + file, e);
        }
        if (!descriptor.datasetId().equals(datasetId)) {
            throw new DatasetLookupException("Descriptor at " + file + " is for dataset '" + descriptor.datasetId()
                    + "', expected '" + datasetId + "'");
        }
        log.info("Dataset resolved: datasetId={} format={} location={} declaredColumns={}",
                datasetId, descriptor.format(), descriptor.location(),
                descriptor.schema() == null ? "none" : descriptor.schema().fields().size());
        return descriptor;
    }
}
