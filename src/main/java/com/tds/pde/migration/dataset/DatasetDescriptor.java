package com.tds.pde.migration.dataset;

import org.jspecify.annotations.Nullable;

/**
 * A dataset as described by the dataset API: where it lives, in which format, and optionally its schema.
 *
 * @param schema declared schema; {@code null} when the API does not define one
 */
public record DatasetDescriptor(String datasetId, DatasetFormat format, String location,
                                @Nullable DatasetSchema schema) {

    public DatasetDescriptor {
        if (datasetId == null || datasetId.isBlank()) {
            throw new IllegalArgumentException("datasetId is required");
        }
        if (format == null) {
            throw new IllegalArgumentException("format is required for dataset " + datasetId);
        }
        if (location == null || location.isBlank()) {
            throw new IllegalArgumentException("location is required for dataset " + datasetId);
        }
    }
}
