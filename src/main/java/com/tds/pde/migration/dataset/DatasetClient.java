package com.tds.pde.migration.dataset;

/**
 * Looks up dataset descriptors. The real dataset API client will replace {@link StubDatasetClient}.
 */
public interface DatasetClient {

    /**
     * @throws DatasetLookupException when the dataset does not exist or its descriptor is invalid
     */
    DatasetDescriptor getDataset(String datasetId);
}
