package com.tds.pde.migration.target;

import com.tds.pde.migration.target.azure.AzureLocation;

/**
 * Opens the target storage once the target location is known.
 */
public interface BlobStoreFactory {

    BlobStore open(AzureLocation location);
}
