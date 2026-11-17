package com.tds.pde.migration.destination;

import com.tds.pde.migration.destination.azure.AzureLocation;

/**
 * Opens the destination storage once the destination location is known.
 */
public interface BlobStoreFactory {

    BlobStore open(AzureLocation location);
}
