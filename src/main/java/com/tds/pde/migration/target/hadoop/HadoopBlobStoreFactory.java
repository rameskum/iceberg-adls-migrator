package com.tds.pde.migration.target.hadoop;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.BlobStoreFactory;
import com.tds.pde.migration.target.azure.AzureLocation;
import java.net.URI;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Opens the target through the Hadoop file system the cluster's Spark session uses for its URI, so the target is
 * written with the access the cluster already has to the storage, and this application needs no Azure credential of
 * its own. It reads no {@code migration.azure} setting.
 */
@Slf4j
public class HadoopBlobStoreFactory implements BlobStoreFactory {

    private final Supplier<ClassLoader> sparkClassLoader;

    /** Finds Spark, and the Hadoop it runs on, through the JVM's system class loader, where the runtime's jars live. */
    public HadoopBlobStoreFactory() {
        this(ClassLoader::getSystemClassLoader);
    }

    HadoopBlobStoreFactory(Supplier<ClassLoader> sparkClassLoader) {
        this.sparkClassLoader = sparkClassLoader;
    }

    @Override
    public BlobStore open(AzureLocation location) {
        URI root = location.fileSystemUri();
        HadoopFileSystem fileSystem = HadoopFileSystem.fromSparkSession(sparkClassLoader.get(), root);
        log.info("Target storage opened: client=hadoop fileSystem={} uri={} prefix={}", fileSystem.implementation(),
                root, location.prefix());
        return new HadoopBlobStore(fileSystem, root);
    }
}
