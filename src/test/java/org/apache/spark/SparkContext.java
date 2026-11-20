package org.apache.spark;

import org.apache.hadoop.conf.Configuration;

/**
 * Test stand-in with the shape of the Spark API that the Hadoop target client calls by reflection. Not Spark. Its
 * Hadoop configuration serves {@code abfs://} and {@code abfss://} with
 * {@link com.tds.pde.migration.testing.FlatNamespaceFileSystem}, the way a cluster's configuration names its ABFS
 * driver.
 */
public class SparkContext {

    /** By name, as a cluster configuration names it: Hadoop loads it through the configuration's class loader. */
    private static final String FILE_SYSTEM = "com.tds.pde.migration.testing.FlatNamespaceFileSystem";

    private static final Configuration HADOOP_CONFIGURATION = defaults();

    public Configuration hadoopConfiguration() {
        return HADOOP_CONFIGURATION;
    }

    private static Configuration defaults() {
        Configuration configuration = new Configuration();
        for (String scheme : new String[] {"abfs", "abfss"}) {
            configuration.set("fs." + scheme + ".impl", FILE_SYSTEM);
            // A new instance per lookup, so no test sees another's file system object.
            configuration.setBoolean("fs." + scheme + ".impl.disable.cache", true);
        }
        return configuration;
    }
}
