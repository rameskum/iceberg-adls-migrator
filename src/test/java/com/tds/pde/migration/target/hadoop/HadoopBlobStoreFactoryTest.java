package com.tds.pde.migration.target.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.BlobStore.BlobInfo;
import com.tds.pde.migration.target.StagedUpload;
import com.tds.pde.migration.target.azure.AzureLocation;
import com.tds.pde.migration.testing.FlatNamespaceFileSystem;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Hadoop client as it runs on Databricks: the file system comes from the active Spark session's Hadoop
 * configuration (the stand-in {@code org.apache.spark.SparkContext} under the test sources), which serves
 * {@code abfss://} with {@link FlatNamespaceFileSystem}, a fake of the ABFS driver on a flat namespace.
 */
class HadoopBlobStoreFactoryTest {

    private static final String AUTHORITY = "data@salesacct.dfs.core.windows.net";
    private static final AzureLocation LOCATION = AzureLocation.parse("abfss://" + AUTHORITY + "/sales/orders");
    private static final String RUN_ID = "20261120-090000-a1b2c3";
    private static final byte[] CONTENT = {1, 2, 3};

    @BeforeEach
    @AfterEach
    void reset() {
        FlatNamespaceFileSystem.reset();
    }

    @Test
    void writesThroughTheFileSystemOfSparksOwnClassLoader() throws IOException {
        // On Databricks, Spark and its Hadoop live in the system class loader, apart from the application's Hadoop.
        try (URLClassLoader sparkLoader = new URLClassLoader(classPath(), ClassLoader.getPlatformClassLoader())) {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            BlobStore store = new HadoopBlobStoreFactory(() -> sparkLoader).open(LOCATION);

            publish(store, LOCATION.blobName("a.parquet"));
            StagedUpload unfinished = store.startUpload(LOCATION.blobName("b.parquet"), RUN_ID);
            unfinished.outputStream().close();

            assertThat(store.list(LOCATION.blobPrefix())).containsExactly(
                    new BlobInfo("sales/orders/_migration/tmp/" + RUN_ID + "/b.parquet", 0),
                    new BlobInfo("sales/orders/a.parquet", CONTENT.length));
            unfinished.abort();
            store.delete(LOCATION.blobName("a.parquet"));
            store.deleteUploadLeftovers(LOCATION.blobPrefix());
            assertThat(store.list(LOCATION.blobPrefix())).isEmpty();

            assertThat(FlatNamespaceFileSystem.files(AUTHORITY))
                    .as("written by Spark's copy of the file system, not the application's").isEmpty();
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(contextLoader);
        }
    }

    @Test
    void publishesOnAFlatNamespaceByRenamingAndLeavesNothingOnceTheLeftoversAreDeleted() throws IOException {
        BlobStore store = factory().open(LOCATION);
        String name = LOCATION.blobName("region=EU__part-1.parquet");

        publish(store, name);

        assertThat(FlatNamespaceFileSystem.files(AUTHORITY)).containsOnlyKeys("/" + name);
        assertThat(FlatNamespaceFileSystem.folders(AUTHORITY)).as("markers the driver left for the temporary file")
                .contains("/sales/orders/_migration/tmp/" + RUN_ID);

        store.deleteUploadLeftovers(LOCATION.blobPrefix());

        assertThat(FlatNamespaceFileSystem.folders(AUTHORITY)).containsExactly("/sales", "/sales/orders");
        assertThat(FlatNamespaceFileSystem.files(AUTHORITY)).containsOnlyKeys("/" + name);
    }

    @Test
    void neverLeavesAPartialFileUnderTheFinalName() throws IOException {
        BlobStore store = factory().open(LOCATION);
        String name = LOCATION.blobName("a.parquet");
        FlatNamespaceFileSystem.dropLastByteOf(path -> path.contains("/_migration/tmp/"));

        assertThatThrownBy(() -> publish(store, name))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("holds 2 bytes, but 3 were written");

        assertThat(FlatNamespaceFileSystem.files(AUTHORITY)).isEmpty();
    }

    @Test
    void failsTheFileWhenTheRenameFailsBetweenCopyAndDeleteKeepingTheCopyComplete() throws IOException {
        BlobStore store = factory().open(LOCATION);
        String name = LOCATION.blobName("a.parquet");
        FlatNamespaceFileSystem.failRenamesAfterCopyTo(path -> path.equals("/" + name));

        assertThatThrownBy(() -> publish(store, name))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining("injected failure after copying");

        assertThat(FlatNamespaceFileSystem.files(AUTHORITY)).containsOnlyKeys("/" + name);
        assertThat(FlatNamespaceFileSystem.files(AUTHORITY).get("/" + name)).isEqualTo(CONTENT);
    }

    @Test
    void reportsAFileSystemHadoopCannotOpen() {
        AzureLocation unserved = AzureLocation.parse("abfss://" + AUTHORITY + "/x");
        HadoopBlobStoreFactory factory = new HadoopBlobStoreFactory(() -> new URLClassLoader(classPath(),
                ClassLoader.getPlatformClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals(FlatNamespaceFileSystem.class.getName())) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        });

        assertThatThrownBy(() -> factory.open(unserved))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Cannot open the target file system abfss://" + AUTHORITY
                        + " through the Spark session's Hadoop configuration: ");
    }

    @Test
    void needsSpark() {
        HadoopBlobStoreFactory withoutSpark = new HadoopBlobStoreFactory(() -> new URLClassLoader(new URL[0], null));

        assertThatThrownBy(() -> withoutSpark.open(LOCATION))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Spark is not available on the system class path; "
                        + "migration.target.client=hadoop needs the Databricks Runtime's Spark session");
    }

    private static HadoopBlobStoreFactory factory() {
        return new HadoopBlobStoreFactory(() -> HadoopBlobStoreFactoryTest.class.getClassLoader());
    }

    private static void publish(BlobStore store, String name) throws IOException {
        StagedUpload upload = store.startUpload(name, RUN_ID);
        try {
            try (OutputStream out = upload.outputStream()) {
                out.write(CONTENT);
            }
            upload.commit(Map.of());
        } catch (IOException | RuntimeException e) {
            upload.abort();
            throw e;
        }
    }

    /** The test JVM's class path, for a class loader that loads its own copy of Hadoop and the Spark stand-ins. */
    private static URL[] classPath() {
        return Stream.of(System.getProperty("java.class.path").split(File.pathSeparator))
                .map(entry -> {
                    try {
                        return new File(entry).toURI().toURL();
                    } catch (MalformedURLException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .toArray(URL[]::new);
    }
}
