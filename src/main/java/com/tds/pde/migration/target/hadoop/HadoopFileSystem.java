package com.tds.pde.migration.target.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * A Hadoop {@code FileSystem} called by reflection, so that it works whichever class loader loaded its Hadoop.
 *
 * <p>On Databricks the file system that holds the cluster's access to the storage belongs to the runtime's Hadoop, in
 * the JVM's system class loader. The launcher's isolated class loader cannot see it, and the Hadoop classes bundled
 * with the application, which read the source, are other classes of the same names: a cast between them fails. So
 * this calls the runtime's own classes, and only their stable public API: {@code FileSystem.get(URI, Configuration)},
 * {@code create(Path, boolean)}, {@code getFileStatus(Path)}, {@code rename(Path, Path)},
 * {@code delete(Path, boolean)}, {@code listFiles(Path, boolean)}, {@code new Path(URI)}, {@code Path.toUri()},
 * {@code FileStatus.getPath()}, {@code getLen()} and {@code isFile()}, and {@code RemoteIterator.hasNext()} and
 * {@code next()}. Paths cross as {@link URI}s and streams as {@link OutputStream}s, JDK classes both sides share.
 *
 * <p>Every call runs with the file system's class loader as the thread's context class loader, as Hadoop and its
 * storage drivers load their own classes through it. The file system is never closed: {@code FileSystem.get} returns
 * the instance the whole JVM, Spark included, shares.
 */
final class HadoopFileSystem {

    private static final String FILE_SYSTEM_CLASS = "org.apache.hadoop.fs.FileSystem";
    private static final String CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";

    /**
     * A file found by a listing.
     *
     * @param path the file's path within the file system, decoded, e.g. {@code /sales/orders/part.parquet}
     */
    record FileInfo(String path, long length) {
    }

    private final Object fileSystem;
    private final ClassLoader loader;
    private final Constructor<?> newPath;
    private final Method toUri;
    private final Method create;
    private final Method getFileStatus;
    private final Method rename;
    private final Method delete;
    private final Method listFiles;
    private final Method statusPath;
    private final Method length;
    private final Method isFile;
    private final Method hasNext;
    private final Method next;

    private HadoopFileSystem(Object fileSystem, Class<?> fileSystemClass) throws ReflectiveOperationException {
        this.fileSystem = fileSystem;
        this.loader = fileSystemClass.getClassLoader();
        Class<?> path = Class.forName("org.apache.hadoop.fs.Path", false, loader);
        Class<?> status = Class.forName("org.apache.hadoop.fs.FileStatus", false, loader);
        Class<?> remoteIterator = Class.forName("org.apache.hadoop.fs.RemoteIterator", false, loader);
        this.newPath = path.getConstructor(URI.class);
        this.toUri = path.getMethod("toUri");
        this.create = fileSystemClass.getMethod("create", path, boolean.class);
        this.getFileStatus = fileSystemClass.getMethod("getFileStatus", path);
        this.rename = fileSystemClass.getMethod("rename", path, path);
        this.delete = fileSystemClass.getMethod("delete", path, boolean.class);
        this.listFiles = fileSystemClass.getMethod("listFiles", path, boolean.class);
        this.statusPath = status.getMethod("getPath");
        this.length = status.getMethod("getLen");
        this.isFile = status.getMethod("isFile");
        this.hasNext = remoteIterator.getMethod("hasNext");
        this.next = remoteIterator.getMethod("next");
    }

    /**
     * The file system for {@code uri} configured by the Spark session that is active in this JVM: the one Spark
     * itself uses for that URI, from the Hadoop configuration of the session's {@code SparkContext}.
     *
     * @param sparkLoader the class loader holding Spark and the Hadoop it runs on
     */
    static HadoopFileSystem fromSparkSession(ClassLoader sparkLoader, URI uri) {
        Object fileSystem;
        try {
            // Classes initialised here capture the context class loader, as Hadoop's Configuration does.
            fileSystem = onLoader(sparkLoader, () -> {
                Class<?> sessionClass = Class.forName("org.apache.spark.sql.SparkSession", true, sparkLoader);
                Class<?> contextClass = Class.forName("org.apache.spark.SparkContext", true, sparkLoader);
                Class<?> fileSystemClass = Class.forName(FILE_SYSTEM_CLASS, true, sparkLoader);
                Class<?> configurationClass = Class.forName(CONFIGURATION_CLASS, true, sparkLoader);
                Object session = sessionClass.getMethod("active").invoke(null);
                Object context = sessionClass.getMethod("sparkContext").invoke(session);
                Object configuration = contextClass.getMethod("hadoopConfiguration").invoke(context);
                return fileSystemClass.getMethod("get", URI.class, configurationClass).invoke(null, uri, configuration);
            });
        // Not retryable: neither a missing Spark session nor a file system Hadoop cannot open comes back on its own.
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("Cannot open the target file system " + uri + " through the Spark "
                    + "session's Hadoop configuration: " + e.getCause(), e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Spark is not available on the system class path; "
                    + "migration.target.client=hadoop needs the Databricks Runtime's Spark session", e);
        }
        return wrap(fileSystem);
    }

    /** Wraps a {@code FileSystem} instance of any class loader. */
    static HadoopFileSystem wrap(Object fileSystem) {
        for (Class<?> type = fileSystem.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().equals(FILE_SYSTEM_CLASS)) {
                return wrap(fileSystem, type);
            }
        }
        throw new IllegalArgumentException(fileSystem.getClass().getName() + " is not a Hadoop FileSystem");
    }

    private static HadoopFileSystem wrap(Object fileSystem, Class<?> fileSystemClass) {
        try {
            return new HadoopFileSystem(fileSystem, fileSystemClass);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("The Hadoop FileSystem API of " + fileSystem.getClass().getName()
                    + " is not the expected one", e);
        }
    }

    /** The file system's implementation class, for messages. */
    String implementation() {
        return fileSystem.getClass().getName();
    }

    /** Creates or overwrites a file; it holds what was written once the stream is closed. */
    OutputStream create(URI file) throws IOException {
        OutputStream stream = (OutputStream) call(create, fileSystem, path(file), true);
        return new ContextLoaderStream(stream);
    }

    /**
     * The length of a file, or empty when nothing exists at the path.
     *
     * @throws IOException when the path is a folder
     */
    OptionalLong length(URI file) throws IOException {
        Object status;
        try {
            status = call(getFileStatus, fileSystem, path(file));
        } catch (FileNotFoundException e) {
            return OptionalLong.empty();
        }
        if (!(boolean) call(isFile, status)) {
            throw new IOException(file + " is a folder, not a file");
        }
        return OptionalLong.of((long) call(length, status));
    }

    /** Renames a file; false when the file system refused, e.g. because the source is gone. */
    boolean rename(URI from, URI to) throws IOException {
        return (boolean) call(rename, fileSystem, path(from), path(to));
    }

    /** Deletes a file, or a folder with everything in it when recursive; false when nothing was there. */
    boolean delete(URI path, boolean recursive) throws IOException {
        return (boolean) call(delete, fileSystem, path(path), recursive);
    }

    /** Every file under the folder, at any depth; none when the folder does not exist. */
    List<FileInfo> listFiles(URI folder) throws IOException {
        List<FileInfo> files = new ArrayList<>();
        try {
            Object iterator = call(listFiles, fileSystem, path(folder), true);
            while ((boolean) call(hasNext, iterator)) {
                Object status = call(next, iterator);
                URI file = (URI) call(toUri, call(statusPath, status));
                files.add(new FileInfo(file.getPath(), (long) call(length, status)));
            }
        } catch (FileNotFoundException e) {
            return List.of();
        }
        return files;
    }

    private Object path(URI uri) throws IOException {
        try {
            return newPath.newInstance(uri);
        } catch (InvocationTargetException e) {
            throw rethrow(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot create a Hadoop Path", e);
        }
    }

    private Object call(Method method, Object target, Object... args) throws IOException {
        try {
            return onLoader(loader, () -> method.invoke(target, args));
        } catch (InvocationTargetException e) {
            throw rethrow(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot call Hadoop's " + method, e);
        }
    }

    /** Hadoop reports storage errors as {@link IOException}s, which stay what they are; anything else is unchecked. */
    private static IOException rethrow(Throwable failure) throws IOException {
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IOException(failure);
    }

    private static <T> T onLoader(ClassLoader loader, Call<T> call) throws ReflectiveOperationException {
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return call.run();
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    @FunctionalInterface
    private interface Call<T> {
        T run() throws ReflectiveOperationException;
    }

    /** A file system's stream, written to with that file system's class loader as the context class loader. */
    private final class ContextLoaderStream extends OutputStream {

        private final OutputStream delegate;

        ContextLoaderStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            run(() -> delegate.write(b));
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            run(() -> delegate.write(bytes, offset, length));
        }

        @Override
        public void flush() throws IOException {
            run(delegate::flush);
        }

        @Override
        public void close() throws IOException {
            run(delegate::close);
        }

        private void run(StreamCall call) throws IOException {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                call.run();
            } finally {
                thread.setContextClassLoader(previous);
            }
        }
    }

    @FunctionalInterface
    private interface StreamCall {
        void run() throws IOException;
    }
}
