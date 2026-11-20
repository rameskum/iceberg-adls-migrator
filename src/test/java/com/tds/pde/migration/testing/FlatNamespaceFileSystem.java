package com.tds.pde.migration.testing;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Hadoop file system fake with the rules of Apache's ABFS driver on a storage account without hierarchical namespace,
 * held in memory per account and container: files are blobs under flat names; a create makes a zero-length blob
 * at once, marks its parent folders with marker blobs, and puts the content when the stream is closed; a rename of a
 * file copies it and then deletes the source, and returns false when the destination exists or its folder does not.
 * Failures can be injected. Tests run one at a time, so the store is shared.
 */
public class FlatNamespaceFileSystem extends FileSystem {

    /** Blob content by authority, then by path (such as {@code /sales/orders/part.parquet}). */
    private static final Map<String, NavigableMap<String, byte[]>> FILES = new ConcurrentSkipListMap<>();
    /** Folder marker blobs by authority. */
    private static final Map<String, NavigableSet<String>> FOLDERS = new ConcurrentSkipListMap<>();
    private static volatile Predicate<String> refusedRenames = path -> false;
    private static volatile Predicate<String> failingCopyDeletes = path -> false;
    private static volatile Predicate<String> droppedLastBytes = path -> false;
    private static volatile Predicate<String> failingCreates = path -> false;

    private URI uri;

    /** Forgets every file and folder and stops injecting failures. */
    public static void reset() {
        FILES.clear();
        FOLDERS.clear();
        stopFailing();
    }

    public static void stopFailing() {
        refusedRenames = path -> false;
        failingCopyDeletes = path -> false;
        droppedLastBytes = path -> false;
        failingCreates = path -> false;
    }

    /** Files of the container, by path, in name order. */
    public static NavigableMap<String, byte[]> files(String authority) {
        return new TreeMap<>(files0(authority));
    }

    /** Folder marker blobs of the container, in name order. */
    public static NavigableSet<String> folders(String authority) {
        return new java.util.TreeSet<>(folders0(authority));
    }

    /** Puts a file as a create would, e.g. one an earlier run left behind. */
    public static void put(String authority, String path, byte[] content) {
        markParents(authority, path);
        files0(authority).put(path, content);
    }

    /** Makes renames whose destination matches return false, as when the service refuses them. */
    public static void refuseRenamesTo(Predicate<String> destinations) {
        refusedRenames = destinations;
    }

    /** Makes renames whose destination matches fail after the copy, before the source is deleted, as a crash would. */
    public static void failRenamesAfterCopyTo(Predicate<String> destinations) {
        failingCopyDeletes = destinations;
    }

    /** Makes files whose path matches lose their last byte when their stream is closed. */
    public static void dropLastByteOf(Predicate<String> paths) {
        droppedLastBytes = paths;
    }

    /** Makes creates of matching paths fail. */
    public static void failCreatesOf(Predicate<String> paths) {
        failingCreates = paths;
    }

    @Override
    public void initialize(URI name, Configuration configuration) throws IOException {
        super.initialize(name, configuration);
        uri = URI.create(name.getScheme() + "://" + name.getAuthority());
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public String getScheme() {
        return uri.getScheme();
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) {
        throw new UnsupportedOperationException("not needed by the tests");
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        String file = key(path);
        if (failingCreates.test(file)) {
            throw new IOException("injected create failure for " + file);
        }
        if (folders().contains(file)) {
            throw new IOException(file + " is a folder");
        }
        if (!overwrite && files().containsKey(file)) {
            throw new IOException(file + " already exists");
        }
        markParents(authority(), file);
        files().put(file, new byte[0]);
        ByteArrayOutputStream content = new ByteArrayOutputStream();
        return new FSDataOutputStream(content, statistics) {
            private boolean closed;

            @Override
            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                super.close();
                byte[] bytes = content.toByteArray();
                if (droppedLastBytes.test(file) && bytes.length > 0) {
                    bytes = java.util.Arrays.copyOf(bytes, bytes.length - 1);
                }
                files().put(file, bytes);
            }
        };
    }

    @Override
    public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) {
        throw new UnsupportedOperationException("not needed by the tests");
    }

    @Override
    public boolean rename(Path source, Path destination) throws IOException {
        String from = key(source);
        String to = key(destination);
        byte[] content = files().get(from);
        if (content == null) {
            return false;
        }
        if (isFolder(to)) {
            to = to + "/" + source.getName();
        }
        if (files().containsKey(to) || !isFolder(parent(to)) || refusedRenames.test(to)) {
            return false;
        }
        files().put(to, content);
        if (failingCopyDeletes.test(to)) {
            throw new IOException("injected failure after copying " + from + " to " + to);
        }
        files().remove(from);
        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive) throws IOException {
        String target = key(path);
        if (files().remove(target) != null) {
            return true;
        }
        if (!isFolder(target)) {
            return false;
        }
        String within = target.equals("/") ? "/" : target + "/";
        Set<String> filesWithin = files().subMap(within, within + Character.MAX_VALUE).keySet();
        Set<String> foldersWithin = folders().subSet(within, within + Character.MAX_VALUE);
        if (!recursive && (!filesWithin.isEmpty() || !foldersWithin.isEmpty())) {
            throw new IOException(target + " is not empty");
        }
        filesWithin.clear();
        foldersWithin.clear();
        folders().remove(target);
        return true;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        String target = key(path);
        if (files().containsKey(target)) {
            return new FileStatus[] {getFileStatus(path)};
        }
        if (!isFolder(target)) {
            throw new FileNotFoundException(target);
        }
        String within = target.equals("/") ? "/" : target + "/";
        Set<String> children = new java.util.TreeSet<>();
        files().subMap(within, within + Character.MAX_VALUE).keySet().forEach(file -> children.add(child(within, file)));
        folders().subSet(within, within + Character.MAX_VALUE).forEach(folder -> children.add(child(within, folder)));
        List<FileStatus> statuses = new ArrayList<>();
        for (String child : children) {
            statuses.add(getFileStatus(new Path(uri.getScheme(), uri.getAuthority(), child)));
        }
        return statuses.toArray(FileStatus[]::new);
    }

    @Override
    public void setWorkingDirectory(Path folder) {
        throw new UnsupportedOperationException("not needed by the tests");
    }

    @Override
    public Path getWorkingDirectory() {
        return new Path(uri.getScheme(), uri.getAuthority(), "/");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission permission) {
        String folder = key(path);
        markParents(authority(), folder + "/x");
        return true;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        String target = key(path);
        Path qualified = new Path(uri.getScheme(), uri.getAuthority(), target);
        byte[] content = files().get(target);
        if (content != null) {
            return new FileStatus(content.length, false, 1, 4096, 0, qualified);
        }
        if (isFolder(target)) {
            return new FileStatus(0, true, 1, 4096, 0, qualified);
        }
        throw new FileNotFoundException(target);
    }

    /** A folder exists when its marker does, or any blob is under it. */
    private boolean isFolder(String path) {
        if (path.equals("/") || folders().contains(path)) {
            return true;
        }
        String within = path + "/";
        return !files().subMap(within, within + Character.MAX_VALUE).isEmpty();
    }

    private static String child(String within, String descendant) {
        int slash = descendant.indexOf('/', within.length());
        return slash < 0 ? descendant : descendant.substring(0, slash);
    }

    private static String parent(String path) {
        int slash = path.lastIndexOf('/');
        return slash <= 0 ? "/" : path.substring(0, slash);
    }

    private static void markParents(String authority, String path) {
        for (String folder = parent(path); !folder.equals("/"); folder = parent(folder)) {
            folders0(authority).add(folder);
        }
    }

    private String key(Path path) {
        URI qualified = makeQualified(path).toUri();
        if (!uri.getAuthority().equals(qualified.getAuthority())) {
            throw new IllegalArgumentException("Wrong file system: " + path + ", expected " + uri);
        }
        String key = qualified.getPath();
        return key.length() > 1 && key.endsWith("/") ? key.substring(0, key.length() - 1) : key;
    }

    private String authority() {
        return uri.getAuthority();
    }

    private NavigableMap<String, byte[]> files() {
        return files0(authority());
    }

    private NavigableSet<String> folders() {
        return folders0(authority());
    }

    private static NavigableMap<String, byte[]> files0(String authority) {
        return FILES.computeIfAbsent(authority, key -> new ConcurrentSkipListMap<>());
    }

    private static NavigableSet<String> folders0(String authority) {
        return FOLDERS.computeIfAbsent(authority, key -> new ConcurrentSkipListSet<>());
    }
}
