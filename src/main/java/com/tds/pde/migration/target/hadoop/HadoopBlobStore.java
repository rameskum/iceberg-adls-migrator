package com.tds.pde.migration.target.hadoop;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.StagedUpload;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.List;

/**
 * {@link BlobStore} on a Hadoop file system, such as the one Spark uses for {@code abfss://}. It needs no hierarchical
 * namespace: a file is written under a temporary name, checked, and renamed to its final name, and on a flat namespace
 * a rename is a server-side copy and delete of one file.
 *
 * <p>Temporary files live in {@code <folder>/_migration/tmp/<runId>/}, next to the files they become. Spark and
 * Hadoop skip folders whose names start with {@code _}, so readers of the target never see them.
 */
public final class HadoopBlobStore implements BlobStore {

    /** The folder, within the target folder, of everything the migration keeps there besides the files. */
    static final String MIGRATION_FOLDER = "_migration";
    private static final String TEMPORARY_FOLDER = MIGRATION_FOLDER + "/tmp/";

    private final HadoopFileSystem fileSystem;
    private final URI root;
    private final String rootPath;

    /**
     * @param root the file system URI under which blob names are paths, such as
     *             {@code abfss://<container>@<account>.dfs.core.windows.net}
     */
    HadoopBlobStore(HadoopFileSystem fileSystem, URI root) {
        this.fileSystem = fileSystem;
        this.root = root;
        this.rootPath = root.getPath() == null ? "" : root.getPath().replaceAll("/+$", "");
    }

    @Override
    public StagedUpload startUpload(String blobName, String runId) {
        int slash = blobName.lastIndexOf('/');
        String temporaryName = blobName.substring(0, slash + 1) + TEMPORARY_FOLDER + runId + "/"
                + blobName.substring(slash + 1);
        return new HadoopStagedUpload(fileSystem, blobName, uri(blobName), uri(temporaryName));
    }

    @Override
    public List<BlobInfo> list(String prefix) {
        URI folder = uri(prefix.substring(0, prefix.lastIndexOf('/') + 1));
        try {
            return fileSystem.listFiles(folder).stream()
                    .map(file -> new BlobInfo(name(file.path()), file.length()))
                    .filter(file -> file.name().startsWith(prefix))
                    .sorted(Comparator.comparing(BlobInfo::name))
                    .toList();
        } catch (IOException e) {
            // The first request of a run: turn configuration and permission problems into a readable message.
            throw new IllegalStateException("Cannot list " + folder + " through " + fileSystem.implementation()
                    + ": " + e, e);
        }
    }

    @Override
    public void delete(String blobName) {
        try {
            fileSystem.delete(uri(blobName), false);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot delete " + uri(blobName) + ": " + e.getMessage(), e);
        }
    }

    /** Deletes the folder that held the temporary files, along with any empty folders left in it. */
    @Override
    public void deleteUploadLeftovers(String prefix) {
        URI migrationFolder = uri(prefix + MIGRATION_FOLDER);
        try {
            fileSystem.delete(migrationFolder, true);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot delete " + migrationFolder + ": " + e.getMessage(), e);
        }
    }

    private URI uri(String blobName) {
        try {
            return new URI(root.getScheme(), root.getAuthority(), rootPath + "/" + blobName, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid blob name '" + blobName + "'", e);
        }
    }

    private String name(String path) {
        if (!path.startsWith(rootPath + "/")) {
            throw new IllegalStateException("Listed file " + path + " is not under " + root);
        }
        return path.substring(rootPath.length() + 1);
    }
}
