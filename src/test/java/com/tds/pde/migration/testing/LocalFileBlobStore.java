package com.tds.pde.migration.testing;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.RunLock;
import com.tds.pde.migration.target.StagedUpload;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Blob store on local disk for data too large to keep in memory: uploads stage into a temporary file that a
 * commit moves into place atomically. Supports uploads and listing metadata only.
 */
public final class LocalFileBlobStore implements BlobStore {

    private final Path root;
    private final Map<String, Map<String, String>> metadata = new ConcurrentHashMap<>();

    public LocalFileBlobStore(Path root) {
        this.root = root;
    }

    public Path file(String blobName) {
        return root.resolve(blobName);
    }

    @Override
    public StagedUpload startUpload(String blobName) {
        Path staged;
        try {
            Files.createDirectories(root);
            staged = Files.createTempFile(root, "staged-", ".tmp");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new StagedUpload() {
            @Override
            public String blobName() {
                return blobName;
            }

            @Override
            public OutputStream outputStream() {
                try {
                    return Files.newOutputStream(staged);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void commit(Map<String, String> blobMetadata) {
                try {
                    Path target = file(blobName);
                    Files.createDirectories(target.getParent());
                    Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                metadata.put(blobName, Map.copyOf(blobMetadata));
            }

            @Override
            public void abort() {
                try {
                    Files.deleteIfExists(staged);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    @Override
    public Stream<BlobMetadata> listWithMetadata(String prefix) {
        return metadata.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> new BlobMetadata(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(BlobMetadata::name))
                .toList().stream();
    }

    @Override
    public List<BlobInfo> list(String prefix) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(String blobName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RunLock acquireLock(String blobName) {
        throw new UnsupportedOperationException();
    }
}
