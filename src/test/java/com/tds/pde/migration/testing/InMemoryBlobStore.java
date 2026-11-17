package com.tds.pde.migration.testing;

import com.tds.pde.migration.destination.BlobStore;
import com.tds.pde.migration.destination.StagedUpload;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Blob store fake with the same visibility rules as Azure block blobs: staged bytes are invisible until a commit,
 * and a commit atomically replaces any existing blob. Failures can be injected at write or commit time.
 */
public final class InMemoryBlobStore implements BlobStore {

    public record Blob(byte[] content, Map<String, String> metadata) {
    }

    private final Map<String, Blob> committed = new ConcurrentHashMap<>();
    private final List<String> aborted = new CopyOnWriteArrayList<>();
    private volatile long failWritesAfterBytes = -1;
    private volatile boolean failNextCommit;

    /** Makes every upload throw once it has received more than {@code bytes} bytes. */
    public void failWritesAfter(long bytes) {
        this.failWritesAfterBytes = bytes;
    }

    public void failNextCommit() {
        this.failNextCommit = true;
    }

    public Optional<Blob> blob(String blobName) {
        return Optional.ofNullable(committed.get(blobName));
    }

    public Map<String, Blob> blobs() {
        return Map.copyOf(committed);
    }

    public List<String> aborted() {
        return List.copyOf(aborted);
    }

    @Override
    public StagedUpload startUpload(String blobName) {
        return new Upload(blobName);
    }

    private final class Upload implements StagedUpload {

        private final String blobName;
        private final ByteArrayOutputStream staged = new ByteArrayOutputStream();
        private boolean streamOpened;
        private boolean streamClosed;

        Upload(String blobName) {
            this.blobName = blobName;
        }

        @Override
        public String blobName() {
            return blobName;
        }

        @Override
        public OutputStream outputStream() {
            if (streamOpened) {
                throw new IllegalStateException("output stream already opened for " + blobName);
            }
            streamOpened = true;
            return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    write(new byte[] {(byte) b}, 0, 1);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    if (failWritesAfterBytes >= 0 && staged.size() + len > failWritesAfterBytes) {
                        throw new IOException("injected write failure for " + blobName);
                    }
                    staged.write(b, off, len);
                }

                @Override
                public void close() {
                    streamClosed = true;
                }
            };
        }

        @Override
        public void commit(Map<String, String> metadata) {
            if (!streamClosed) {
                throw new IllegalStateException("commit before the output stream was closed: " + blobName);
            }
            if (failNextCommit) {
                failNextCommit = false;
                throw new UncheckedIOException(new IOException("injected commit failure for " + blobName));
            }
            committed.put(blobName, new Blob(staged.toByteArray(), Map.copyOf(metadata)));
        }

        @Override
        public void abort() {
            aborted.add(blobName);
        }
    }
}
