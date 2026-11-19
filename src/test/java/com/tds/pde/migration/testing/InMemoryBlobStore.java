package com.tds.pde.migration.testing;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.StagedUpload;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Blob store fake with the same rules as Azure block blobs: staged bytes are invisible until a commit, and a commit
 * atomically replaces any existing blob. Failures can be injected.
 */
public final class InMemoryBlobStore implements BlobStore {

    public record Blob(byte[] content, Map<String, String> metadata) {
    }

    private final Map<String, Blob> committed = new ConcurrentHashMap<>();
    private final List<String> aborted = new CopyOnWriteArrayList<>();
    private final AtomicReference<Runnable> onFirstWrite = new AtomicReference<>();
    private final Map<String, Integer> uploadsStarted = new ConcurrentHashMap<>();
    private volatile long failWritesAfterBytes = -1;
    private volatile Predicate<String> failWritesTo = name -> false;
    private volatile boolean failNextCommit;
    private final AtomicInteger openUploads = new AtomicInteger();
    private final AtomicInteger maxOpenUploads = new AtomicInteger();
    private volatile int holdCommitsUntilOpen;
    private volatile CountDownLatch commitGate = new CountDownLatch(0);

    private volatile Predicate<String> heldCommits = name -> true;

    /** Holds every commit until the latch is released, and fails it if that takes more than ten seconds. */
    public void holdCommitsUntil(CountDownLatch release) {
        holdCommitsTo(name -> true, release);
    }

    /** Holds the commits to matching blob names until the latch is released, failing them after ten seconds. */
    public void holdCommitsTo(Predicate<String> blobNames, CountDownLatch release) {
        this.heldCommits = blobNames;
        this.commitGate = release;
    }

    /** Runs {@code action} once, on the thread of the first write to any upload, before the bytes are taken. */
    public void onFirstWrite(Runnable action) {
        this.onFirstWrite.set(action);
    }

    /**
     * Holds every commit until {@code uploads} uploads have been open at the same time, and fails it if that does
     * not happen within ten seconds: only files copied in parallel get through.
     */
    public void holdCommitsUntilConcurrent(int uploads) {
        this.holdCommitsUntilOpen = uploads;
    }

    /** The most uploads that were open at the same time. */
    public int maxConcurrentUploads() {
        return maxOpenUploads.get();
    }

    /** Makes every upload throw once it has received more than {@code bytes} bytes. */
    public void failWritesAfter(long bytes) {
        this.failWritesAfterBytes = bytes;
    }

    /** Makes uploads to matching blob names throw on their first write. */
    public void failWritesTo(Predicate<String> blobNames) {
        this.failWritesTo = blobNames;
    }

    public void stopFailing() {
        this.failWritesAfterBytes = -1;
        this.failWritesTo = name -> false;
        this.failNextCommit = false;
    }

    /** How many uploads were started for the blob, committed or not. */
    public int uploadsStarted(String blobName) {
        return uploadsStarted.getOrDefault(blobName, 0);
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
        uploadsStarted.merge(blobName, 1, Integer::sum);
        maxOpenUploads.accumulateAndGet(openUploads.incrementAndGet(), Math::max);
        return new Upload(blobName);
    }


    @Override
    public List<BlobInfo> list(String prefix) {
        return committed.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> new BlobInfo(entry.getKey(), entry.getValue().content().length))
                .sorted(java.util.Comparator.comparing(BlobInfo::name))
                .toList();
    }

    @Override
    public void delete(String blobName) {
        committed.remove(blobName);
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
                    Runnable action = onFirstWrite.getAndSet(null);
                    if (action != null) {
                        action.run();
                    }
                    if (failWritesTo.test(blobName)
                            || failWritesAfterBytes >= 0 && staged.size() + len > failWritesAfterBytes) {
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
            awaitConcurrentUploads();
            if (heldCommits.test(blobName)) {
                awaitCommitGate();
            }
            committed.put(blobName, new Blob(staged.toByteArray(), Map.copyOf(metadata)));
            openUploads.decrementAndGet();
        }

        @Override
        public void abort() {
            aborted.add(blobName);
            openUploads.decrementAndGet();
        }

        private void awaitCommitGate() {
            try {
                if (!commitGate.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("commit of " + blobName + " was never released");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while holding the commit of " + blobName, e);
            }
        }

        private void awaitConcurrentUploads() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (maxOpenUploads.get() < holdCommitsUntilOpen) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("only " + maxOpenUploads.get() + " uploads were open at once");
                }
                Thread.onSpinWait();
            }
        }
    }
}
