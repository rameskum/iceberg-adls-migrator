package com.tds.pde.migration.testing;

import com.tds.pde.migration.target.BlobStore;
import com.tds.pde.migration.target.LockUnavailableException;
import com.tds.pde.migration.target.RunLock;
import com.tds.pde.migration.target.StagedUpload;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Blob store fake with the same rules as Azure block blobs: staged bytes are invisible until a commit, a commit
 * atomically replaces any existing blob, and a lock has one holder at a time. Failures can be injected.
 */
public final class InMemoryBlobStore implements BlobStore {

    public record Blob(byte[] content, Map<String, String> metadata) {
    }

    private final Map<String, Blob> committed = new ConcurrentHashMap<>();
    private final List<String> aborted = new CopyOnWriteArrayList<>();
    private final List<String> reads = new CopyOnWriteArrayList<>();
    private final Set<String> locks = ConcurrentHashMap.newKeySet();
    private final Set<String> lostLocks = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> heldChecksBeforeLoss = new ConcurrentHashMap<>();
    private final Map<String, Integer> uploadsStarted = new ConcurrentHashMap<>();
    private volatile long failWritesAfterBytes = -1;
    private volatile Predicate<String> failWritesTo = name -> false;
    private volatile boolean failNextCommit;
    private final AtomicInteger openUploads = new AtomicInteger();
    private final AtomicInteger maxOpenUploads = new AtomicInteger();
    private volatile int holdCommitsUntilOpen;
    private volatile CountDownLatch commitGate = new CountDownLatch(0);

    /** Holds every commit until the latch is released, and fails it if that takes more than ten seconds. */
    public void holdCommitsUntil(CountDownLatch release) {
        this.commitGate = release;
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

    /** Every read request made to the store, such as {@code list sales/orders/}, in order. */
    public List<String> reads() {
        return List.copyOf(reads);
    }

    public void clearReads() {
        reads.clear();
    }

    @Override
    public StagedUpload startUpload(String blobName) {
        uploadsStarted.merge(blobName, 1, Integer::sum);
        maxOpenUploads.accumulateAndGet(openUploads.incrementAndGet(), Math::max);
        return new Upload(blobName);
    }


    @Override
    public List<BlobInfo> list(String prefix) {
        reads.add("list " + prefix);
        return committed.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> new BlobInfo(entry.getKey(), entry.getValue().content().length))
                .sorted(java.util.Comparator.comparing(BlobInfo::name))
                .toList();
    }

    @Override
    public Stream<BlobMetadata> listWithMetadata(String prefix) {
        reads.add("list with metadata " + prefix);
        return committed.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> new BlobMetadata(entry.getKey(), entry.getValue().metadata()))
                .sorted(java.util.Comparator.comparing(BlobMetadata::name))
                .toList().stream();
    }

    @Override
    public void delete(String blobName) {
        committed.remove(blobName);
    }

    @Override
    public RunLock acquireLock(String blobName) {
        if (!locks.add(blobName)) {
            throw new LockUnavailableException("Another migration run holds the lock " + blobName);
        }
        committed.putIfAbsent(blobName, new Blob(new byte[0], Map.of()));
        return new RunLock() {
            @Override
            public void ensureHeld() {
                Integer checksLeft = heldChecksBeforeLoss.computeIfPresent(blobName, (name, left) -> left - 1);
                if (lostLocks.contains(blobName) || checksLeft != null && checksLeft < 0) {
                    throw new LockUnavailableException("Lost the run lock " + blobName);
                }
            }

            @Override
            public void close() {
                locks.remove(blobName);
            }
        };
    }

    /** Simulates another run taking over: the current holder's next {@link RunLock#ensureHeld} fails. */
    public void loseLock(String blobName) {
        lostLocks.add(blobName);
    }

    /** Lets {@link RunLock#ensureHeld} pass {@code checks} more times, then fail as if another run took over. */
    public void loseLockAfter(String blobName, int checks) {
        heldChecksBeforeLoss.put(blobName, checks);
    }

    public boolean isLocked(String blobName) {
        return locks.contains(blobName);
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
            awaitCommitGate();
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
