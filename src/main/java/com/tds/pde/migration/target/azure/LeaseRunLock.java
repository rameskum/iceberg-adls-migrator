package com.tds.pde.migration.target.azure;

import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.tds.pde.migration.target.LockUnavailableException;
import com.tds.pde.migration.target.RunLock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * A blob lease renewed in the background. If the process dies the lease lapses within {@link #LEASE_DURATION}.
 */
@Slf4j
final class LeaseRunLock implements RunLock {

    static final Duration LEASE_DURATION = Duration.ofSeconds(60);
    /** A third of the lease, so two renewals can fail transiently before the lease lapses. */
    static final Duration RENEW_INTERVAL = Duration.ofSeconds(20);

    private final BlobLeaseClient lease;
    private final String blobName;
    private final ScheduledExecutorService renewer;
    private volatile Exception renewalFailure;

    LeaseRunLock(BlobLeaseClient lease, String blobName, Duration renewInterval) {
        this.lease = lease;
        this.blobName = blobName;
        this.renewer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "lock-renewal");
            thread.setDaemon(true);
            return thread;
        });
        long millis = renewInterval.toMillis();
        renewer.scheduleWithFixedDelay(this::renew, millis, millis, TimeUnit.MILLISECONDS);
    }

    private void renew() {
        try {
            lease.renewLease();
        } catch (RuntimeException e) {
            if (renewalFailure == null) {
                log.error("Failed to renew the run lock; another run may take it over: lock={} reason={}",
                        blobName, e.getMessage(), e);
            }
            renewalFailure = e;
        }
    }

    @Override
    public void ensureHeld() {
        Exception failure = renewalFailure;
        if (failure != null) {
            throw new LockUnavailableException("Lost the run lock " + blobName, failure);
        }
    }

    @Override
    public void close() {
        renewer.shutdownNow();
        try {
            if (!renewer.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Run lock renewal did not stop in time: lock={}", blobName);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            lease.releaseLease();
            log.info("Run lock released: lock={}", blobName);
        } catch (RuntimeException e) {
            log.warn("Failed to release the run lock; it expires within {}: lock={} reason={}",
                    LEASE_DURATION, blobName, e.getMessage());
        }
    }
}
