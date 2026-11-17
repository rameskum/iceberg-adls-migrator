package com.tds.pde.migration.destination;

/**
 * Exclusive lock guaranteeing a single active run per destination. It renews itself while held and expires on
 * its own if the holder dies, so a crashed run never blocks the next one for long.
 */
public interface RunLock extends AutoCloseable {

    /**
     * @throws LockUnavailableException when the lock could not be renewed and another run may now hold it
     */
    void ensureHeld();

    /** Releases the lock. Never throws. */
    @Override
    void close();
}
