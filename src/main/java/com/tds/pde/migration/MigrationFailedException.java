package com.tds.pde.migration;

/**
 * Thrown out of {@code main} when a migration run does not complete successfully.
 */
public class MigrationFailedException extends RuntimeException {

    public MigrationFailedException(String message) {
        super(message);
    }

    public MigrationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
