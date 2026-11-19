package com.tds.pde.migration.state;

import org.jspecify.annotations.Nullable;

/**
 * A SQL statement failed.
 *
 * <p>{@link #isRetryable()} tells whether running it again may succeed: a commit that conflicted with a concurrent
 * one, or a transient failure, may; an error that was certain from the start, such as a statement the engine
 * rejects or a missing permission, will not.
 */
public class SqlExecutionException extends RuntimeException {

    private final boolean retryable;

    public SqlExecutionException(String message, @Nullable Throwable cause, boolean retryable) {
        super(message, cause);
        this.retryable = retryable;
    }

    public boolean isRetryable() {
        return retryable;
    }
}
