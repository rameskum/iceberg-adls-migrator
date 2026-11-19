package org.apache.spark;

import org.jspecify.annotations.Nullable;

/**
 * Test stand-in for Spark's {@code SparkException}: a classified error carries a SQLSTATE, an unclassified one, such
 * as a job that failed because an executor was lost, has none.
 */
public class SparkException extends Exception {

    private final @Nullable String sqlState;

    public SparkException(String message, @Nullable String sqlState, @Nullable Throwable cause) {
        super(message, cause);
        this.sqlState = sqlState;
    }

    public @Nullable String getSqlState() {
        return sqlState;
    }
}
