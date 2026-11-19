package org.apache.spark.sql;

/** Test stand-in for Spark's {@code AnalysisException}, with its SQLSTATE; see {@link SparkSession}. */
public class AnalysisException extends Exception {

    private final String sqlState;

    public AnalysisException(String message, String sqlState) {
        super(message);
        this.sqlState = sqlState;
    }

    public String getSqlState() {
        return sqlState;
    }
}
