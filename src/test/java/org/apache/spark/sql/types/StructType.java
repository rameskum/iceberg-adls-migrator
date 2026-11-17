package org.apache.spark.sql.types;

/** Test stand-in for Spark's {@code StructType}; see {@code org.apache.spark.sql.SparkSession}. */
public class StructType {

    private final String[] fieldNames;

    public StructType(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public String[] fieldNames() {
        return fieldNames;
    }
}
