package org.apache.spark.sql;

/** Test stand-in for Spark's {@code RowFactory}; see {@link SparkSession}. */
public class RowFactory {

    public static Row create(Object... values) {
        return new Row(new String[values.length], values);
    }
}
