package org.apache.spark.sql;

import java.util.List;

/** Test stand-in for Spark's {@code Dataset}; see {@link SparkSession}. */
public class Dataset {

    private final List<Row> rows;

    Dataset(List<Row> rows) {
        this.rows = rows;
    }

    public List<Row> collectAsList() {
        return rows;
    }

    public void createOrReplaceTempView(String viewName) {
        SparkSession.views.put(viewName, rows);
    }
}
