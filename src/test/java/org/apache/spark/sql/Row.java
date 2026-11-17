package org.apache.spark.sql;

import org.apache.spark.sql.types.StructType;

/** Test stand-in for Spark's {@code Row}; see {@link SparkSession}. */
public class Row {

    private final StructType schema;
    private final Object[] values;

    public Row(String[] columns, Object... values) {
        this.schema = new StructType(columns);
        this.values = values;
    }

    public StructType schema() {
        return schema;
    }

    public Object get(int index) {
        return values[index];
    }
}
