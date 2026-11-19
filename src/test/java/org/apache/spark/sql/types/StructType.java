package org.apache.spark.sql.types;

import java.util.Arrays;
import org.apache.spark.sql.AnalysisException;

/** Test stand-in for Spark's {@code StructType}; see {@code org.apache.spark.sql.SparkSession}. */
public class StructType {

    private final String[] fieldNames;

    public StructType(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    /** Reads only the field names of {@code name TYPE, ...}; fails, as Spark would, on the type UNKNOWN_TYPE. */
    public static StructType fromDDL(String ddl) throws AnalysisException {
        if (ddl.contains("UNKNOWN_TYPE")) {
            throw new AnalysisException("[UNSUPPORTED_DATATYPE] Unsupported data type \"UNKNOWN_TYPE\"", "0A000");
        }
        return new StructType(Arrays.stream(ddl.split(",")).map(field -> field.strip().split(" ")[0])
                .toArray(String[]::new));
    }

    public String[] fieldNames() {
        return fieldNames;
    }
}
