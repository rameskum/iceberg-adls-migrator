package org.apache.spark.sql;

import io.delta.exceptions.ConcurrentAppendException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.sql.types.StructType;

/**
 * Test stand-in with the shape of the Spark API that {@code SparkSqlExecutor} calls by reflection. Not Spark.
 */
public class SparkSession {

    public static List<Row> nextResult = List.of();
    public static final List<String> statements = new ArrayList<>();
    public static final List<Map<String, Object>> parameters = new ArrayList<>();
    public static final Map<String, List<Row>> views = new LinkedHashMap<>();
    public static final List<ClassLoader> contextLoaders = new ArrayList<>();

    private static final SparkSession ACTIVE = new SparkSession();

    public static SparkSession active() {
        return ACTIVE;
    }

    public String version() {
        return "test";
    }

    public SparkContext sparkContext() {
        return new SparkContext();
    }

    public Dataset createDataFrame(List<Row> rows, StructType schema) {
        return new Dataset(rows.stream().map(row -> new Row(schema.fieldNames(), row.values())).toList());
    }

    /**
     * Fails as Spark would when the text names a failure: {@code FAIL} (a missing table), {@code DENIED} (a missing
     * permission), {@code CONFLICT} (a Delta commit conflict) or {@code LOST} (a job that lost its executor).
     */
    public Dataset sql(String sqlText, Map<String, Object> args) throws Exception {
        if (sqlText.contains("FAIL")) {
            throw new AnalysisException("[TABLE_OR_VIEW_NOT_FOUND] The table cannot be found", "42P01");
        }
        if (sqlText.contains("DENIED")) {
            throw new SparkException("[INSUFFICIENT_PERMISSIONS] User does not have MODIFY on table", "42501",
                    null);
        }
        if (sqlText.contains("CONFLICT")) {
            throw new ConcurrentAppendException("[DELTA_CONCURRENT_APPEND] Files were added by a concurrent update");
        }
        if (sqlText.contains("LOST")) {
            throw new SparkException("Job aborted due to stage failure: ExecutorLostFailure", null,
                    new IOException("Connection reset by peer"));
        }
        statements.add(sqlText);
        contextLoaders.add(Thread.currentThread().getContextClassLoader());
        if (sqlText.startsWith("DROP VIEW IF EXISTS ")) {
            views.remove(sqlText.substring("DROP VIEW IF EXISTS ".length()));
        }
        parameters.add(args);
        return new Dataset(nextResult);
    }
}
