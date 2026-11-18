package org.apache.spark.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    public Dataset createDataFrame(List<Row> rows, StructType schema) {
        return new Dataset(rows.stream().map(row -> new Row(schema.fieldNames(), row.values())).toList());
    }

    public Dataset sql(String sqlText, Map<String, Object> args) {
        if (sqlText.contains("FAIL")) {
            throw new IllegalStateException("[TABLE_OR_VIEW_NOT_FOUND] The table cannot be found");
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
