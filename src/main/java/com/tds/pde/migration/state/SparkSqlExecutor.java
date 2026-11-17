package com.tds.pde.migration.state;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * Runs SQL through the Databricks cluster's own Spark session, as the job's identity, so the state tables are
 * ordinary Unity Catalog Delta tables and no credentials are needed.
 *
 * <p>The application runs in the launcher's isolated class loader, which cannot see the Databricks Runtime's
 * classes, so Spark is reached by reflection through the JVM's system class loader, where the runtime's jars
 * live. Only the public, stable Spark API is used: {@code SparkSession.active()},
 * {@code SparkSession.sql(String, java.util.Map)}, {@code Dataset.collectAsList()}, {@code Row.schema()},
 * {@code Row.get(int)} and {@code StructType.fieldNames()}.
 */
@Slf4j
@Component
public class SparkSqlExecutor implements SqlExecutor {

    private static final String SESSION_CLASS = "org.apache.spark.sql.SparkSession";
    private static final String ROW_CLASS = "org.apache.spark.sql.Row";
    private static final String STRUCT_TYPE_CLASS = "org.apache.spark.sql.types.StructType";
    /** Column Delta reports for UPDATE, DELETE, MERGE and INSERT. */
    private static final String AFFECTED_ROWS = "num_affected_rows";

    private final Supplier<ClassLoader> sparkClassLoader;
    private volatile Bridge bridge;

    public SparkSqlExecutor() {
        this(ClassLoader::getSystemClassLoader);
    }

    SparkSqlExecutor(Supplier<ClassLoader> sparkClassLoader) {
        this.sparkClassLoader = sparkClassLoader;
    }

    @Override
    public long update(String sql, Map<String, ?> params) {
        for (Map<String, Object> row : query(sql, params)) {
            Object affected = row.get(AFFECTED_ROWS);
            if (affected instanceof Number number) {
                return number.longValue();
            }
        }
        return -1;
    }

    @Override
    public List<Map<String, Object>> query(String sql, Map<String, ?> params) {
        Bridge spark = bridge();
        try {
            Object dataset = spark.sql.invoke(spark.session, sql, params);
            List<?> rows = (List<?>) spark.collectAsList.invoke(dataset);
            List<Map<String, Object>> result = new ArrayList<>(rows.size());
            for (Object row : rows) {
                String[] columns = (String[]) spark.fieldNames.invoke(spark.schema.invoke(row));
                Map<String, Object> values = new LinkedCaseInsensitiveMap<>(columns.length);
                for (int i = 0; i < columns.length; i++) {
                    values.put(columns[i], spark.get.invoke(row, i));
                }
                result.add(values);
            }
            return result;
        } catch (InvocationTargetException e) {
            throw new SqlExecutionException("Spark SQL failed: " + e.getCause().getMessage(), e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new SqlExecutionException("Cannot call Spark", e);
        }
    }

    private Bridge bridge() {
        Bridge resolved = bridge;
        if (resolved == null) {
            synchronized (this) {
                resolved = bridge;
                if (resolved == null) {
                    resolved = Bridge.resolve(sparkClassLoader.get());
                    bridge = resolved;
                }
            }
        }
        return resolved;
    }

    /** The resolved Spark session and the methods called on it, its datasets and rows. */
    private record Bridge(Object session, Method sql, Method collectAsList, Method schema, Method get,
                          Method fieldNames) {

        static Bridge resolve(ClassLoader loader) {
            try {
                Class<?> sessionClass = Class.forName(SESSION_CLASS, true, loader);
                Object session = sessionClass.getMethod("active").invoke(null);
                Method sql = sessionClass.getMethod("sql", String.class, Map.class);
                Method collectAsList = sql.getReturnType().getMethod("collectAsList");
                Class<?> rowClass = Class.forName(ROW_CLASS, true, loader);
                Class<?> structType = Class.forName(STRUCT_TYPE_CLASS, true, loader);
                log.info("Spark session found for the state tables: sparkVersion={}",
                        sessionClass.getMethod("version").invoke(session));
                return new Bridge(session, sql, collectAsList, rowClass.getMethod("schema"),
                        rowClass.getMethod("get", int.class), structType.getMethod("fieldNames"));
            // Not SqlExecutionException: retrying cannot make Spark appear.
            } catch (InvocationTargetException e) {
                throw new IllegalStateException("No active Spark session for the state tables: "
                        + e.getCause().getMessage(), e.getCause());
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Spark is not available on the system class path; the state tables "
                        + "need the Databricks Runtime's Spark session", e);
            }
        }
    }
}
