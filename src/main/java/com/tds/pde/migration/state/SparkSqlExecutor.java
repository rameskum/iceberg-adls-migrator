package com.tds.pde.migration.state;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedCaseInsensitiveMap;

/**
 * Runs SQL through the Databricks cluster's own Spark session, as the job's identity, so the state tables are
 * ordinary Unity Catalog Delta tables and no credentials are needed.
 *
 * <p>The application runs in the launcher's isolated class loader, which cannot see the Databricks Runtime's
 * classes, so Spark is reached by reflection through the JVM's system class loader, where the runtime's jars
 * live. Only the public, stable Spark API is used: {@code SparkSession.active()},
 * {@code SparkSession.sql(String, java.util.Map)}, {@code SparkSession.createDataFrame(java.util.List, StructType)},
 * {@code Dataset.collectAsList()}, {@code Dataset.createOrReplaceTempView(String)}, {@code Row.schema()},
 * {@code Row.get(int)}, {@code RowFactory.create(Object...)}, {@code StructType.fromDDL(String)} and
 * {@code StructType.fieldNames()}; an array value, a Scala {@code Seq}, is read with {@code Seq.iterator()},
 * {@code Iterator.hasNext()} and {@code Iterator.next()} into a {@link List}.
 */
@Slf4j
@Component
public class SparkSqlExecutor implements SqlExecutor {

    private static final String SESSION_CLASS = "org.apache.spark.sql.SparkSession";
    private static final String ROW_CLASS = "org.apache.spark.sql.Row";
    private static final String ROW_FACTORY_CLASS = "org.apache.spark.sql.RowFactory";
    private static final String STRUCT_TYPE_CLASS = "org.apache.spark.sql.types.StructType";
    /** What {@code Row.get} returns for an array column. */
    private static final String SEQ_CLASS = "scala.collection.Seq";
    /** Column Delta reports for UPDATE, DELETE, MERGE and INSERT. */
    private static final String AFFECTED_ROWS = "num_affected_rows";
    /** Spark's error for a statement it cannot analyse, e.g. a missing table or column; parse errors extend it. */
    private static final String ANALYSIS_EXCEPTION_CLASS = "org.apache.spark.sql.AnalysisException";
    private static final String SECURITY_EXCEPTION_CLASS = "org.apache.spark.SparkSecurityException";
    /**
     * SQLSTATE classes of errors in the statement or its data, which fail again however often they are retried:
     * 0A feature not supported, 21 cardinality violation (e.g. a MERGE source row matching two target rows),
     * 22 data exception, 23 integrity constraint violation (e.g. a NULL in a NOT NULL column), 42 syntax error or
     * access rule violation (e.g. a missing table or permission).
     */
    private static final Set<String> PERMANENT_SQL_STATE_CLASSES = Set.of("0A", "21", "22", "23", "42");

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
            return onSparkLoader(spark.loader, () -> {
                Object dataset = spark.sql.invoke(spark.session, sql, params);
                List<?> rows = (List<?>) spark.collectAsList.invoke(dataset);
                List<Map<String, Object>> result = new ArrayList<>(rows.size());
                for (Object row : rows) {
                    String[] columns = (String[]) spark.fieldNames.invoke(spark.schema.invoke(row));
                    Map<String, Object> values = new LinkedCaseInsensitiveMap<>(columns.length);
                    for (int i = 0; i < columns.length; i++) {
                        values.put(columns[i], spark.javaValue(spark.get.invoke(row, i)));
                    }
                    result.add(values);
                }
                return result;
            });
        } catch (InvocationTargetException e) {
            throw new SqlExecutionException("Spark SQL failed: " + e.getCause().getMessage(), e.getCause(),
                    isRetryable(e.getCause()));
        } catch (ReflectiveOperationException e) {
            throw new SqlExecutionException("Cannot call Spark", e, false);
        }
    }

    @Override
    public <T> T withRows(String schema, List<Object[]> rows, Function<String, T> body) {
        Bridge spark = bridge();
        String view = "migration_rows_" + UUID.randomUUID().toString().replace("-", "");
        try {
            onSparkLoader(spark.loader, () -> {
                List<Object> sparkRows = new ArrayList<>(rows.size());
                for (Object[] row : rows) {
                    sparkRows.add(spark.createRow.invoke(null, (Object) row));
                }
                Object dataset = spark.createDataFrame.invoke(spark.session, sparkRows,
                        spark.fromDdl.invoke(null, schema));
                return spark.createOrReplaceTempView.invoke(dataset, view);
            });
        } catch (InvocationTargetException e) {
            throw new SqlExecutionException("Spark cannot create the rows to merge: " + e.getCause().getMessage(),
                    e.getCause(), isRetryable(e.getCause()));
        } catch (ReflectiveOperationException e) {
            throw new SqlExecutionException("Cannot call Spark", e, false);
        }
        try {
            return body.apply(view);
        } finally {
            // A view left behind only lives as long as the Spark session, so it must not hide the body's outcome.
            try {
                update("DROP VIEW IF EXISTS " + view, Map.of());
            } catch (SqlExecutionException e) {
                log.warn("Cannot drop temporary view: view={} reason={}", view, e.getMessage());
            }
        }
    }

    /**
     * Whether running a failed statement again may succeed. A Delta commit conflict always may: Delta's conflict
     * exceptions extend {@link ConcurrentModificationException}. An error that was certain from the start never
     * does: the engine rejected the statement ({@code AnalysisException}, which includes parse errors), the job's
     * identity lacks a permission, or the SQLSTATE that Spark gives every classified error says the statement or its
     * data is at fault. Any other failure, such as a lost executor or a storage timeout, is taken as transient.
     */
    static boolean isRetryable(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConcurrentModificationException) {
                return true;
            }
        }
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SecurityException || isA(cause, ANALYSIS_EXCEPTION_CLASS)
                    || isA(cause, SECURITY_EXCEPTION_CLASS)) {
                return false;
            }
            String sqlState = sqlState(cause);
            if (sqlState != null && sqlState.length() >= 2
                    && PERMANENT_SQL_STATE_CLASSES.contains(sqlState.substring(0, 2))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isA(Throwable failure, String className) {
        for (Class<?> type = failure.getClass(); type != null; type = type.getSuperclass()) {
            if (type.getName().equals(className)) {
                return true;
            }
        }
        return false;
    }

    /** The SQLSTATE of a Spark error ({@code SparkThrowable.getSqlState()}), or null when it has none. */
    private static @Nullable String sqlState(Throwable failure) {
        try {
            Method getSqlState = failure.getClass().getMethod("getSqlState");
            if (getSqlState.getReturnType() != String.class || !getSqlState.trySetAccessible()) {
                return null;
            }
            return (String) getSqlState.invoke(failure);
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
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

    /**
     * Runs {@code call} with Spark's class loader as the thread's context class loader. The launcher sets the
     * application's isolated loader there, which cannot see Spark, yet Spark and Hadoop load their own classes and
     * services through it; Hadoop even caches a failed FileSystem lookup for the life of the JVM.
     */
    private static <T> T onSparkLoader(ClassLoader loader, SparkCall<T> call) throws ReflectiveOperationException {
        Thread thread = Thread.currentThread();
        ClassLoader applicationLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return call.run();
        } finally {
            thread.setContextClassLoader(applicationLoader);
        }
    }

    @FunctionalInterface
    private interface SparkCall<T> {
        T run() throws ReflectiveOperationException;
    }

    /** The resolved Spark session and the methods called on it, its datasets and rows. */
    private record Bridge(ClassLoader loader, Object session, Method sql, Method collectAsList, Method schema,
                          Method get, Method fieldNames, Method createRow, Method fromDdl, Method createDataFrame,
                          Method createOrReplaceTempView, Class<?> seq, Method iterator, Method hasNext,
                          Method next) {

        Object javaValue(Object value) throws ReflectiveOperationException {
            if (!seq.isInstance(value)) {
                return value;
            }
            List<Object> elements = new ArrayList<>();
            Object elementIterator = iterator.invoke(value);
            while ((boolean) hasNext.invoke(elementIterator)) {
                elements.add(javaValue(next.invoke(elementIterator)));
            }
            return elements;
        }

        static Bridge resolve(ClassLoader loader) {
            try {
                Class<?> sessionClass = Class.forName(SESSION_CLASS, true, loader);
                Object session = onSparkLoader(loader, () -> sessionClass.getMethod("active").invoke(null));
                Method sql = sessionClass.getMethod("sql", String.class, Map.class);
                Method collectAsList = sql.getReturnType().getMethod("collectAsList");
                Class<?> rowClass = Class.forName(ROW_CLASS, true, loader);
                Class<?> structType = Class.forName(STRUCT_TYPE_CLASS, true, loader);
                Method createDataFrame = sessionClass.getMethod("createDataFrame", List.class, structType);
                Class<?> seq = Class.forName(SEQ_CLASS, true, loader);
                Method iterator = seq.getMethod("iterator");
                log.info("Spark session found for the state tables: sparkVersion={}",
                        sessionClass.getMethod("version").invoke(session));
                return new Bridge(loader, session, sql, collectAsList, rowClass.getMethod("schema"),
                        rowClass.getMethod("get", int.class), structType.getMethod("fieldNames"),
                        Class.forName(ROW_FACTORY_CLASS, true, loader).getMethod("create", Object[].class),
                        structType.getMethod("fromDDL", String.class), createDataFrame,
                        createDataFrame.getReturnType().getMethod("createOrReplaceTempView", String.class), seq,
                        iterator, iterator.getReturnType().getMethod("hasNext"),
                        iterator.getReturnType().getMethod("next"));
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
