package com.tds.pde.migration.state;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Runs SQL with named parameters ({@code :name}) against the engine holding the state tables.
 */
public interface SqlExecutor {

    /**
     * Runs a statement that changes data or schema.
     *
     * @return rows affected when the engine reports it, otherwise -1
     * @throws SqlExecutionException when the statement fails
     */
    long update(String sql, Map<String, ?> params);

    /**
     * Runs a query.
     *
     * @return one map per row, keyed by column name case-insensitively; an array value is a {@link List}
     * @throws SqlExecutionException when the query fails
     */
    List<Map<String, Object>> query(String sql, Map<String, ?> params);

    /**
     * Registers {@code rows} as a temporary view typed by {@code schema}, runs {@code body} with the view's name,
     * then drops the view. Used as the source of a {@code MERGE INTO}.
     *
     * @param schema column definitions as DDL, e.g. {@code run_id STRING, attempts INT}
     * @param rows   values in schema order: String, Integer, Long or java.sql.Timestamp, or null
     * @throws SqlExecutionException when the view cannot be created
     */
    <T> T withRows(String schema, List<Object[]> rows, Function<String, T> body);
}
