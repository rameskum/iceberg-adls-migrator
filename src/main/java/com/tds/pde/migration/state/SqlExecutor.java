package com.tds.pde.migration.state;

import java.util.List;
import java.util.Map;

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
     * @return one map per row, keyed by column name case-insensitively
     * @throws SqlExecutionException when the query fails
     */
    List<Map<String, Object>> query(String sql, Map<String, ?> params);
}
