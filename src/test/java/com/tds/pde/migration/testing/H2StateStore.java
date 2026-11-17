package com.tds.pde.migration.testing;

import com.tds.pde.migration.state.SqlExecutionException;
import com.tds.pde.migration.state.SqlExecutor;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * A private in-memory H2 database holding the state tables (named as in {@link TestProperties}), and a
 * {@link SqlExecutor} running the production SQL on it in place of Spark.
 */
public final class H2StateStore {

    public static final String CONTROL_TABLE = "migration_control";
    public static final String RUN_TABLE = "migration_run";
    public static final String FILE_TABLE = "migration_file";

    private final DataSource dataSource = new DriverManagerDataSource(
            "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
    private final NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(dataSource);

    public H2StateStore() {
        String script;
        try {
            script = new ClassPathResource("db/h2/migration-state.sql").getContentAsString(StandardCharsets.UTF_8)
                    .replace("${control_table}", CONTROL_TABLE)
                    .replace("${run_table}", RUN_TABLE)
                    .replace("${file_table}", FILE_TABLE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Arrays.stream(script.replaceAll("(?m)^\\s*--.*$", "").split(";"))
                .map(String::strip)
                .filter(statement -> !statement.isEmpty())
                .forEach(statement -> jdbc.getJdbcOperations().execute(statement));
    }

    /** For inspecting or tampering with state directly in tests. */
    public JdbcClient jdbc() {
        return JdbcClient.create(dataSource);
    }

    public SqlExecutor executor() {
        return new SqlExecutor() {
            @Override
            public long update(String sql, Map<String, ?> params) {
                // The tables already exist in H2; the production DDL is Delta-specific.
                if (sql.strip().toUpperCase(Locale.ROOT).startsWith("CREATE TABLE")) {
                    return 0;
                }
                try {
                    return jdbc.update(sql, params);
                } catch (DataAccessException e) {
                    throw new SqlExecutionException(e.getMessage(), e);
                }
            }

            @Override
            public List<Map<String, Object>> query(String sql, Map<String, ?> params) {
                try {
                    return jdbc.queryForList(sql, params);
                } catch (DataAccessException e) {
                    throw new SqlExecutionException(e.getMessage(), e);
                }
            }
        };
    }
}
