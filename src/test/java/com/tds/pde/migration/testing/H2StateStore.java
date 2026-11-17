package com.tds.pde.migration.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** A private in-memory H2 database with the state tables, named as in {@link TestProperties}. */
public final class H2StateStore {

    public static final String RUN_TABLE = "migration_run";
    public static final String FILE_TABLE = "migration_file";

    private final String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
    private final DataSource dataSource;

    public H2StateStore() {
        this.dataSource = new DriverManagerDataSource(url, "sa", "");
        createTables(JdbcClient.create(dataSource));
    }

    /** JDBC URL of this database, for an application context to connect to the same tables. */
    public String url() {
        return url;
    }

    public DataSource dataSource() {
        return dataSource;
    }

    public JdbcClient jdbc() {
        return JdbcClient.create(dataSource);
    }

    /** Creates the state tables through any data source, e.g. the one a Spring context created. */
    public static void createTables(JdbcClient jdbc) {
        String script;
        try {
            script = new ClassPathResource("db/h2/migration-state.sql").getContentAsString(StandardCharsets.UTF_8)
                    .replace("${run_table}", RUN_TABLE)
                    .replace("${file_table}", FILE_TABLE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Arrays.stream(script.split(";"))
                .map(statement -> statement.replaceAll("(?m)^\\s*--.*$", "").strip())
                .filter(statement -> !statement.isEmpty())
                .forEach(statement -> jdbc.sql(statement).update());
    }
}
