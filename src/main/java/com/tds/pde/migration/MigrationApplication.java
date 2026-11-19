package com.tds.pde.migration;

import java.util.Locale;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Entry point invoked by the Databricks launcher inside an isolated class loader.
 *
 * <p>The launcher shares the driver JVM, so this never calls {@code System.exit} and registers no shutdown
 * hook: the context is closed explicitly, which stops every non-daemon thread before {@code main} returns.
 * A failed migration surfaces as an exception thrown from {@code main}, which fails the Databricks task.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class MigrationApplication {

    /** Properties holding secrets, by canonical name, with the environment variable each is set from instead. */
    private static final Map<String, String> SECRET_PROPERTIES =
            Map.of("migration.azure.auth.connectionstring", "MIGRATION_AZURE_AUTH_CONNECTIONSTRING");

    public static void main(String[] args) {
        run(new SpringApplication(MigrationApplication.class), args);
    }

    static void run(SpringApplication application, String... args) {
        requireKnownOptions(args);
        application.setRegisterShutdownHook(false);
        try (ConfigurableApplicationContext context = application.run(args)) {
            context.getBean(MigrationOrchestrator.class).migrate();
        }
    }

    /**
     * Spring Boot silently ignores arguments it does not bind, so a mistyped job parameter would quietly change what
     * runs. Unknown keys under {@code migration.} are rejected by the configuration binding; this rejects the rest.
     * It also rejects a secret, such as a connection string, which as a job parameter would be shown in the
     * Databricks job's settings and run history: secrets only come from environment variables.
     */
    private static void requireKnownOptions(String... args) {
        for (String arg : args) {
            if (!arg.matches("--(migration|spring|logging)\\.[^=\\s]+=.*")) {
                throw new IllegalArgumentException("Unknown job parameter '" + arg + "': expected --migration.<name>=<value>"
                        + " (or --spring.* / --logging.*), e.g. --migration.source.dataset-id=<uuid>");
            }
            String key = arg.substring(2, arg.indexOf('='));
            if (SECRET_PROPERTIES.containsKey(canonical(key))) {
                // Names the key only: the value is the secret.
                throw new IllegalArgumentException("Job parameter --" + key + " holds a secret, which would show in "
                        + "the job's settings and run history; set the environment variable "
                        + SECRET_PROPERTIES.get(canonical(key)) + " instead, from a secret reference "
                        + "{{secrets/<scope>/<key>}}");
            }
        }
    }

    /** Spring Boot's relaxed binding form of a property name: lower case, without dashes or underscores. */
    private static String canonical(String key) {
        return key.replaceAll("\\[([^]]*)]", ".$1").replaceAll("[-_]", "").toLowerCase(Locale.ROOT);
    }
}
