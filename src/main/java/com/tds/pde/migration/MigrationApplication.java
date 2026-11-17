package com.tds.pde.migration;

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
     */
    private static void requireKnownOptions(String... args) {
        for (String arg : args) {
            if (!arg.matches("--(migration|spring|logging)\\.[^=\\s]+=.*")) {
                throw new IllegalArgumentException("Unknown job parameter '" + arg + "': expected --migration.<name>=<value>"
                        + " (or --spring.* / --logging.*), e.g. --migration.source.dataset-id=<uuid>");
            }
        }
    }
}
