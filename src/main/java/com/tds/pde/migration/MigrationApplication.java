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
        application.setRegisterShutdownHook(false);
        try (ConfigurableApplicationContext context = application.run(args)) {
            context.getBean(MigrationOrchestrator.class).migrate();
        }
    }
}
