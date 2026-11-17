package com.tds.pde.migration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.config.MigrationProperties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.event.ContextClosedEvent;

class MigrationApplicationTest {

    private static final String[] ARGS = {
            "--migration.source.dataset-id=orders-src",
            "--migration.destination.dataset-id=orders-adls",
            "--migration.datasets.stub-directory=/tmp/datasets",
            "--migration.state.run-table=migration_run",
            "--migration.state.file-table=migration_file",
            "--spring.datasource.url=jdbc:h2:mem:application-test",
            "--spring.datasource.driver-class-name=org.h2.Driver",
    };

    @Test
    void closesTheContextAfterASuccessfulMigration() {
        AtomicBoolean closed = new AtomicBoolean();
        SpringApplication application = new SpringApplication(MigrationApplication.class);
        application.addListeners(event -> {
            if (event instanceof ContextClosedEvent) {
                closed.set(true);
            }
        });

        MigrationApplication.run(application, ARGS);

        assertThat(closed).isTrue();
    }

    @Test
    void closesTheContextAndRethrowsWhenTheMigrationFails() {
        AtomicBoolean closed = new AtomicBoolean();
        SpringApplication application =
                new SpringApplication(MigrationApplication.class, FailingOrchestratorConfig.class);
        application.addListeners(event -> {
            if (event instanceof ContextClosedEvent) {
                closed.set(true);
            }
        });

        assertThatThrownBy(() -> MigrationApplication.run(application, ARGS))
                .isInstanceOf(MigrationFailedException.class)
                .hasMessage("boom");
        assertThat(closed).isTrue();
    }

    @Test
    void failsStartupWhenRequiredConfigurationIsMissing() {
        SpringApplication application = new SpringApplication(MigrationApplication.class);

        assertThatThrownBy(() -> MigrationApplication.run(application))
                .hasRootCauseMessage("migration.source is required");
    }

    /** Deliberately not a {@code @Configuration}, so component scanning never picks it up for other tests. */
    static class FailingOrchestratorConfig {

        @Bean
        @Primary
        MigrationOrchestrator failingOrchestrator(MigrationProperties properties) {
            return new MigrationOrchestrator(properties) {
                @Override
                public void migrate() {
                    throw new MigrationFailedException("boom");
                }
            };
        }
    }
}
