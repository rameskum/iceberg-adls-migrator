package com.tds.pde.migration.testing;

import com.tds.pde.migration.state.SqlExecutor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Extra application source that runs the state tables on {@link #current} instead of Spark. Deliberately not a
 * {@code @Configuration}, so component scanning never picks it up; pass it to {@code SpringApplication}
 * explicitly. Tests run one at a time, so one shared store is enough.
 */
public class H2State {

    public static H2StateStore current = new H2StateStore();

    @Bean
    @Primary
    SqlExecutor h2SqlExecutor() {
        return current.executor();
    }
}
