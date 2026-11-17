package com.tds.pde.migration.source;

import com.tds.pde.migration.config.MigrationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class SourceConfig {

    /**
     * Hadoop configuration for reading the source table: Hadoop defaults plus {@code migration.source.hadoop.*}.
     */
    @Bean
    org.apache.hadoop.conf.Configuration hadoopConfiguration(MigrationProperties properties) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        properties.source().hadoop().forEach(configuration::set);
        return configuration;
    }
}
