package com.tds.pde.migration.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tds.pde.migration.config.MigrationProperties.Azure.AuthMode;
import com.tds.pde.migration.config.MigrationProperties.Failure.OnFileFailure;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

class MigrationPropertiesTest {

    private static Map<String, String> required() {
        Map<String, String> values = new HashMap<>();
        values.put("migration.source.dataset-id", "0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01");
        values.put("migration.target.dataset-id", "5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02");
        values.put("migration.datasets.stub-directory", "/tmp/datasets");
        values.put("migration.state.control-table", "main.migration.migration_control");
        values.put("migration.state.run-table", "main.migration.migration_run");
        values.put("migration.state.file-table", "main.migration.migration_file");
        return values;
    }

    /** Binds and validates the way Spring Boot does for {@code @Validated} configuration properties. */
    private static MigrationProperties bind(Map<String, String> values) {
        return bind(new Binder(new MapConfigurationPropertySource(values)));
    }

    private static MigrationProperties bind(Binder binder) {
        LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        return binder.bindOrCreate("migration", Bindable.of(MigrationProperties.class),
                new ValidationBindHandler(validator));
    }

    @Test
    void appliesDefaultsWhenOnlyRequiredValuesAreSet() {
        MigrationProperties properties = bind(required());

        assertThat(properties.runId()).isNull();
        assertThat(properties.source().snapshotId()).isNull();
        assertThat(properties.source().hadoop()).isEmpty();
        assertThat(properties.target().clean()).isFalse();
        assertThat(properties.target().blockSize()).isEqualTo(DataSize.ofMegabytes(8));
        assertThat(properties.datasets().stubDirectory()).isEqualTo(Path.of("/tmp/datasets"));
        assertThat(properties.azure().auth().mode()).isEqualTo(AuthMode.MANAGED_IDENTITY);
        assertThat(properties.azure().auth().clientId()).isNull();
        assertThat(properties.parquet().rowGroupSize()).isEqualTo(DataSize.ofMegabytes(32));
        assertThat(properties.parquet().pageSize()).isEqualTo(DataSize.ofMegabytes(1));
        assertThat(properties.progress().logEvery()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.failure().onFileFailure()).isEqualTo(OnFileFailure.FAIL_FAST);
        assertThat(properties.state().writeRetries()).isEqualTo(5);
        assertThat(properties.state().outcomeBatchSize()).isEqualTo(50);
        assertThat(properties.state().outcomeFlushInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(properties.processing().parallelFiles()).isEqualTo(4);
    }

    @Test
    void rejectsParallelFilesOutsideItsRange() {
        Map<String, String> values = required();
        values.put("migration.processing.parallel-files", "0");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("on field 'parallelFiles'");
    }

    @Test
    void treatsBlankRunIdAsAbsent() {
        Map<String, String> values = required();
        values.put("migration.run-id", "  ");

        assertThat(bind(values).runId()).isNull();
    }

    @Test
    void keepsCaseOfBracketedHadoopKeys() {
        Map<String, String> values = required();
        values.put("migration.source.hadoop[fs.defaultFS]", "hdfs://namenode:8020");
        values.put("migration.source.hadoop[dfs.client.use.datanode.hostname]", "true");

        assertThat(bind(values).source().hadoop())
                .containsExactlyInAnyOrderEntriesOf(Map.of(
                        "fs.defaultFS", "hdfs://namenode:8020",
                        "dfs.client.use.datanode.hostname", "true"));
    }

    @Test
    void rejectsMissingSourceDataset() {
        Map<String, String> values = required();
        values.remove("migration.source.dataset-id");
        values.put("migration.source.hadoop[fs.defaultFS]", "hdfs://namenode:8020");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("Field error in object 'migration.source' on field 'datasetId'");
    }

    @Test
    void rejectsTableNamesThatAreNotPlainIdentifiers() {
        Map<String, String> values = required();
        values.put("migration.state.run-table", "main.migration.runs; DROP TABLE x");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("runTable")
                .hasStackTraceContaining("must be [catalog.][schema.]table");
    }

    @Test
    void rejectsTableNamesWithMoreThanThreeParts() {
        Map<String, String> values = required();
        values.put("migration.state.file-table", "a.b.c.d");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("fileTable")
                .hasStackTraceContaining("must be [catalog.][schema.]table");
    }

    @Test
    void requiresConnectionStringForConnectionStringMode() {
        Map<String, String> values = required();
        values.put("migration.azure.auth.mode", "CONNECTION_STRING");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("connection-string is required when mode is CONNECTION_STRING");
    }

    @Test
    void rejectsBlockSizeAboveAzureLimit() {
        Map<String, String> values = required();
        values.put("migration.target.block-size", "5GB");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("blockSize")
                .hasStackTraceContaining("must be between 1 and 4194304000 bytes");
    }

    @Test
    void reportsEveryInvalidPropertyOfASectionTogether() {
        Map<String, String> values = required();
        values.put("migration.state.run-table", "runs; DROP TABLE x");
        values.put("migration.state.file-table", "a.b.c.d");
        values.put("migration.state.write-retries", "0");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("on field 'runTable'")
                .hasStackTraceContaining("on field 'fileTable'")
                .hasStackTraceContaining("on field 'writeRetries'");
    }

    @Test
    void rejectsARunIdThatIsNotOfTheGeneratedShape() {
        Map<String, String> values = required();
        values.put("migration.run-id", "resume-please");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("must look like 20261117-103000-ab12cd");
    }

    @Test
    void rejectsANonPositiveLogInterval() {
        Map<String, String> values = required();
        values.put("migration.progress.log-every", "0s");

        assertThatThrownBy(() -> bind(values))
                .isInstanceOf(BindException.class)
                .hasStackTraceContaining("on field 'logEvery'");
    }

    @Test
    void acceptsARunIdOfTheGeneratedShape() {
        Map<String, String> values = required();
        values.put("migration.run-id", "20261117-103000-ab12cd");

        assertThat(bind(values).runId()).isEqualTo("20261117-103000-ab12cd");
    }

    @Test
    void bindsTheDocumentedExampleConfiguration() throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        new YamlPropertySourceLoader()
                .load("example", new FileSystemResource("config/application-example.yaml"))
                .forEach(environment.getPropertySources()::addFirst);

        MigrationProperties properties = bind(new Binder(ConfigurationPropertySources.get(environment)));

        assertThat(properties.runId()).isNull();
        assertThat(properties.source().datasetId())
                .isEqualTo(java.util.UUID.fromString("0e9c1a5e-4c55-4a3b-9f3e-1c2a7d8b6e01"));
        assertThat(properties.source().hadoop()).containsEntry("fs.defaultFS", "hdfs://namenode:8020");
        assertThat(properties.target().datasetId())
                .isEqualTo(java.util.UUID.fromString("5b2f7d3c-8a1e-4f6b-b2d4-9c0e3a7f1d02"));
        assertThat(properties.state().controlTable()).isEqualTo("main.migration.migration_control");
        assertThat(properties.state().runTable()).isEqualTo("main.migration.migration_run");
        assertThat(properties.state().fileTable()).isEqualTo("main.migration.migration_file");
    }
}
