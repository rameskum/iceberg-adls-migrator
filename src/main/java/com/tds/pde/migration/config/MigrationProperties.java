package com.tds.pde.migration.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.hibernate.validator.constraints.time.DurationMin;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

/**
 * All externalized migration settings, bound from the {@code migration.*} namespace.
 * Invalid, missing or unknown values (such as a misspelled job parameter) fail application startup, before any
 * data is touched; every invalid property is reported at once.
 */
@Validated
@ConfigurationProperties(prefix = "migration", ignoreUnknownFields = false)
public record MigrationProperties(
        @Nullable @Pattern(regexp = "\\d{8}-\\d{6}-[0-9a-f]{6}", message = "must look like 20261117-103000-ab12cd")
        String runId,
        @NotNull @Valid Source source,
        @NotNull @Valid Target target,
        @NotNull @Valid Datasets datasets,
        @DefaultValue @Valid Azure azure,
        @DefaultValue @Valid Parquet parquet,
        @DefaultValue @Valid Progress progress,
        @DefaultValue @Valid Failure failure,
        @NotNull @Valid State state) {

    public MigrationProperties {
        runId = blankToNull(runId);
    }

    /**
     * @param datasetId  UUID of the source dataset in the dataset API
     * @param snapshotId optional snapshot to migrate instead of the table's current snapshot
     * @param hadoop     extra Hadoop configuration, copied verbatim; quote dotted keys as {@code "[fs.defaultFS]"}
     */
    public record Source(@NotNull UUID datasetId, @Nullable Long snapshotId, Map<String, String> hadoop) {

        public Source {
            hadoop = hadoop == null ? Map.of() : Map.copyOf(hadoop);
        }
    }

    /**
     * @param datasetId UUID of the target dataset in the dataset API
     * @param clean     delete everything under the target location and start a fresh run
     * @param blockSize size of each staged block uploaded to Azure Blob Storage, at most Azure's 4000 MiB
     */
    public record Target(@NotNull UUID datasetId, @DefaultValue("false") boolean clean,
                         @DefaultValue("8MB") @DataSizeRange(min = 1, max = 4000L * 1024 * 1024) DataSize blockSize) {
    }

    /**
     * @param stubDirectory folder holding one {@code <dataset-id>.json} descriptor per dataset
     */
    public record Datasets(@NotNull Path stubDirectory) {
    }

    public record Azure(@DefaultValue @Valid Auth auth) {

        public enum AuthMode {
            MANAGED_IDENTITY,
            /** Local runs and tests against Azurite only. */
            CONNECTION_STRING
        }

        /**
         * @param clientId         client id of a user-assigned managed identity; empty for system-assigned
         * @param connectionString storage connection string, required for {@link AuthMode#CONNECTION_STRING}
         */
        public record Auth(@DefaultValue("MANAGED_IDENTITY") AuthMode mode, @Nullable String clientId,
                           @Nullable String connectionString) {

            public Auth {
                clientId = blankToNull(clientId);
                connectionString = blankToNull(connectionString);
            }

            @AssertTrue(message = "connection-string is required when mode is CONNECTION_STRING")
            public boolean isConnectionStringSetWhenRequired() {
                return mode != AuthMode.CONNECTION_STRING || connectionString != null;
            }
        }
    }

    public record Parquet(@DefaultValue("128MB") @DataSizeRange(min = 1) DataSize rowGroupSize,
                          @DefaultValue("1MB") @DataSizeRange(min = 1) DataSize pageSize,
                          @DefaultValue("ZSTD") Compression compression) {

        public enum Compression {
            ZSTD,
            SNAPPY,
            GZIP,
            UNCOMPRESSED
        }
    }

    /**
     * Record progress is logged whenever either threshold is crossed.
     */
    public record Progress(@DefaultValue("100000") @Positive long logEveryRecords,
                           @DefaultValue("30s") @DurationMin(millis = 1) Duration logEvery) {
    }

    public record Failure(@DefaultValue("FAIL_FAST") OnFileFailure onFileFailure) {

        public enum OnFileFailure {
            /** Stop the run at the first failed file. */
            FAIL_FAST,
            /** Process the remaining files, then fail the run if any file failed. */
            CONTINUE
        }
    }

    /**
     * The state tables, shared by every migration job; created at startup when missing. Their names are spliced
     * into SQL because identifiers cannot be bound as parameters, so only plain {@code [catalog.][schema.]table}
     * identifiers are accepted.
     *
     * @param controlTable fully qualified control table, e.g. {@code main.migration.migration_control}
     * @param runTable     fully qualified run table, e.g. {@code main.migration.migration_run}
     * @param fileTable    fully qualified file table, e.g. {@code main.migration.migration_file}
     * @param writeRetries attempts for each state write before the run is stopped
     */
    public record State(@NotNull @Pattern(regexp = TABLE_NAME, message = TABLE_NAME_MESSAGE) String controlTable,
                        @NotNull @Pattern(regexp = TABLE_NAME, message = TABLE_NAME_MESSAGE) String runTable,
                        @NotNull @Pattern(regexp = TABLE_NAME, message = TABLE_NAME_MESSAGE) String fileTable,
                        @DefaultValue("5") @Min(1) int writeRetries) {

        static final String TABLE_NAME = "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}";
        static final String TABLE_NAME_MESSAGE =
                "must be [catalog.][schema.]table using letters, digits and underscores";
    }

    private static @Nullable String blankToNull(@Nullable String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
