package com.tds.pde.migration.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
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
        @DefaultValue @Valid Processing processing,
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
     * @param blockSize size of each staged block uploaded to Azure Blob Storage, at most Azure's 4000 MiB; each
     *                  file in flight holds up to two, one filling while the other is uploaded
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

    /**
     * @param rowGroupSize target row group size; the writer buffers about this much per file in flight, so
     *                     {@code parallel-files} times this is the main driver memory cost of the target side
     */
    public record Parquet(@DefaultValue("32MB") @DataSizeRange(min = 1) DataSize rowGroupSize,
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
     * @param parallelFiles source files copied at the same time. Memory grows with it: each file in flight holds
     *                      about one source row group, one target row group and up to two upload blocks.
     */
    public record Processing(@DefaultValue("4") @Min(1) @Max(64) int parallelFiles) {
    }

    /**
     * @param logEvery how often the {@code Progress:} heartbeat line is logged while files are being copied
     */
    public record Progress(@DefaultValue("30s") @DurationMin(millis = 1) Duration logEvery) {
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
     * @param controlTable          fully qualified control table, e.g. {@code main.migration.migration_control}
     * @param runTable              fully qualified run table, e.g. {@code main.migration.migration_run}
     * @param fileTable             fully qualified file table, e.g. {@code main.migration.migration_file}
     * @param writeRetries          attempts for each state write before the run is stopped
     * @param outcomeBatchSize      file outcomes written to the file table in one commit
     * @param outcomeFlushInterval  longest a file outcome waits for its batch to fill; the next outcome after it
     *                              writes the batch
     */
    public record State(@NotNull @Pattern(regexp = TABLE_NAME, message = TABLE_NAME_MESSAGE) String controlTable,
                        @NotNull @Pattern(regexp = TABLE_NAME, message = TABLE_NAME_MESSAGE) String runTable,
                        @NotNull @Pattern(regexp = TABLE_NAME, message = TABLE_NAME_MESSAGE) String fileTable,
                        @DefaultValue("5") @Min(1) int writeRetries,
                        @DefaultValue("50") @Min(1) @Max(5000) int outcomeBatchSize,
                        @DefaultValue("30s") @NotNull Duration outcomeFlushInterval) {

        static final String TABLE_NAME = "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}";
        static final String TABLE_NAME_MESSAGE =
                "must be [catalog.][schema.]table using letters, digits and underscores";
    }

    private static @Nullable String blankToNull(@Nullable String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
