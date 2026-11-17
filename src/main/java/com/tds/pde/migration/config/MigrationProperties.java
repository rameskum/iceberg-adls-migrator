package com.tds.pde.migration.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * All externalized migration settings, bound from the {@code migration.*} namespace.
 * Invalid, missing or unknown values (such as a misspelled job parameter) fail application startup, before any
 * data is touched.
 */
@ConfigurationProperties(prefix = "migration", ignoreUnknownFields = false)
public record MigrationProperties(
        @Nullable String runId,
        Source source,
        Target target,
        Datasets datasets,
        @DefaultValue Azure azure,
        @DefaultValue Parquet parquet,
        @DefaultValue Progress progress,
        @DefaultValue Failure failure,
        State state) {

    public MigrationProperties {
        require(source, "migration.source");
        require(target, "migration.target");
        require(datasets, "migration.datasets");
        require(state, "migration.state");
        runId = blankToNull(runId);
    }

    /**
     * @param datasetId  UUID of the source dataset in the dataset API
     * @param snapshotId optional snapshot to migrate instead of the table's current snapshot
     * @param hadoop     extra Hadoop configuration, copied verbatim; quote dotted keys as {@code "[fs.defaultFS]"}
     */
    public record Source(UUID datasetId, @Nullable Long snapshotId, Map<String, String> hadoop) {

        public Source {
            require(datasetId, "migration.source.dataset-id");
            hadoop = hadoop == null ? Map.of() : Map.copyOf(hadoop);
        }
    }

    /**
     * @param datasetId UUID of the target dataset in the dataset API
     * @param clean     delete everything under the target location and start a fresh run
     * @param blockSize size of each staged block uploaded to Azure Blob Storage
     */
    public record Target(UUID datasetId, @DefaultValue("false") boolean clean,
                              @DefaultValue("8MB") DataSize blockSize) {

        private static final DataSize MAX_BLOCK_SIZE = DataSize.ofMegabytes(4000);

        public Target {
            require(datasetId, "migration.target.dataset-id");
            if (blockSize.toBytes() <= 0 || blockSize.compareTo(MAX_BLOCK_SIZE) > 0) {
                throw new IllegalArgumentException(
                        "migration.target.block-size must be between 1B and 4000MB, was " + blockSize);
            }
        }
    }

    /**
     * @param stubDirectory folder holding one {@code <dataset-id>.json} descriptor per dataset
     */
    public record Datasets(Path stubDirectory) {

        public Datasets {
            require(stubDirectory, "migration.datasets.stub-directory");
        }
    }

    public record Azure(@DefaultValue Auth auth) {

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
                if (mode == AuthMode.CONNECTION_STRING && connectionString == null) {
                    throw new IllegalArgumentException(
                            "migration.azure.auth.connection-string is required when mode is CONNECTION_STRING");
                }
            }
        }
    }

    public record Parquet(@DefaultValue("128MB") DataSize rowGroupSize, @DefaultValue("1MB") DataSize pageSize,
                          @DefaultValue("ZSTD") Compression compression) {

        public enum Compression {
            ZSTD,
            SNAPPY,
            GZIP,
            UNCOMPRESSED
        }

        public Parquet {
            requirePositive(rowGroupSize, "migration.parquet.row-group-size");
            requirePositive(pageSize, "migration.parquet.page-size");
        }
    }

    /**
     * Record progress is logged whenever either threshold is crossed.
     */
    public record Progress(@DefaultValue("100000") long logEveryRecords, @DefaultValue("30s") Duration logEvery) {

        public Progress {
            if (logEveryRecords <= 0) {
                throw new IllegalArgumentException("migration.progress.log-every-records must be positive");
            }
            if (logEvery.isNegative() || logEvery.isZero()) {
                throw new IllegalArgumentException("migration.progress.log-every must be positive");
            }
        }
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
     * The state tables, shared by every migration job; created at startup when missing.
     *
     * @param controlTable fully qualified control table, e.g. {@code main.migration.migration_control}
     * @param runTable     fully qualified run table, e.g. {@code main.migration.migration_run}
     * @param fileTable    fully qualified file table, e.g. {@code main.migration.migration_file}
     * @param writeRetries attempts for each state write before the run is stopped
     */
    public record State(String controlTable, String runTable, String fileTable,
                        @DefaultValue("5") int writeRetries) {

        // Table names are spliced into SQL because identifiers cannot be bound as parameters, so only plain
        // identifiers pass.
        private static final Pattern TABLE_NAME =
                Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}");

        public State {
            requireTableName(controlTable, "migration.state.control-table");
            requireTableName(runTable, "migration.state.run-table");
            requireTableName(fileTable, "migration.state.file-table");
            if (writeRetries < 1) {
                throw new IllegalArgumentException("migration.state.write-retries must be at least 1");
            }
        }

        private static void requireTableName(String value, String property) {
            requireText(value, property);
            if (!TABLE_NAME.matcher(value).matches()) {
                throw new IllegalArgumentException(
                        property + " must be [catalog.][schema.]table using letters, digits and underscores, was '"
                                + value + "'");
            }
        }
    }

    private static void require(Object value, String property) {
        Objects.requireNonNull(value, () -> property + " is required");
    }

    private static void requireText(String value, String property) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(property + " is required");
        }
    }

    private static void requirePositive(DataSize value, String property) {
        if (value.toBytes() <= 0) {
            throw new IllegalArgumentException(property + " must be positive");
        }
    }

    private static @Nullable String blankToNull(@Nullable String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
