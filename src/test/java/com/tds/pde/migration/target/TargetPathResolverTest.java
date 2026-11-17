package com.tds.pde.migration.target;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TargetPathResolverTest {

    private static final String SOURCE =
            "hdfs://nn/warehouse/orders/data/region=EU/00000-5-a1b2c3d4-0001-00001.parquet";

    private final TargetPathResolver resolver = new TargetPathResolver();

    @Test
    void encodesThePartitionAndSourceFileInAFlatFileName() {
        String name = resolver.resolve("sales/orders", "region=EU/event_ts_day=2026-01-15", SOURCE);

        assertThat(name).matches(
                "sales/orders/region=EU\\.event_ts_day=2026-01-15__00000-5-a1b2c3d4-0001-00001-[0-9a-f]{8}\\.parquet");
    }

    @Test
    void omitsThePartitionForUnpartitionedTables() {
        assertThat(resolver.resolve("sales/orders", "", SOURCE))
                .matches("sales/orders/00000-5-a1b2c3d4-0001-00001-[0-9a-f]{8}\\.parquet");
    }

    @Test
    void writesToTheContainerRootWhenThereIsNoPrefix() {
        assertThat(resolver.resolve("", "", SOURCE)).doesNotContain("/").endsWith(".parquet");
    }

    @Test
    void isDeterministic() {
        assertThat(resolver.resolve("p", "region=EU", SOURCE)).isEqualTo(resolver.resolve("p", "region=EU", SOURCE));
    }

    @Test
    void keepsApartSourceFilesWithTheSameNameInDifferentDirectories() {
        String first = resolver.resolve("p", "", "hdfs://nn/import/a/part-0.parquet");
        String second = resolver.resolve("p", "", "hdfs://nn/import/b/part-0.parquet");

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void shortensLongPartitionPathsToTheFileNameLimit() {
        String longPartition = "comment=" + "x".repeat(400);

        String name = resolver.resolve("p", longPartition, SOURCE);

        String fileName = name.substring("p/".length());
        assertThat(fileName).hasSize(TargetPathResolver.MAX_FILE_NAME_LENGTH);
        assertThat(fileName).startsWith("comment=xxx").contains("__00000-5-a1b2c3d4-0001-00001-").endsWith(".parquet");
    }

    @Test
    void neverStartsAFileNameWithACharacterSparkSkips() {
        assertThat(resolver.resolve("p", "_hidden=1", SOURCE)).startsWith("p/p_hidden=1__");
        assertThat(resolver.resolve("p", "", "hdfs://nn/data/.part-0.parquet")).startsWith("p/p.part-0-");
    }
}
