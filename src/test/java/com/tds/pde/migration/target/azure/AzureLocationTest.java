package com.tds.pde.migration.target.azure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AzureLocationTest {

    @Test
    void parsesAnAbfssLocationIntoBlobEndpointContainerAndPrefix() {
        AzureLocation location = AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net/sales/orders/");

        assertThat(location.account()).isEqualTo("salesacct");
        assertThat(location.container()).isEqualTo("data");
        assertThat(location.prefix()).isEqualTo("sales/orders");
        assertThat(location.blobEndpoint()).isEqualTo("https://salesacct.blob.core.windows.net");
        assertThat(location.isContainerRoot()).isFalse();
        assertThat(location.blobName("part.parquet")).isEqualTo("sales/orders/part.parquet");
    }

    @Test
    void keepsTheEndpointSuffixOfSovereignClouds() {
        assertThat(AzureLocation.parse("abfss://data@govacct.dfs.core.usgovcloudapi.net/x").blobEndpoint())
                .isEqualTo("https://govacct.blob.core.usgovcloudapi.net");
    }

    @Test
    void recognisesTheContainerRoot() {
        AzureLocation location = AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net");

        assertThat(location.isContainerRoot()).isTrue();
        assertThat(location.blobName("part.parquet")).isEqualTo("part.parquet");
        assertThat(AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net/").isContainerRoot()).isTrue();
    }

    @Test
    void rejectsOtherSchemes() {
        assertThatThrownBy(() -> AzureLocation.parse("https://salesacct.blob.core.windows.net/data/x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be abfss://");
    }

    @Test
    void rejectsALocationWithoutContainer() {
        assertThatThrownBy(() -> AzureLocation.parse("abfss://salesacct.dfs.core.windows.net/x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must name a container");
    }

    @Test
    void rejectsANonDfsHost() {
        assertThatThrownBy(() -> AzureLocation.parse("abfss://data@salesacct.blob.core.windows.net/x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dfs endpoint");
    }

    @Test
    void rejectsEmptyPathSegments() {
        assertThatThrownBy(() -> AzureLocation.parse("abfss://data@salesacct.dfs.core.windows.net/a//b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty path segment");
    }
}
