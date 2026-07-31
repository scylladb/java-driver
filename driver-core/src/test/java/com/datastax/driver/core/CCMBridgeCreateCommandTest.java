package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge.Builder.ResolvedVersions;
import org.testng.annotations.Test;

/**
 * Unit tests for the part of {@link CCMBridge.Builder} that decides which server flavor and version
 * to install. No CCM cluster is created.
 *
 * <p>Each test configures the flavor explicitly, so that it doesn't depend on the {@code
 * scylla.version} / {@code dse} system properties of the surrounding run.
 */
public class CCMBridgeCreateCommandTest {

  @Test(groups = "unit")
  public void should_create_scylla_cluster_when_scylla_version_configured() {
    CCMBridge.Builder builder =
        CCMBridge.builder()
            .withDSE(false)
            .withScylla(true)
            .withVersion(VersionNumber.parse("2026.1.0"));

    ResolvedVersions versions = builder.resolveVersions();
    assertThat(versions.scylla).isEqualTo(VersionNumber.parse("2026.1.0"));
    assertThat(versions.cassandra).isEqualTo(VersionNumber.parse("3.0.8"));
    assertThat(versions.dse).isNull();

    String command = builder.buildCreateCommand("test_cluster", versions);
    assertThat(command).contains("--scylla").contains("-v release:2026.1.0");
    assertThat(command).doesNotContain("--dse");
    // 3.0.8 is only what Scylla reports in system.local, it is never an install target
    assertThat(command).doesNotContain("3.0.8");
  }

  @Test(groups = "unit")
  public void should_create_cassandra_cluster_when_cassandra_version_configured() {
    CCMBridge.Builder builder =
        CCMBridge.builder()
            .withDSE(false)
            .withScylla(false)
            .withVersion(VersionNumber.parse("4.1.3"));

    ResolvedVersions versions = builder.resolveVersions();
    assertThat(versions.cassandra).isEqualTo(VersionNumber.parse("4.1.3"));
    assertThat(versions.dse).isNull();
    assertThat(versions.scylla).isNull();

    String command = builder.buildCreateCommand("test_cluster", versions);
    assertThat(command).contains("-v 4.1.3");
    assertThat(command).doesNotContain("--scylla").doesNotContain("--dse");
  }

  @Test(groups = "unit")
  public void should_create_dse_cluster_when_dse_version_configured() {
    CCMBridge.Builder builder =
        CCMBridge.builder()
            .withDSE(true)
            .withScylla(false)
            .withVersion(VersionNumber.parse("6.8.0"));

    ResolvedVersions versions = builder.resolveVersions();
    assertThat(versions.dse).isEqualTo(VersionNumber.parse("6.8.0"));
    assertThat(versions.cassandra).isNotNull();
    assertThat(versions.scylla).isNull();

    String command = builder.buildCreateCommand("test_cluster", versions);
    assertThat(command).contains("--dse").contains("-v 6.8.0");
    assertThat(command).doesNotContain("--scylla");
  }
}
