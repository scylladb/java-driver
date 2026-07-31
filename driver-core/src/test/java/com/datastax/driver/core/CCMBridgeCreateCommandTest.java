package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge.Builder.ResolvedVersions;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Unit tests for the part of {@link CCMBridge.Builder} that decides which server flavor and version
 * to install, i.e. the {@code ccm create} command, the environment it runs in and the
 * flavor-specific yaml it writes. No CCM cluster is created.
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

    // 2026.1.0 is an Enterprise version, it must not be installed from the OSS repository
    assertThat(CCMBridge.Builder.buildEnvironmentMap(versions))
        .containsEntry("SCYLLA_PRODUCT", "enterprise");
  }

  @Test(groups = "unit")
  public void should_not_use_enterprise_repository_for_open_source_scylla_version() {
    CCMBridge.Builder builder =
        CCMBridge.builder()
            .withDSE(false)
            .withScylla(true)
            .withVersion(VersionNumber.parse("6.2.0"));

    ResolvedVersions versions = builder.resolveVersions();
    assertThat(versions.scylla).isEqualTo(VersionNumber.parse("6.2.0"));

    // Would leak in from a `-Dscylla.version=<year>.<x>` run if the product was global
    assertThat(CCMBridge.Builder.buildEnvironmentMap(versions)).doesNotContainKey("SCYLLA_PRODUCT");
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

    assertThat(CCMBridge.Builder.buildEnvironmentMap(versions)).doesNotContainKey("SCYLLA_PRODUCT");
  }

  /**
   * The globally configured version keeps the environment built for it in the static initializer:
   * that one is derived from the raw {@code scylla.version} string, which may be a branch spec
   * whose resolved version number looks like an Enterprise one without being installed as such.
   */
  @Test(groups = "unit")
  public void should_use_global_environment_when_no_version_configured() {
    VersionNumber cassandra = VersionNumber.parse("3.0.8");
    VersionNumber enterpriseScylla = VersionNumber.parse("2026.1.0");

    assertThat(
            CCMBridge.Builder.buildEnvironmentMap(
                new ResolvedVersions(false, cassandra, null, enterpriseScylla)))
        .isSameAs(
            CCMBridge.Builder.buildEnvironmentMap(
                new ResolvedVersions(false, cassandra, null, null)));
  }

  /**
   * Scylla reads a PEM certificate and key, not the JKS keystore Cassandra reads, so an explicitly
   * configured Scylla cluster must not be given the Cassandra settings just because the surrounding
   * run has no {@code scylla.version}.
   */
  @Test(groups = "unit")
  public void should_use_pem_client_encryption_for_configured_scylla_version() {
    CCMBridge.Builder builder =
        CCMBridge.builder()
            .withDSE(false)
            .withScylla(true)
            .withVersion(VersionNumber.parse("2026.1.0"))
            .withAuth();

    Map<String, Object> options = builder.buildClientEncryptionOptions(builder.resolveVersions());

    assertThat(options)
        .containsEntry("client_encryption_options.enabled", "true")
        .containsEntry("client_encryption_options.require_client_auth", "true")
        .containsKey("client_encryption_options.certificate")
        .containsKey("client_encryption_options.keyfile")
        .containsKey("client_encryption_options.truststore");
    assertThat(options)
        .doesNotContainKey("client_encryption_options.keystore")
        .doesNotContainKey("client_encryption_options.keystore_password")
        .doesNotContainKey("client_encryption_options.truststore_password");
  }

  /** The mirror image: an explicit Cassandra version under a global Scylla run. */
  @Test(groups = "unit")
  public void should_use_keystore_client_encryption_for_configured_cassandra_version() {
    CCMBridge.Builder builder =
        CCMBridge.builder()
            .withDSE(false)
            .withScylla(false)
            .withVersion(VersionNumber.parse("4.1.3"))
            .withAuth();

    Map<String, Object> options = builder.buildClientEncryptionOptions(builder.resolveVersions());

    assertThat(options)
        .containsEntry("client_encryption_options.enabled", "true")
        .containsEntry("client_encryption_options.require_client_auth", "true")
        .containsKey("client_encryption_options.keystore")
        .containsKey("client_encryption_options.keystore_password")
        .containsKey("client_encryption_options.truststore")
        .containsKey("client_encryption_options.truststore_password");
    assertThat(options)
        .doesNotContainKey("client_encryption_options.certificate")
        .doesNotContainKey("client_encryption_options.keyfile");
  }

  /** {@code withSSL()} alone must not enable client certificate authentication. */
  @Test(groups = "unit")
  public void should_not_require_client_auth_without_with_auth() {
    CCMBridge.Builder sslOnly = CCMBridge.builder().withDSE(false).withScylla(true).withSSL();
    assertThat(sslOnly.buildClientEncryptionOptions(sslOnly.resolveVersions()))
        .containsEntry("client_encryption_options.enabled", "true")
        .doesNotContainKey("client_encryption_options.require_client_auth");

    CCMBridge.Builder plaintext = CCMBridge.builder().withDSE(false).withScylla(true);
    assertThat(plaintext.buildClientEncryptionOptions(plaintext.resolveVersions())).isEmpty();
  }

  /**
   * {@code ssl}/{@code auth} are no longer reflected in {@code cassandraConfiguration} at
   * configuration time, so {@link CCMBridge.Builder} has to compare them itself: {@link CCMCache}
   * keys cached clusters on the builder, and would otherwise hand an encrypted cluster to a test
   * that asked for a plaintext one.
   */
  @Test(groups = "unit")
  public void should_not_consider_encrypted_and_plaintext_clusters_equal() {
    CCMBridge.Builder plaintext = CCMBridge.builder().withNodes(1);
    CCMBridge.Builder encrypted = CCMBridge.builder().withNodes(1).withSSL();
    CCMBridge.Builder authenticated = CCMBridge.builder().withNodes(1).withAuth();

    assertThat(plaintext).isNotEqualTo(encrypted).isNotEqualTo(authenticated);
    assertThat(encrypted).isNotEqualTo(authenticated);
    assertThat(encrypted).isEqualTo(CCMBridge.builder().withNodes(1).withSSL());
    assertThat(encrypted.hashCode())
        .isEqualTo(CCMBridge.builder().withNodes(1).withSSL().hashCode());
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
