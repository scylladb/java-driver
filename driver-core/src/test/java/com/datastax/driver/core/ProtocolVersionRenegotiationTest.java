/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2021 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.ProtocolVersion.V1;
import static com.datastax.driver.core.ProtocolVersion.V5;
import static com.datastax.driver.core.ProtocolVersion.V6;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.SkipException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@CCMConfig(createCluster = false)
public class ProtocolVersionRenegotiationTest extends CCMTestsSupport {

  private ProtocolVersion protocolVersion;

  @BeforeMethod(groups = "short")
  public void setUp() {
    protocolVersion = ccm().getProtocolVersion();
  }

  /** @jira_ticket JAVA-1367 */
  @Test(groups = "short")
  public void should_succeed_when_version_provided_and_matches() {
    Cluster cluster = connectWithVersion(protocolVersion);
    assertThat(actualProtocolVersion(cluster)).isEqualTo(protocolVersion);
  }

  /** @jira_ticket JAVA-1367 */
  @Test(groups = "short", enabled = false /* @IntegrationTestDisabledCassandra3Failure */)
  @CassandraVersion("3.8")
  public void should_fail_when_version_provided_and_too_low_3_8_plus() {
    UnsupportedProtocolVersionException e = connectWithUnsupportedVersion(V1);
    assertThat(e.getUnsupportedVersion()).isEqualTo(V1);
    // post-CASSANDRA-11464: server replies with client's version
    assertThat(e.getServerVersion()).isEqualTo(V1);
  }

  /** @jira_ticket JAVA-1367 */
  @Test(groups = "short")
  public void should_fail_when_version_provided_and_too_high() {
    if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("3.10")) >= 0) {
      throw new SkipException("Server supports protocol V5");
    }
    UnsupportedProtocolVersionException e = connectWithUnsupportedVersion(V5);
    assertThat(e.getUnsupportedVersion()).isEqualTo(V5);
    // see CASSANDRA-11464: for C* < 3.0.9 and 3.8, server replies with its own version;
    // otherwise it replies with the client's version.
    assertThat(e.getServerVersion()).isIn(V5, protocolVersion);
  }

  /** @jira_ticket JAVA-1367 */
  @Test(groups = "short")
  public void should_fail_when_beta_allowed_and_too_high() {
    if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("4.0.0")) >= 0) {
      throw new SkipException("Server supports protocol protocol V6 beta");
    }
    UnsupportedProtocolVersionException e = connectWithUnsupportedBetaVersion();
    assertThat(e.getUnsupportedVersion()).isEqualTo(V6);
  }

  /** @jira_ticket JAVA-1367 */
  @Test(groups = "short", enabled = false /* @IntegrationTestDisabledCassandra3Failure */)
  @CCMConfig(version = "2.1.16", createCluster = false)
  public void should_negotiate_when_no_version_provided() {
    if (protocolVersion.compareTo(ProtocolVersion.NEWEST_SUPPORTED) >= 0) {
      throw new SkipException("Server supports newest protocol version driver supports");
    }
    Cluster cluster = connectWithoutVersion();
    assertThat(actualProtocolVersion(cluster)).isEqualTo(protocolVersion);
  }

  @Test(groups = "short")
  public void should_successfully_negotiate_down_from_newest_supported_version() {
    // By default, the driver will connect with ProtocolVersion.DEFAULT (<=
    // ProtocolVersion.NEWEST_SUPPORTED).
    // This test verifies that the driver can connect starting with the
    // newest supported version, potentially renegotiating the protocol
    // version to a lower version.

    // We will explicitly set a protocol version, so we need to force
    // the driver to negotiate protocol version.
    Cluster.shouldAlwaysNegotiateProtocolVersion = true;

    try {
      Cluster cluster = connectWithVersion(ProtocolVersion.NEWEST_SUPPORTED);
      assertThat(actualProtocolVersion(cluster))
          .isLessThanOrEqualTo(ProtocolVersion.NEWEST_SUPPORTED);
    } catch (RuntimeException e) {
      Cluster.shouldAlwaysNegotiateProtocolVersion = false;
      throw e;
    }

    Cluster.shouldAlwaysNegotiateProtocolVersion = false;
  }

  private UnsupportedProtocolVersionException connectWithUnsupportedVersion(
      ProtocolVersion version) {
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(version)
                .build());
    return initWithUnsupportedVersion(cluster);
  }

  private UnsupportedProtocolVersionException connectWithUnsupportedBetaVersion() {
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .allowBetaProtocolVersion()
                .build());
    return initWithUnsupportedVersion(cluster);
  }

  private UnsupportedProtocolVersionException initWithUnsupportedVersion(Cluster cluster) {
    Throwable t = null;
    try {
      cluster.init();
    } catch (Throwable t2) {
      t = t2;
    }
    if (t instanceof UnsupportedProtocolVersionException) {
      return (UnsupportedProtocolVersionException) t;
    } else {
      throw new AssertionError("Expected UnsupportedProtocolVersionException, got " + t);
    }
  }

  private Cluster connectWithVersion(ProtocolVersion version) {
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(version)
                .build());
    cluster.init();
    return cluster;
  }

  private Cluster connectWithoutVersion() {
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build());
    cluster.init();
    return cluster;
  }

  private ProtocolVersion actualProtocolVersion(Cluster cluster) {
    return cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
  }
}
