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

import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.driver.core.ProtocolVersion.V6;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Tests for the new USE_BETA flag introduced in protocol v5 and Cassandra 3.10. */
@CassandraVersion("3.10")
@CCMConfig(createCluster = false)
public class ProtocolBetaVersionTest extends CCMTestsSupport {

  @BeforeClass
  public void checkNotCassandra4OrHigher() {
    if (ccm().getCassandraVersion().getMajor() > 3) {
      throw new SkipException(
          "ProtocolBetaVersionTest should only be executed against C* versions >= 3.10 and < 4.0");
    }
  }

  /**
   * Verifies that the cluster builder fails when version is explicitly set and user attempts to set
   * beta flag.
   *
   * @jira_ticket JAVA-1248
   */
  @Test(groups = "short")
  public void should_not_initialize_when_version_explicitly_required_and_beta_flag_is_set() {
    try {
      Cluster.builder()
          .addContactPoints(getContactPoints())
          .withPort(ccm().getBinaryPort())
          .withProtocolVersion(V4)
          .allowBetaProtocolVersion()
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage())
          .isEqualTo("Can't use beta flag with initial protocol version of V4");
    }
  }

  /**
   * Verifies that the cluster builder fails when beta flag is set and user attempts to pass a
   * version explicitly.
   *
   * @jira_ticket JAVA-1248
   */
  @Test(groups = "short")
  public void should_not_initialize_when_beta_flag_is_set_and_version_explicitly_required() {
    try {
      Cluster.builder()
          .addContactPoints(getContactPoints())
          .withPort(ccm().getBinaryPort())
          .allowBetaProtocolVersion()
          .withProtocolVersion(V4)
          .build();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage())
          .isEqualTo("Can not set the version explicitly if `allowBetaProtocolVersion` was used.");
    }
  }

  /**
   * Verifies that the driver CANNOT connect to 3.10 with the following combination of options:
   * Version V6 Flag UNSET
   *
   * @jira_ticket JAVA-1248
   */
  @Test(groups = "short")
  public void should_not_connect_when_beta_version_explicitly_required_and_flag_not_set() {
    try {
      Cluster.builder()
          .addContactPoints(getContactPoints())
          .withPort(ccm().getBinaryPort())
          .withProtocolVersion(V6)
          .build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage())
          .startsWith("Can not use V6 protocol version. Newest supported protocol version is: V5");
    }
  }

  /**
   * Verifies that the driver can connect to 3.10 with the following combination of options: Version
   * UNSET Flag SET Expected version: V5
   *
   * @jira_ticket JAVA-1248
   */
  @Test(groups = "short", enabled = false)
  public void should_connect_with_beta_when_no_version_explicitly_required_and_flag_set() {
    Cluster cluster =
        Cluster.builder()
            .addContactPoints(getContactPoints())
            .withPort(ccm().getBinaryPort())
            .allowBetaProtocolVersion()
            .build();
    cluster.connect();
    assertThat(cluster.getConfiguration().getProtocolOptions().getProtocolVersion()).isEqualTo(V6);
  }

  /**
   * Verifies that the driver can connect to 3.10 with the following combination of options: Version
   * UNSET Flag UNSET Expected version: V4
   *
   * <p>This test has been disabled as of driver 3.11 because v5 is not beta anymore in the driver.
   * As a consequence, protocol negotiation without specifying an initial version is not possible
   * anymore against C* >= 3.10 and < 4.0.
   *
   * @jira_ticket JAVA-1248
   */
  @Test(groups = "short", enabled = false)
  public void
      should_connect_after_renegotiation_when_no_version_explicitly_required_and_flag_not_set() {
    Cluster cluster =
        Cluster.builder()
            .addContactPoints(getContactPoints())
            .withPort(ccm().getBinaryPort())
            .build();
    cluster.connect();
    assertThat(cluster.getConfiguration().getProtocolOptions().getProtocolVersion()).isEqualTo(V4);
  }
}
