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
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;

import com.datastax.driver.core.utils.Bytes;
import com.google.common.base.Throwables;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@CCMConfig(
    // force the initial token to a non-min value to validate that the single range will always be
    // ]minToken, minToken]
    config = "initial_token:1",
    clusterProvider = "createClusterBuilderNoDebouncing")
public class SingleTokenIntegrationTest extends CCMTestsSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleTokenIntegrationTest.class);

  /**
   * Override to create the keyspace with tablets disabled when running against Scylla. This test
   * exercises token-range and token-map based replica lookup (getReplicasList with table == null).
   * With tablets enabled (the default on modern Scylla), replica placement is controlled by the
   * tablet map and getReplicasList returns an empty list when the table is unknown, breaking the
   * test assertions. Cassandra does not support the tablets property.
   */
  @Override
  protected void initTestKeyspace() {
    try {
      keyspace = TestUtils.generateIdentifier("ks_");
      LOGGER.debug("Using keyspace " + keyspace);
      boolean isScylla = Objects.nonNull(ccm().getScyllaVersion());
      session()
          .execute(
              String.format(
                  "CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy',"
                      + " 'datacenter1': 1}"
                      + (isScylla ? " AND tablets = {'enabled': false}" : ""),
                  keyspace));
      useKeyspace(keyspace);
    } catch (Exception e) {
      errorOut();
      LOGGER.error("Could not create test keyspace", e);
      Throwables.propagate(e);
    }
  }

  /** JAVA-684: Empty TokenRange returned in a one token cluster */
  @Test(groups = "short")
  public void should_return_single_non_empty_range_when_cluster_has_one_single_token() {
    cluster().manager.controlConnection.refreshNodeListAndTokenMap();
    Metadata metadata = cluster().getMetadata();
    Set<TokenRange> tokenRanges = metadata.getTokenRanges();
    assertThat(tokenRanges).hasSize(1);
    TokenRange tokenRange = tokenRanges.iterator().next();
    assertThat(tokenRange)
        .startsWith(Token.M3PToken.FACTORY.minToken())
        .endsWith(Token.M3PToken.FACTORY.minToken())
        .isNotEmpty()
        .isNotWrappedAround();

    Set<Host> hostsForRange = metadata.getReplicas(keyspace, tokenRange);
    Host host1 = TestUtils.findHost(cluster(), 1);
    assertThat(hostsForRange).containsOnly(host1);

    List<Host> hostsForRangeList = metadata.getReplicasList(keyspace, tokenRange);
    assertThat(hostsForRangeList).containsOnly(host1);

    ByteBuffer randomPartitionKey = Bytes.fromHexString("0xCAFEBABE");
    Set<Host> hostsForKey = metadata.getReplicas(keyspace, null, randomPartitionKey);
    assertThat(hostsForKey).containsOnly(host1);

    List<Host> hostsForKeyList = metadata.getReplicasList(keyspace, null, null, randomPartitionKey);
    assertThat(hostsForKeyList).containsOnly(host1);

    Set<TokenRange> rangesForHost = metadata.getTokenRanges(keyspace, host1);
    assertThat(rangesForHost).containsOnly(tokenRange);
  }
}
