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
 * Copyright (C) 2026 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assume.assumeTrue;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class LWTLoadBalancingMultiDcIT {
  private static final String LOCAL_DC = "dc1";
  private static final String KEYSPACE = "test";
  private static final String LOCAL_SERIAL_PROFILE = "local-serial";

  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder().withNodes(2, 1).build(); // 2 nodes in DC1, 1 node in DC2

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withKeyspace(false)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, LOCAL_DC)
                  .withString(
                      DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD,
                      "PRESERVE_REPLICA_ORDER")
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .startProfile(LOCAL_SERIAL_PROFILE)
                  .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_SERIAL")
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  public static final int FIRST_TEST_PARTITION_KEY = 4242;
  public static final int SECOND_TEST_PARTITION_KEY = 4343;
  public static final int THIRD_TEST_PARTITION_KEY = 4444;
  public static final int NUM_TEST_ITERATIONS = 30;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    // Create multi-DC keyspace and table similarly to DefaultLoadBalancingPolicyIT.
    if (CcmBridge.isDistributionOf(BackendType.SCYLLA)
        && ((CcmBridge.SCYLLA_ENTERPRISE
                && CcmBridge.getDistributionVersion().compareTo(Version.parse("2024.2.0")) >= 0)
            || (!CcmBridge.SCYLLA_ENTERPRISE
                && CcmBridge.getDistributionVersion().compareTo(Version.parse("6.1.0")) >= 0))) {
      session.execute(
          "CREATE KEYSPACE test "
              + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1} "
              + "AND tablets = { 'enabled': false }");
    } else {
      session.execute(
          "CREATE KEYSPACE test "
              + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}");
    }

    session.execute("CREATE TABLE test.foo (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

    // Wait for schema readiness
    await()
        .pollInterval(200, TimeUnit.MILLISECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(session.getMetadata().getKeyspace(KEYSPACE)).isPresent();
              TokenMap tm = session.getMetadata().getTokenMap().get();
              ByteBuffer routingKey =
                  TypeCodecs.INT.encodePrimitive(FIRST_TEST_PARTITION_KEY, ProtocolVersion.DEFAULT);
              Set<Node> replicas =
                  new HashSet<>(tm.getReplicasList(CqlIdentifier.fromCql(KEYSPACE), routingKey));
              assertThat(replicas).hasSize(3); // RF 2 in dc1, 1 in dc2
              assertThat(replicas.stream().filter(n -> LOCAL_DC.equals(n.getDatacenter())))
                  .hasSizeGreaterThanOrEqualTo(1);
            });
  }

  @Test
  public void should_route_lwt_to_local_dc_replicas() {
    int pk = FIRST_TEST_PARTITION_KEY;
    CqlIdentifier keyspace = CqlIdentifier.fromCql(KEYSPACE);
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(pk, ProtocolVersion.DEFAULT);

    TokenMap tokenMap = SESSION_RULE.session().getMetadata().getTokenMap().get();
    Set<Node> localReplicas = new HashSet<>();
    Set<Node> allReplicas = new HashSet<>(tokenMap.getReplicasList(keyspace, routingKey));
    for (Node replica : allReplicas) {
      if (LOCAL_DC.equals(replica.getDatacenter())) {
        localReplicas.add(replica);
      }
    }
    assertThat(localReplicas).isNotEmpty();

    PreparedStatement lwt =
        SESSION_RULE
            .session()
            .prepare("INSERT INTO test.foo (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS");
    // Cassandra does not expose LWT flag via prepare metadata; driver may not detect LWT.
    if (!CcmBridge.isDistributionOf(BackendType.CASSANDRA)) {
      assertThat(lwt.isLWT()).isTrue();
    }

    Set<Node> coordinators = new HashSet<>();
    Set<String> coordinatorDcs = new HashSet<>();
    for (int i = 0; i < NUM_TEST_ITERATIONS; i++) {
      ResultSet result = SESSION_RULE.session().execute(lwt.bind(pk, i, 7));
      Node coord = result.getExecutionInfo().getCoordinator();
      coordinators.add(coord);
      coordinatorDcs.add(coord.getDatacenter());
    }

    assertThat(coordinators).isSubsetOf(allReplicas);
    assertThat(coordinators).isSubsetOf(localReplicas);
    assertThat(coordinatorDcs).containsOnly(LOCAL_DC);
  }

  @Test
  public void should_route_lwt_batch_to_local_dc_replicas() {
    int pk = SECOND_TEST_PARTITION_KEY;
    CqlIdentifier keyspace = CqlIdentifier.fromCql(KEYSPACE);
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(pk, ProtocolVersion.DEFAULT);

    TokenMap tokenMap = SESSION_RULE.session().getMetadata().getTokenMap().get();
    Set<Node> localReplicas = new HashSet<>();
    Set<Node> allReplicas = new HashSet<>(tokenMap.getReplicasList(keyspace, routingKey));
    for (Node replica : allReplicas) {
      if (LOCAL_DC.equals(replica.getDatacenter())) {
        localReplicas.add(replica);
      }
    }
    assertThat(localReplicas).isNotEmpty();

    PreparedStatement lwt =
        SESSION_RULE
            .session()
            .prepare("INSERT INTO test.foo (pk, ck, v) VALUES (?, ?, ?) IF NOT EXISTS");
    PreparedStatement nonLwtPrepared =
        SESSION_RULE.session().prepare("INSERT INTO test.foo (pk, ck, v) VALUES (?, ?, ?)");

    // Run a bunch of times to exercise load balancing.
    Set<Node> coordinators = new HashSet<>();
    Set<String> coordinatorDcs = new HashSet<>();
    for (int i = 0; i < NUM_TEST_ITERATIONS; i++) {
      BatchStatementBuilder builder =
          BatchStatement.builder(BatchType.UNLOGGED)
              .setRoutingKeyspace(keyspace)
              .setRoutingKey(routingKey)
              .addStatement(nonLwtPrepared.bind(pk, 0, 101))
              .addStatement(lwt.bind(pk, i, 202));
      // Ensure LWT routing type on Cassandra where detection may be absent
      if (CcmBridge.isDistributionOf(BackendType.CASSANDRA)) {
        builder = builder.setRequestRoutingType(RequestRoutingType.LWT);
      }
      BatchStatement batch = builder.build();
      assertThat(batch.isLWT()).isTrue();

      ResultSet result = SESSION_RULE.session().execute(batch);
      Node coord = result.getExecutionInfo().getCoordinator();
      coordinators.add(coord);
      coordinatorDcs.add(coord.getDatacenter());
    }

    assertThat(coordinators).isSubsetOf(allReplicas);
    assertThat(coordinators).isSubsetOf(localReplicas);
    assertThat(coordinatorDcs).containsOnly(LOCAL_DC);
  }

  @Test
  public void should_route_prepared_local_serial_simple_select_as_lwt() {
    assumeTrue(CcmBridge.isDistributionOf(BackendType.SCYLLA));

    CqlSession session = SESSION_RULE.session();
    SimpleStatement simpleSelect =
        SimpleStatement.builder("SELECT * FROM test.foo WHERE pk = ? AND ck = ?")
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();
    PreparedStatement select = session.prepare(simpleSelect);
    BoundStatement statement = select.bind(THIRD_TEST_PARTITION_KEY, 0);

    assertThat(simpleSelect.isLWT()).isFalse();
    assertThat(simpleSelect.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);
    assertThat(simpleSelect.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL);

    assertThat(select.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);

    assertThat(statement.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);
    assertThat(statement.getRoutingKeyspace()).isEqualTo(CqlIdentifier.fromCql(KEYSPACE));
    assertThat(statement.getRoutingKey()).isNotNull();
    assertThat(statement.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL);

    ResultSet result = session.execute(statement);
    assertThat(result.getExecutionInfo().getCoordinator()).isNotNull();
  }

  @Test
  public void should_route_prepared_profiled_local_serial_simple_select_with_lwt_policy() {
    assumeTrue(CcmBridge.isDistributionOf(BackendType.SCYLLA));

    CqlSession session = SESSION_RULE.session();
    SimpleStatement simpleSelect =
        SimpleStatement.builder("SELECT * FROM test.foo WHERE pk = ? AND ck = ?")
            .setExecutionProfileName(LOCAL_SERIAL_PROFILE)
            .build();
    PreparedStatement select = session.prepare(simpleSelect);
    BoundStatement statement = select.bind(THIRD_TEST_PARTITION_KEY, 0);

    assertThat(simpleSelect.isLWT()).isFalse();
    assertThat(simpleSelect.getRequestRoutingType()).isNull();
    assertThat(simpleSelect.getConsistencyLevel()).isNull();

    assertThat(select.getRequestRoutingType()).isNull();

    assertThat(statement.getRequestRoutingType()).isNull();
    assertThat(statement.getExecutionProfileName()).isEqualTo(LOCAL_SERIAL_PROFILE);
    assertThat(statement.getRoutingKeyspace()).isEqualTo(CqlIdentifier.fromCql(KEYSPACE));
    assertThat(statement.getRoutingKey()).isNotNull();
    assertThat(statement.getConsistencyLevel()).isNull();

    ResultSet result = session.execute(statement);
    assertThat(result.getExecutionInfo().getCoordinator()).isNotNull();
  }
}
