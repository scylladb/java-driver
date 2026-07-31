/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.policies;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CCMConfig;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TestUtils;
import com.google.common.base.Throwables;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

/**
 * Integration tests verifying that statements with SERIAL/LOCAL_SERIAL consistency level are routed
 * through the LWT load-balancing path (PRESERVE_REPLICA_ORDER).
 */
@CCMConfig(numberOfNodes = 3)
public class LWTLoadBalancingTest extends CCMTestsSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(LWTLoadBalancingTest.class);

  /** Equal to the node count, so that every node is a replica of every partition. */
  private static final int REPLICATION_FACTOR = 3;

  private static final int EXECUTIONS = 30;

  @Override
  public Cluster.Builder createClusterBuilder() {
    return Cluster.builder()
        .withLoadBalancingPolicy(
            new TokenAwarePolicy(new RoundRobinPolicy(), TokenAwarePolicy.ReplicaOrdering.RANDOM));
  }

  /**
   * Override to create the keyspace with a replication factor greater than 1. The default test
   * keyspace created by {@link CCMTestsSupport} is hardcoded to RF=1, and with a single replica per
   * partition "the first replica" is trivially unique — every assertion below would hold under
   * {@code REGULAR} routing too, so the tests could not tell {@code PRESERVE_REPLICA_ORDER} apart
   * from {@code RANDOM}.
   *
   * <p>Tablets are disabled when running against Scylla: with tablets enabled, replica placement
   * comes from the tablet map, which is empty until it has been learned from a misrouted query, and
   * an empty replica list makes the LWT query plan fall back to the child policy — a non-replica
   * coordinator on the first execution. Cassandra does not support the tablets property.
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
                      + " 'datacenter1': %d}"
                      + (isScylla ? " AND tablets = {'enabled': false}" : ""),
                  keyspace,
                  REPLICATION_FACTOR));
      useKeyspace(keyspace);
    } catch (Exception e) {
      errorOut();
      LOGGER.error("Could not create test keyspace", e);
      Throwables.propagate(e);
    }
  }

  @Override
  public void onTestContextInitialized() {
    execute("CREATE TABLE IF NOT EXISTS test_lwt (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    for (int i = 0; i < 10; i++) {
      execute(String.format("INSERT INTO test_lwt (pk, ck, v) VALUES (%d, %d, %d)", i, 0, i));
    }
  }

  @Test(groups = "short")
  public void should_route_local_serial_select_through_lwt_path() {
    Session session = session();

    SimpleStatement simpleSelect =
        new SimpleStatement("SELECT * FROM test_lwt WHERE pk = ? AND ck = ?");
    simpleSelect.setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

    PreparedStatement preparedSelect = session.prepare(simpleSelect);

    // Verify statement properties
    assertThat(simpleSelect.isLWT()).isFalse();
    assertThat(simpleSelect.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);

    // With PRESERVE_REPLICA_ORDER, the first replica is deterministic for a given partition key,
    // so every execution should hit the same coordinator. Contrast with the non-serial control in
    // should_spread_non_serial_select_across_replicas, which shares the same statement and policy.
    assertThat(collectCoordinators(session, preparedSelect, 1)).hasSize(1);
  }

  @Test(groups = "short")
  public void should_route_serial_select_through_lwt_path() {
    Session session = session();

    SimpleStatement simpleSelect =
        new SimpleStatement("SELECT * FROM test_lwt WHERE pk = ? AND ck = ?");
    simpleSelect.setConsistencyLevel(ConsistencyLevel.SERIAL);

    PreparedStatement preparedSelect = session.prepare(simpleSelect);

    // With PRESERVE_REPLICA_ORDER, the first replica is deterministic for a given partition key.
    assertThat(collectCoordinators(session, preparedSelect, 2)).hasSize(1);
  }

  /**
   * Control for the two tests above. This is the same statement against the same table, executed by
   * the same {@code TokenAwarePolicy(RoundRobinPolicy, RANDOM)} — only the consistency level
   * differs. A non-serial level takes the {@code REGULAR} routing path, which shuffles the replicas
   * on every query, so the coordinator must vary. If this test ever collapses to a single
   * coordinator as well, the {@code hasSize(1)} assertions above have stopped proving anything
   * about {@code PRESERVE_REPLICA_ORDER}.
   */
  @Test(groups = "short")
  public void should_spread_non_serial_select_across_replicas() {
    Session session = session();

    SimpleStatement simpleSelect =
        new SimpleStatement("SELECT * FROM test_lwt WHERE pk = ? AND ck = ?");
    simpleSelect.setConsistencyLevel(ConsistencyLevel.ONE);

    PreparedStatement preparedSelect = session.prepare(simpleSelect);
    BoundStatement boundSelect = preparedSelect.bind(3, 0);

    assertThat(boundSelect.isLWT()).isFalse();
    assertThat(boundSelect.getConsistencyLevel().isSerial()).isFalse();

    // Uniform over REPLICATION_FACTOR replicas across EXECUTIONS queries, so the probability of a
    // false failure here is REPLICATION_FACTOR^(1 - EXECUTIONS).
    assertThat(collectCoordinators(session, preparedSelect, 3).size()).isGreaterThan(1);
  }

  /**
   * Executes {@code prepared} against partition {@code pk} {@link #EXECUTIONS} times and returns
   * the distinct coordinators used.
   *
   * <p>A fresh {@link BoundStatement} is bound for every execution on purpose. {@link
   * PagingOptimizingLoadBalancingPolicy}, which the driver wraps around the configured policy,
   * returns {@code Statement.getLastHost()} ahead of the real query plan, and that field is set on
   * every successful {@code BoundStatement} execution. Reusing a single instance would therefore
   * pin the coordinator after the first query and make every assertion in this class hold
   * regardless of how routing actually behaves.
   */
  private static Set<InetSocketAddress> collectCoordinators(
      Session session, PreparedStatement prepared, int pk) {
    Set<InetSocketAddress> coordinators = new HashSet<>();
    for (int i = 0; i < EXECUTIONS; i++) {
      ResultSet rs = session.execute(prepared.bind(pk, 0));
      Host coordinator = rs.getExecutionInfo().getQueriedHost();
      assertThat(coordinator).isNotNull();
      coordinators.add(coordinator.getEndPoint().resolve());
    }
    return coordinators;
  }
}
