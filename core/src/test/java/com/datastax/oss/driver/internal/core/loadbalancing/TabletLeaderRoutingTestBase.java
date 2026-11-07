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
package com.datastax.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public abstract class TabletLeaderRoutingTestBase extends LoadBalancingPolicyTestBase {
  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");
  private static final CqlIdentifier TABLE = CqlIdentifier.fromInternal("t");
  @Mock private BoundStatement statement;
  @Mock private PreparedStatement prepared;
  @Mock private DefaultSession session;
  @Mock private Metadata metadata;
  @Mock private KeyspaceMetadata keyspace;
  @Mock private TabletMap tablets;
  @Mock private Tablet tablet;
  @Mock private NodeDistanceEvaluator evaluator;

  protected abstract BasicLoadBalancingPolicy createPolicy();

  @Before
  @Override
  public void setup() {
    super.setup();
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.of(keyspace));
    when(metadata.getTabletMap()).thenReturn(Optional.of(tablets));
    when(keyspace.isUsingTablets()).thenReturn(true);
    when(keyspace.isGloballyConsistent()).thenReturn(true);
    when(statement.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(statement.getRoutingTable()).thenReturn(TABLE);
    when(statement.getRoutingToken()).thenReturn(new Murmur3Token(5));
    when(statement.getPreparedStatement()).thenReturn(prepared);
    when(statement.getConsistencyLevel()).thenReturn(DefaultConsistencyLevel.QUORUM);
    when(prepared.getQuery()).thenReturn("SELECT * FROM ks.t WHERE p=?");
    when(tablets.getTablet(KEYSPACE, TABLE, 5)).thenReturn(tablet);
    when(tablet.getTabletVersion()).thenReturn(0L);
    when(tablet.getLeader()).thenReturn(node2);
    when(tablet.getReplicaNodesList()).thenReturn(ImmutableList.of(node2, node1));
  }

  private BasicLoadBalancingPolicy init() {
    BasicLoadBalancingPolicy policy = createPolicy();
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);
    return policy;
  }

  @Test
  public void should_prefer_leader_for_strong_reads() {
    Queue<Node> plan = init().newQueryPlan(statement, session);
    assertThat(plan.poll()).isSameAs(node2);
    assertThat(plan).containsExactlyInAnyOrder(node1, node3);
  }

  @Test
  public void should_use_replica_routing_for_weak_reads_with_cql_comments() {
    for (DefaultConsistencyLevel consistency :
        new DefaultConsistencyLevel[] {
          DefaultConsistencyLevel.ONE, DefaultConsistencyLevel.LOCAL_ONE
        }) {
      when(statement.getConsistencyLevel()).thenReturn(consistency);
      when(prepared.getQuery())
          .thenReturn(" /* comment */ -- line\n // line\n select * FROM ks.t WHERE p=?");
      assertThat(init().getTabletLeader(statement, session)).isNull();
    }
  }

  @Test
  public void should_prefer_leader_for_writes_at_one() {
    when(statement.getConsistencyLevel()).thenReturn(DefaultConsistencyLevel.ONE);
    when(prepared.getQuery()).thenReturn("INSERT INTO ks.t(p) VALUES(?)");
    assertThat(init().newQueryPlan(statement, session).poll()).isSameAs(node2);
  }

  @Test
  public void should_ignore_v1_and_eventual_keyspace_leader_order() {
    when(tablet.getTabletVersion()).thenReturn(null);
    assertThat(init().getTabletLeader(statement, session)).isNull();
    when(tablet.getTabletVersion()).thenReturn(0L);
    when(keyspace.isGloballyConsistent()).thenReturn(false);
    assertThat(init().getTabletLeader(statement, session)).isNull();
  }

  @Test
  public void should_not_add_remote_leader_when_dc_failover_disabled() {
    when(node2.getDatacenter()).thenReturn("dc2");
    assertThat(init().newQueryPlan(statement, session)).containsExactlyInAnyOrder(node1, node3);
  }

  @Test
  public void should_promote_remote_leader_admitted_by_dc_failover() {
    when(node2.getDatacenter()).thenReturn("dc2");
    when(defaultProfile.getInt(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .thenReturn(1);
    assertThat(init().newQueryPlan(statement, session).poll()).isSameAs(node2);
  }

  @Test
  public void should_never_add_ignored_leader() {
    when(context.getNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(evaluator);
    when(evaluator.evaluateDistance(node1, "dc1")).thenReturn(NodeDistance.LOCAL);
    when(evaluator.evaluateDistance(node2, "dc1")).thenReturn(NodeDistance.IGNORED);
    when(evaluator.evaluateDistance(node3, "dc1")).thenReturn(NodeDistance.LOCAL);
    assertThat(init().newQueryPlan(statement, session)).containsExactlyInAnyOrder(node1, node3);
  }
}
