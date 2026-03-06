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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

/**
 * Abstract base for testing LWT preserve-replica-order routing on both {@link
 * BasicLoadBalancingPolicy} and {@link DefaultLoadBalancingPolicy}.
 */
public abstract class LwtRoutingTestBase extends LoadBalancingPolicyTestBase {

  protected static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");
  protected static final ByteBuffer ROUTING_KEY = Bytes.fromHexString("0xdeadbeef");

  @Mock protected Request request;
  @Mock protected DefaultSession session;
  @Mock protected Metadata metadata;
  @Mock protected TokenMap tokenMap;
  @Mock protected Token routingToken;

  protected BasicLoadBalancingPolicy policy;

  protected abstract BasicLoadBalancingPolicy createPolicy(
      DriverContext context, String profileName);

  @Before
  @Override
  public void setup() {
    super.setup();
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenAnswer(invocation -> Optional.of(this.tokenMap));

    // Enable remote DC nodes
    when(defaultProfile.getInt(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .thenReturn(2);

    // Configure for PRESERVE_REPLICA_ORDER routing for LWT
    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn("PRESERVE_REPLICA_ORDER");

    // Set up nodes with proper DCs
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc1");
    when(node3.getDatacenter()).thenReturn("dc1");
    when(node4.getDatacenter()).thenReturn("dc2");
    when(node5.getDatacenter()).thenReturn("dc2");

    policy = createPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3,
            UUID.randomUUID(), node4,
            UUID.randomUUID(), node5),
        distanceReporter);
  }

  @Test
  public void should_fallback_to_all_nodes_when_empty_replicas() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY)).willReturn(ImmutableList.of());

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan).containsExactlyInAnyOrder(node1, node2, node3, node4, node5);
  }

  @Test
  public void should_preserve_replica_order_with_single_local_replica() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node2));

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node2);
    assertThat(plan).containsExactlyInAnyOrder(node1, node3, node4, node5);
  }

  @Test
  public void should_preserve_replica_order_with_multiple_local_replicas() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node3, node1, node2));

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node3);
    assertThat(plan.poll()).isEqualTo(node1);
    assertThat(plan.poll()).isEqualTo(node2);
    assertThat(plan).containsExactlyInAnyOrder(node4, node5);
  }

  @Test
  public void should_push_remote_replicas_to_end() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node4, node1, node5, node2));

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node1); // local replica
    assertThat(plan.poll()).isEqualTo(node2); // local replica
    assertThat(plan.poll()).isEqualTo(node4); // remote replica
    assertThat(plan.poll()).isEqualTo(node5); // remote replica
    assertThat(plan.poll()).isEqualTo(node3); // local non-replica
  }

  @Test
  public void should_preserve_replica_order_with_all_remote_replicas() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node5, node4));

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node5);
    assertThat(plan.poll()).isEqualTo(node4);
    assertThat(plan).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_preserve_order_when_no_routing_key() {
    given(request.getRoutingKeyspace()).willReturn(null);
    given(request.getRoutingKey()).willReturn(null);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.REGULAR);

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).isNotNull();
  }

  @Test
  public void should_dispatch_to_preserve_replicas_when_lwt_and_config_set() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1, node2));

    Queue<Node> plan1 = policy.newQueryPlan(request, session);
    Queue<Node> plan2 = policy.newQueryPlan(request, session);
    Queue<Node> plan3 = policy.newQueryPlan(request, session);

    assertThat(plan1).hasSize(5);
    assertThat(plan2).hasSize(5);
    assertThat(plan3).hasSize(5);

    Node[] plan1Array = plan1.toArray(new Node[0]);
    Node[] plan2Array = plan2.toArray(new Node[0]);
    Node[] plan3Array = plan3.toArray(new Node[0]);

    assertThat(plan1Array[0]).isEqualTo(node1);
    assertThat(plan1Array[1]).isEqualTo(node2);
    assertThat(plan2Array[0]).isEqualTo(node1);
    assertThat(plan2Array[1]).isEqualTo(node2);
    assertThat(plan3Array[0]).isEqualTo(node1);
    assertThat(plan3Array[1]).isEqualTo(node2);
  }

  @Test
  public void should_add_non_replicas_after_replicas_in_preserve_mode() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1));

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node1);
    assertThat(plan).containsExactlyInAnyOrder(node2, node3, node4, node5);
  }

  @Test
  public void should_fallback_to_all_live_nodes_when_lwt_has_no_routing_info() {
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(request.getKeyspace()).willReturn(null);
    given(request.getRoutingKeyspace()).willReturn(null);
    given(request.getRoutingKey()).willReturn(null);
    given(request.getRoutingToken()).willReturn(null);

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan).containsExactlyInAnyOrder(node1, node2, node3, node4, node5);
  }

  @Test
  public void
      should_maintain_node_priority_order_local_replicas_then_remote_then_local_non_replicas() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node2, node5));

    Queue<Node> plan = policy.newQueryPlan(request, session);

    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node2); // local replica
    assertThat(plan.poll()).isEqualTo(node5); // remote replica
    assertThat(plan).containsExactlyInAnyOrder(node1, node3, node4);
  }

  @Test
  public void should_rotate_non_replicas_with_controlled_randomness() {
    // Put all nodes in dc1 so we have 3 non-replicas for controlled rotation
    when(node4.getDatacenter()).thenReturn("dc1");

    BasicLoadBalancingPolicy spyPolicy =
        spy(createPolicy(context, DriverExecutionProfile.DEFAULT_NAME));
    spyPolicy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3,
            UUID.randomUUID(), node4),
        distanceReporter);

    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(null); // null key = random rotation
    given(request.getRoutingToken()).willReturn(routingToken); // token for replica lookup
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, routingToken)).willReturn(ImmutableList.of(node1));

    doReturn(0).when(spyPolicy).randomNextInt(3);
    Queue<Node> plan1 = spyPolicy.newQueryPlan(request, session);

    doReturn(1).when(spyPolicy).randomNextInt(3);
    Queue<Node> plan2 = spyPolicy.newQueryPlan(request, session);

    doReturn(2).when(spyPolicy).randomNextInt(3);
    Queue<Node> plan3 = spyPolicy.newQueryPlan(request, session);

    Node[] plan1Array = plan1.toArray(new Node[0]);
    Node[] plan2Array = plan2.toArray(new Node[0]);
    Node[] plan3Array = plan3.toArray(new Node[0]);

    assertThat(plan1Array[0]).isEqualTo(node1);
    assertThat(plan2Array[0]).isEqualTo(node1);
    assertThat(plan3Array[0]).isEqualTo(node1);

    assertThat(plan1Array).isNotEqualTo(plan2Array);
    assertThat(plan2Array).isNotEqualTo(plan3Array);

    assertThat(plan1).hasSize(4);
    assertThat(plan1).containsExactlyInAnyOrder(plan2Array);
    assertThat(plan1).containsExactlyInAnyOrder(plan3Array);
  }

  @Test
  public void should_rotate_non_replicas_consistently_when_routing_key_present() {
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1));

    Queue<Node> plan1 = policy.newQueryPlan(request, session);
    Queue<Node> plan2 = policy.newQueryPlan(request, session);
    Queue<Node> plan3 = policy.newQueryPlan(request, session);

    assertThat(plan1).containsExactly(plan2.toArray(new Node[0]));
    assertThat(plan1).containsExactly(plan3.toArray(new Node[0]));
    assertThat(plan1).hasSize(5);
    assertThat(plan1.poll()).isEqualTo(node1);
  }
}
