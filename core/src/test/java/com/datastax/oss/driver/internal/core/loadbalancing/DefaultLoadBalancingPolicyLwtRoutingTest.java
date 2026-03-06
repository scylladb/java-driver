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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultLoadBalancingPolicyLwtRoutingTest extends LoadBalancingPolicyTestBase {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");
  private static final ByteBuffer ROUTING_KEY = Bytes.fromHexString("0xdeadbeef");

  @Mock protected Request request;
  @Mock protected DefaultSession session;
  @Mock protected Metadata metadata;
  @Mock protected TokenMap tokenMap;
  @Mock protected Token routingToken;

  private DefaultLoadBalancingPolicy policy;

  @Before
  @Override
  public void setup() {
    super.setup();
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenAnswer(invocation -> Optional.of(this.tokenMap));

    // Set up nodes with proper DCs
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc1");
    when(node3.getDatacenter()).thenReturn("dc1");
    when(node4.getDatacenter()).thenReturn("dc2");
    when(node5.getDatacenter()).thenReturn("dc2");

    // Configure for PRESERVE_REPLICA_ORDER routing for LWT
    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn("PRESERVE_REPLICA_ORDER");

    policy = new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
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
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY)).willReturn(ImmutableList.of());

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - should fallback to all live nodes in local DC when replicas are empty
    assertThat(plan).containsExactly(node1, node2, node3, node4);
  }

  @Test
  public void should_preserve_replica_order_with_single_local_replica() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node2));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - replica first, then non-replicas with rotation
    assertThat(plan).hasSize(4);
    assertThat(plan.poll()).isEqualTo(node2); // replica first
    // Remaining nodes are non-replicas: node1, node3, node4 (in some rotated order)
    assertThat(plan).containsExactlyInAnyOrder(node1, node3, node4);
  }

  @Test
  public void should_preserve_replica_order_with_multiple_local_replicas() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node3, node1, node2));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - replicas preserved exactly as returned from token map, then non-replicas
    assertThat(plan).hasSize(4);
    assertThat(plan.poll()).isEqualTo(node3); // first replica
    assertThat(plan.poll()).isEqualTo(node1); // second replica
    assertThat(plan.poll()).isEqualTo(node2); // third replica
    // Last node is node4 (the only non-replica)
    assertThat(plan.poll()).isEqualTo(node4);
  }

  @Test
  public void should_push_remote_replicas_to_end() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    // Token map returns replicas in mixed order: remote, local, remote, local
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node4, node1, node5, node2));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - local replicas first (preserving their order), remote replicas next, then local
    // non-replicas
    assertThat(plan).hasSize(5); // All 5 nodes (4 replicas + 1 non-replica node3)
    assertThat(plan.poll()).isEqualTo(node1); // local replica 1 (from dc1)
    assertThat(plan.poll()).isEqualTo(node2); // local replica 2 (from dc1)
    assertThat(plan.poll()).isEqualTo(node4); // remote replica 1 (from dc2)
    assertThat(plan.poll()).isEqualTo(node5); // remote replica 2 (from dc2)
    assertThat(plan.poll()).isEqualTo(node3); // local non-replica (from dc1)
  }

  @Test
  public void should_preserve_replica_order_with_all_remote_replicas() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node5, node4));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - remote replicas first (order preserved), then local non-replicas
    assertThat(plan).hasSize(5); // All 5 nodes (2 replicas + 3 local non-replicas)
    assertThat(plan.poll()).isEqualTo(node5); // remote replica 1
    assertThat(plan.poll()).isEqualTo(node4); // remote replica 2
    // Remaining 3 nodes are local non-replicas: node1, node2, node3 (rotated order)
    assertThat(plan).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_handle_null_local_datacenter() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);

    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1, node2));

    // When - calling with request that might not have local DC set
    // The method should handle null localDc gracefully
    Queue<Node> plan = policy.newQueryPlanPreserveReplicas(request, session);

    // Then - returns replicas first, then non-replicas when localDc is not defined
    assertThat(plan).hasSize(5); // All 5 nodes (2 replicas + 3 non-replicas)
    assertThat(plan.poll()).isEqualTo(node1); // replica 1
    assertThat(plan.poll()).isEqualTo(node2); // replica 2
    // Remaining 3 nodes are non-replicas: node3, node4, node5 (rotated order)
    assertThat(plan).containsExactlyInAnyOrder(node3, node4, node5);
  }

  @Test
  public void should_preserve_order_when_no_routing_key() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(null);
    given(request.getRoutingKey()).willReturn(null);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.REGULAR);

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - with no routing key, no replicas identified, falls back to empty or default behavior
    // This tests the edge case where getReplicas returns empty list
    assertThat(plan).isNotNull();
  }

  @Test
  public void should_dispatch_to_preserve_replicas_when_lwt_and_config_set() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1, node2));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - verify it preserves replica order with consistent rotation
    // Call multiple times to ensure replica order is preserved and rotation is consistent
    Queue<Node> plan2 = policy.newQueryPlan(request, session);
    Queue<Node> plan3 = policy.newQueryPlan(request, session);

    // All plans should have same size and same replica order
    assertThat(plan).hasSize(5); // 2 replicas + 3 non-replicas
    assertThat(plan2).hasSize(5);
    assertThat(plan3).hasSize(5);

    // First two nodes should always be the replicas in order
    Node[] planArray = plan.toArray(new Node[0]);
    Node[] plan2Array = plan2.toArray(new Node[0]);
    Node[] plan3Array = plan3.toArray(new Node[0]);

    assertThat(planArray[0]).isEqualTo(node1); // first replica
    assertThat(planArray[1]).isEqualTo(node2); // second replica
    assertThat(plan2Array[0]).isEqualTo(node1);
    assertThat(plan2Array[1]).isEqualTo(node2);
    assertThat(plan3Array[0]).isEqualTo(node1);
    assertThat(plan3Array[1]).isEqualTo(node2);
  }

  @Test
  public void should_add_non_replicas_after_replicas_in_preserve_mode() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    // Only node1 is a replica
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - replica first, then non-replicas are added with rotation
    assertThat(plan).hasSize(4);
    assertThat(plan.poll()).isEqualTo(node1); // replica first
    // Remaining nodes are non-replicas: node2, node3, node4 (in rotated order)
    assertThat(plan).containsExactlyInAnyOrder(node2, node3, node4);
  }

  @Test
  public void should_fallback_to_all_live_nodes_when_lwt_prepared_statement_has_no_routing_info() {
    // Given - LWT request with PRESERVE_REPLICA_ORDER and no routing information
    given(context.getMetadataManager()).willReturn(metadataManager);
    given(metadataManager.getMetadata()).willReturn(metadata);
    given(metadata.getTokenMap()).willReturn(Optional.of(tokenMap));
    given(metadata.getTabletMap()).willReturn(Optional.empty());

    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    // Simulate prepared statement scenario where routing info is not available
    given(request.getKeyspace()).willReturn(null);
    given(request.getRoutingKeyspace()).willReturn(null);
    given(request.getRoutingKey()).willReturn(null);
    given(request.getRoutingToken()).willReturn(null);

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - should fallback to all live nodes in local DC to prevent "No node available" error
    assertThat(plan).containsExactly(node1, node2, node3, node4);
  }

  @Test
  public void
      should_maintain_node_priority_order_local_replicas_then_remote_then_local_non_replicas() {
    // Given - mixed replica scenario to test full priority ordering
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    // Mix of local and remote replicas: node2, node5 are replicas
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node2, node5));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - verify priority: local replica, remote replica, local non-replicas
    assertThat(plan).hasSize(5);
    assertThat(plan.poll()).isEqualTo(node2); // local replica
    assertThat(plan.poll()).isEqualTo(node5); // remote replica
    // Remaining are local non-replicas: node1, node3, node4 (in some rotated order)
    assertThat(plan).containsExactlyInAnyOrder(node1, node3, node4);
  }

  @Test
  public void should_rotate_non_replicas_with_controlled_randomness() {
    // Given - spy on policy to control randomness
    DefaultLoadBalancingPolicy spyPolicy =
        spy(new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME));
    spyPolicy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3,
            UUID.randomUUID(), node4),
        distanceReporter);

    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(null); // No routing key = random rotation
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, null)).willReturn(ImmutableList.of(node1));

    // Mock different random values to test rotation
    doReturn(0).when(spyPolicy).randomNextInt(3); // First call: no rotation
    Queue<Node> plan1 = spyPolicy.newQueryPlan(request, session);

    doReturn(1).when(spyPolicy).randomNextInt(3); // Second call: rotate by 1
    Queue<Node> plan2 = spyPolicy.newQueryPlan(request, session);

    doReturn(2).when(spyPolicy).randomNextInt(3); // Third call: rotate by 2
    Queue<Node> plan3 = spyPolicy.newQueryPlan(request, session);

    // Then - replica always first, non-replicas rotated differently
    Node[] plan1Array = plan1.toArray(new Node[0]);
    Node[] plan2Array = plan2.toArray(new Node[0]);
    Node[] plan3Array = plan3.toArray(new Node[0]);

    assertThat(plan1Array[0]).isEqualTo(node1); // replica always first
    assertThat(plan2Array[0]).isEqualTo(node1);
    assertThat(plan3Array[0]).isEqualTo(node1);

    // Verify different rotations occurred
    assertThat(plan1Array).isNotEqualTo(plan2Array);
    assertThat(plan2Array).isNotEqualTo(plan3Array);

    // All plans should contain same nodes, just in different order
    assertThat(plan1).hasSize(4);
    assertThat(plan1).containsExactlyInAnyOrder(plan2Array);
    assertThat(plan1).containsExactlyInAnyOrder(plan3Array);
  }

  @Test
  public void should_rotate_non_replicas_consistently_when_routing_key_present() {
    // Given - LWT request with routing key for consistent rotation
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey())
        .willReturn(ROUTING_KEY); // Fixed routing key = consistent rotation
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1));

    // When - call multiple times with same routing key
    Queue<Node> plan1 = policy.newQueryPlan(request, session);
    Queue<Node> plan2 = policy.newQueryPlan(request, session);
    Queue<Node> plan3 = policy.newQueryPlan(request, session);

    // Then - should get identical order every time (consistent rotation)
    assertThat(plan1).containsExactly(plan2.toArray(new Node[0]));
    assertThat(plan1).containsExactly(plan3.toArray(new Node[0]));
    assertThat(plan1).hasSize(4);
    assertThat(plan1.poll()).isEqualTo(node1); // replica always first
  }
}
