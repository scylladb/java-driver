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
  public void should_preserve_replica_order_with_empty_replicas() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY)).willReturn(ImmutableList.of());

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then
    assertThat(plan).isEmpty();
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

    // Then
    assertThat(plan).containsExactly(node2);
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

    // Then - order preserved exactly as returned from token map
    assertThat(plan).containsExactly(node3, node1, node2);
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

    // Then - local replicas first (preserving their order), remote replicas last (preserving their
    // order)
    assertThat(plan).containsExactly(node1, node2, node4, node5);
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

    // Then - all remote replicas, order preserved
    assertThat(plan).containsExactly(node5, node4);
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
    // The method should handle null localDc gracefully and just return replicas as-is
    Queue<Node> plan = policy.newQueryPlanPreserveReplicas(request, session);

    // Then - returns all replicas in order when localDc is not defined
    assertThat(plan).containsExactly(node1, node2);
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

    // Then - verify it used preserve replica order (no shuffling)
    // Call multiple times to ensure order is always preserved (not shuffled)
    Queue<Node> plan2 = policy.newQueryPlan(request, session);
    Queue<Node> plan3 = policy.newQueryPlan(request, session);

    assertThat(plan).containsExactly(node1, node2);
    assertThat(plan2).containsExactly(node1, node2);
    assertThat(plan3).containsExactly(node1, node2);
  }

  @Test
  public void should_not_add_non_replicas_in_preserve_mode() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    // Only node1 is a replica
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - only the replica is in the plan, other live nodes are NOT added
    assertThat(plan).containsExactly(node1);
  }
}
