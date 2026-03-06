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
import com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy.RequestRoutingMethod;
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
public class DefaultLoadBalancingPolicyRequestRoutingTest extends LoadBalancingPolicyTestBase {

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

    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc1");
    when(node3.getDatacenter()).thenReturn("dc1");
  }

  private void initPolicy(String routingMethod) {
    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn(routingMethod);

    policy = new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3),
        distanceReporter);
  }

  @Test
  public void should_return_regular_when_request_is_null() {
    // Given
    initPolicy("REGULAR");

    // When
    RequestRoutingMethod method = policy.getRequestRoutingMethod(null);

    // Then
    assertThat(method).isEqualTo(RequestRoutingMethod.REGULAR);
  }

  @Test
  public void should_return_regular_when_routing_type_is_regular() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.REGULAR);

    // When
    RequestRoutingMethod method = policy.getRequestRoutingMethod(request);

    // Then
    assertThat(method).isEqualTo(RequestRoutingMethod.REGULAR);
  }

  @Test
  public void should_return_regular_for_lwt_when_config_is_regular() {
    // Given
    initPolicy("REGULAR");
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);

    // When
    RequestRoutingMethod method = policy.getRequestRoutingMethod(request);

    // Then
    assertThat(method).isEqualTo(RequestRoutingMethod.REGULAR);
  }

  @Test
  public void should_return_preserve_replica_order_for_lwt_when_config_is_preserve() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);

    // When
    RequestRoutingMethod method = policy.getRequestRoutingMethod(request);

    // Then
    assertThat(method).isEqualTo(RequestRoutingMethod.PRESERVE_REPLICA_ORDER);
  }

  @Test
  public void should_dispatch_to_regular_query_plan_when_request_is_regular() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.REGULAR);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1, node2));

    // When
    Queue<Node> plan1 = policy.newQueryPlan(request, session);
    Queue<Node> plan2 = policy.newQueryPlan(request, session);

    // Then - regular routing shuffles replicas (node1, node2), and also adds the local
    // non-replica node (node3); order may vary between plans but the same three nodes
    // must be present in each plan
    assertThat(plan1).containsExactlyInAnyOrder(node1, node2, node3);
    assertThat(plan2).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_dispatch_to_preserve_query_plan_when_lwt_and_config_preserve() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node2, node1));

    // When
    Queue<Node> plan1 = policy.newQueryPlan(request, session);
    Queue<Node> plan2 = policy.newQueryPlan(request, session);
    Queue<Node> plan3 = policy.newQueryPlan(request, session);

    // Then - preserve routing maintains replica order, non-replicas follow
    assertThat(plan1).containsExactly(node2, node1, node3);
    assertThat(plan2).containsExactly(node2, node1, node3);
    assertThat(plan3).containsExactly(node2, node1, node3);
  }

  @Test
  public void should_dispatch_to_regular_query_plan_when_lwt_but_config_regular() {
    // Given
    initPolicy("REGULAR");
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1, node2));

    // When
    Queue<Node> plan = policy.newQueryPlan(request, session);

    // Then - uses regular routing which may shuffle and add non-replicas
    assertThat(plan).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_handle_null_request_in_new_query_plan() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");

    // When
    Queue<Node> plan = policy.newQueryPlan(null, session);

    // Then - null request should use regular routing
    assertThat(plan).isNotNull();
    assertThat(plan).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_use_regular_routing_for_unknown_routing_type() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    // Use REGULAR as a stand-in for any "unknown" type - the switch has a default case
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.REGULAR);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1));

    // When
    RequestRoutingMethod method = policy.getRequestRoutingMethod(request);

    // Then - defaults to REGULAR for any unrecognized type
    assertThat(method).isEqualTo(RequestRoutingMethod.REGULAR);
  }

  @Test
  public void should_consistently_route_same_request_type() {
    // Given
    initPolicy("PRESERVE_REPLICA_ORDER");
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(request.getRequestRoutingType()).willReturn(RequestRoutingType.LWT);
    given(tokenMap.getReplicasList(KEYSPACE, null, ROUTING_KEY))
        .willReturn(ImmutableList.of(node1, node2, node3));

    // When - call multiple times
    RequestRoutingMethod method1 = policy.getRequestRoutingMethod(request);
    RequestRoutingMethod method2 = policy.getRequestRoutingMethod(request);
    RequestRoutingMethod method3 = policy.getRequestRoutingMethod(request);

    // Then - should always return the same method
    assertThat(method1).isEqualTo(RequestRoutingMethod.PRESERVE_REPLICA_ORDER);
    assertThat(method2).isEqualTo(RequestRoutingMethod.PRESERVE_REPLICA_ORDER);
    assertThat(method3).isEqualTo(RequestRoutingMethod.PRESERVE_REPLICA_ORDER);
  }
}
