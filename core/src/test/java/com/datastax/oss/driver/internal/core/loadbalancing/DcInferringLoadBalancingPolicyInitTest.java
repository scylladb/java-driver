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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DcInferringLoadBalancingPolicyInitTest extends LoadBalancingPolicyTestBase {

  @Test
  public void should_use_local_dc_if_provided_via_config() {
    // Given
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    // the parent class sets the config option to "dc1"
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_use_local_dc_if_provided_via_context() {
    // Given
    when(context.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME)).thenReturn("dc1");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    // note: programmatic takes priority, the config won't even be inspected so no need to stub the
    // option to null
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
    verify(defaultProfile, never())
        .getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
  }

  @Test
  public void
      should_infer_local_dc_from_cluster_nodes_if_not_configured_and_no_control_connection() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_include_nodes_from_local_dc() {
    // Given
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(node1.getState()).thenReturn(NodeState.UP);
    when(node2.getState()).thenReturn(NodeState.DOWN);
    when(node3.getState()).thenReturn(NodeState.UNKNOWN);
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    // Set distance for all nodes in the local DC
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    // But only include UP or UNKNOWN nodes in the live set
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node3);
  }

  @Test
  public void should_ignore_nodes_from_remote_dcs() {
    // Given
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1);
  }

  @Test
  public void should_ignore_nodes_excluded_by_distance_reporter() {
    // Given
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(context.getNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn((node, dc) -> node.equals(node1) ? NodeDistance.IGNORED : null);

    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node2, node3);
  }

  @Test
  public void should_infer_local_dc_from_control_node_hostId() {
    // Given — DC not configured, but controlNode returns a node whose hostId is in the nodes map
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    UUID node1HostId = UUID.randomUUID();
    when(node1.getHostId()).thenReturn(node1HostId);
    when(controlConnection.controlNode()).thenReturn(node1);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(node1HostId, node1), distanceReporter);

    // Then — DC should be inferred from the control node's hostId lookup
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_throw_when_nodes_from_different_dcs_and_no_control_connection() {
    // Given — DC not configured, nodes span multiple DCs, no control connection
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc2");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    BasicLoadBalancingPolicy policy = createPolicy();

    // When/Then
    assertThatThrownBy(
            () ->
                policy.init(
                    ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2),
                    distanceReporter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("different DCs");
  }

  @Test
  public void should_throw_when_all_nodes_have_null_dc_and_no_control_connection() {
    // Given — DC not configured, all nodes have null DC, no control connection
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(node1.getDatacenter()).thenReturn(null);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    BasicLoadBalancingPolicy policy = createPolicy();

    // When/Then
    assertThatThrownBy(
            () -> policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("could not be inferred");
  }

  @Test
  public void should_warn_if_configured_dc_matches_no_node() {
    // Given — DC is configured as "dc1" but nodes are all in "dc2"
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(node1.getDatacenter()).thenReturn("dc2");
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then — should log a warning about the configured DC not matching any node
    verify(appender, atLeastOnce()).doAppend(loggingEventCaptor.capture());
    assertThat(
            loggingEventCaptor.getAllValues().stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .anyMatch(
                    e -> e.getFormattedMessage().contains("does not match any node's datacenter")))
        .isTrue();
  }

  @Test
  public void should_infer_local_dc_from_control_channel_endpoint() {
    // Given — DC not configured, controlNode has no hostId,
    // but channel endpoint matches a node in the nodes map
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(controlConnection.controlNode()).thenReturn(null);
    DriverChannel channel = mock(DriverChannel.class);
    Mockito.when(channel.getEndPoint())
        .thenReturn(new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)));
    when(controlConnection.channel()).thenReturn(channel);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    UUID node1Id = UUID.randomUUID();
    policy.init(ImmutableMap.of(node1Id, node1), distanceReporter);

    // Then — DC should be inferred from the channel endpoint matching node1
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_throw_when_control_channel_endpoint_matches_multiple_dcs() {
    // Given — DC not configured, controlNode has no hostId,
    // channel endpoint matches nodes in different DCs (ambiguous)
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(controlConnection.controlNode()).thenReturn(null);
    DriverChannel channel = mock(DriverChannel.class);
    // Both node1 and node2 share the same endpoint but different DCs
    DefaultEndPoint sharedEndpoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    Mockito.when(channel.getEndPoint()).thenReturn(sharedEndpoint);
    when(controlConnection.channel()).thenReturn(channel);
    when(node1.getEndPoint()).thenReturn(sharedEndpoint);
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getEndPoint()).thenReturn(sharedEndpoint);
    when(node2.getDatacenter()).thenReturn("dc2");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    BasicLoadBalancingPolicy policy = createPolicy();

    // When/Then — inference fails due to ambiguity, falls through to "different DCs" error
    assertThatThrownBy(
            () ->
                policy.init(
                    ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2),
                    distanceReporter))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("different DCs");
  }

  @NonNull
  protected DcInferringLoadBalancingPolicy createPolicy() {
    return new DcInferringLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
  }
}
