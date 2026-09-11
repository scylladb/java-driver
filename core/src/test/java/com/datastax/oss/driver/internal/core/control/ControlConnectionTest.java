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
package com.datastax.oss.driver.internal.core.control;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DefaultNodeInfo;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;

@RunWith(DataProviderRunner.class)
public class ControlConnectionTest extends ControlConnectionTestBase {

  @Test
  public void should_close_successfully_if_it_was_never_init() {
    // When
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();

    // Then
    assertThatStage(closeFuture).isSuccess();
  }

  @Test
  public void should_init_with_first_contact_point_if_reachable() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    // Then
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_always_return_same_init_future() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // When
    CompletionStage<Void> initFuture1 = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    CompletionStage<Void> initFuture2 = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture1).isEqualTo(initFuture2);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_init_with_second_contact_point_if_first_one_fails() {
    // Given
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.controlConnectionFailed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    // each attempt tries all nodes, so there is no reconnection
    verify(reconnectionPolicy, never()).newNodeSchedule(any(Node.class));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_fail_to_init_if_all_contact_points_fail() {
    // Given
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node1, "mock failure")
            .failure(node2, "mock failure")
            .build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    assertThatStage(initFuture).isFailed();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.controlConnectionFailed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.controlConnectionFailed(node2));
    // no reconnections at init
    verify(reconnectionPolicy, never()).newNodeSchedule(any(Node.class));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_channel_goes_down() throws Exception {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    channel1.close();

    // Then
    // a reconnection was started
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(metadataManager, VERIFY_TIMEOUT).refreshNodes();
    verify(loadBalancingPolicyWrapper, VERIFY_TIMEOUT).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_node_becomes_ignored() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When — use a separate node with the same hostId to simulate a real event
    // (real-world events come from different Node objects with matching hostIds)
    DefaultNode eventNode1 = TestNodeFactory.newNode(1, node1.getHostId(), context);
    mockQueryPlan(node2);
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, eventNode1));

    // Then
    // an immediate reconnection was started
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(reconnectionSchedule, never()).nextDelay();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(metadataManager, VERIFY_TIMEOUT).refreshNodes();
    verify(loadBalancingPolicyWrapper, VERIFY_TIMEOUT).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_node_is_removed() {
    // Use a separate node with the same hostId to simulate a real event
    DefaultNode eventNode1 = TestNodeFactory.newNode(1, node1.getHostId(), context);
    should_reconnect_if_event(NodeStateEvent.removed(eventNode1));
  }

  @Test
  public void should_reconnect_if_node_is_forced_down() {
    // Use a separate node with the same hostId to simulate a real event
    DefaultNode eventNode1 = TestNodeFactory.newNode(1, node1.getHostId(), context);
    should_reconnect_if_event(
        NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, eventNode1));
  }

  private void should_reconnect_if_event(NodeStateEvent event) {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    mockQueryPlan(node2);
    eventBus.fire(event);

    // Then
    // an immediate reconnection was started
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(reconnectionSchedule, never()).nextDelay();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(metadataManager, VERIFY_TIMEOUT).refreshNodes();
    verify(loadBalancingPolicyWrapper, VERIFY_TIMEOUT).init();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_if_node_became_ignored_during_reconnection_attempt() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node1, channel1)
            // reconnection
            .pending(node2, channel2Future)
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    mockQueryPlan(node2, node1);
    // channel1 goes down, triggering a reconnection
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    // the reconnection to node2 is in progress
    factoryHelper.waitForCall(node2);

    // When
    // node2 becomes ignored
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));
    // the reconnection to node2 completes
    channel2Future.complete(channel2);

    // Then
    // The channel should get closed and we should try the next node
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_reconnect_if_node_was_removed_during_reconnection_attempt() {
    should_reconnect_if_event_during_reconnection_attempt(NodeStateEvent.removed(node2));
  }

  @Test
  public void should_reconnect_if_node_was_forced_down_during_reconnection_attempt() {
    should_reconnect_if_event_during_reconnection_attempt(
        NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));
  }

  private void should_reconnect_if_event_during_reconnection_attempt(NodeStateEvent event) {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // init
            .success(node1, channel1)
            // reconnection
            .pending(node2, channel2Future)
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    assertThatStage(initFuture).isSuccess();
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    mockQueryPlan(node2, node1);
    // channel1 goes down, triggering a reconnection
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    // the reconnection to node2 is in progress
    factoryHelper.waitForCall(node2);

    // When
    // node2 goes into the new state
    eventBus.fire(event);
    // the reconnection to node2 completes
    channel2Future.complete(channel2);

    // Then
    // The channel should get closed and we should try the next node
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_force_reconnection_if_pending() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofDays(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // the channel fails and a reconnection is scheduled for later
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();

    // When
    controlConnection.reconnectNow();
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_force_reconnection_even_if_connected() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // When
    controlConnection.reconnectNow();

    // Then
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(channel1, VERIFY_TIMEOUT).forceClose();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_not_force_reconnection_if_not_init() throws InterruptedException {
    // When
    controlConnection.reconnectNow();
    TimeUnit.MILLISECONDS.sleep(500);

    // Then
    verify(reconnectionSchedule, never()).nextDelay();
  }

  @Test
  public void should_not_force_reconnection_if_closed() throws InterruptedException {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();

    // When
    controlConnection.reconnectNow();
    TimeUnit.MILLISECONDS.sleep(500);

    // Then
    verify(reconnectionSchedule, never()).nextDelay();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_channel_when_closing() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // When
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();

    // Then
    assertThatStage(closeFuture).isSuccess();
    verify(channel1, VERIFY_TIMEOUT).forceClose();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_close_channel_if_closed_during_reconnection() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .pending(node2, channel2Future)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // the channel fails and a reconnection is scheduled
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(node1);
    // channel2 starts initializing (but the future is not completed yet)
    factoryHelper.waitForCall(node2);

    // When
    // the control connection gets closed before channel2 initialization is complete
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();
    channel2Future.complete(channel2);

    // Then
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    // no event because the control connection never "owned" the channel
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node2));
    verify(eventBus, never()).fire(ChannelEvent.channelClosed(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_try_next_node_if_resolve_endpoint_fails() {
    // Given — use a contact point (no hostId) so resolveChannelNodeIfNeeded
    // actually calls getChannelNodeInfo instead of short-circuiting
    node1 = TestNodeFactory.newContactPoint(1, context);
    mockQueryPlan(node1, node2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(node2, channel2)
            .build();

    // Make resolveChannelNodeInfo fail for channel1
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    CompletableFuture<NodeInfo> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new RuntimeException("mock resolve failure"));
    when(topologyMonitor.getChannelNodeInfo(channel1)).thenReturn(failedFuture);

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    // channel1's resolve failed, so channelOpened should NOT have been fired for node1
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node1));
    // channel1 should be force-closed by the resolve failure handler (previousChannel is null
    // at that point, so channel2's success does not close channel1 a second time)
    verify(channel1, timeout(500)).forceClose();

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_try_next_node_if_channel_closes_during_init_resolve() {
    // Given — use a contact point (no hostId) so resolveChannelNodeIfNeeded is async
    DefaultNode contactPoint1 = TestNodeFactory.newContactPoint(1, context);
    mockQueryPlan(contactPoint1, node2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(contactPoint1, channel1)
            .success(node2, channel2)
            .build();

    // Make getChannelNodeInfo return a pending future for channel1
    CompletableFuture<NodeInfo> pendingResolve = new CompletableFuture<>();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel1)).thenReturn(pendingResolve);

    // When — start init, channel1 opens successfully
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint1);

    // Close the channel before the resolve completes
    channel1.close();

    // Now complete the resolve — the code should detect channel is closed and try next node
    pendingResolve.complete(
        DefaultNodeInfo.builder()
            .withEndPoint(channel1.getEndPoint())
            .withHostId(UUID.randomUUID())
            .build());

    // Then — should fall through to node2
    factoryHelper.waitForCall(node2);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_try_next_node_if_channel_closes_during_reconnect_resolve() throws Exception {
    // Given — init normally with node1 (has hostId, resolve is synchronous)
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);

    // Contact point for reconnection (no hostId → async resolve)
    DefaultNode contactPoint2 = TestNodeFactory.newContactPoint(2, context);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint2, channel2) // reconnect: first attempt
            .success(node1, channel3) // reconnect: fallback after channel2 closes during resolve
            .build();

    // Init with node1
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // Make getChannelNodeInfo return a pending future for channel2
    CompletableFuture<NodeInfo> pendingResolve = new CompletableFuture<>();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2)).thenReturn(pendingResolve);

    // When — channel1 goes down, reconnect query plan returns contactPoint2 then node1
    mockQueryPlan(contactPoint2, node1);
    channel1.close();
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(contactPoint2);

    // Close channel2 before resolve completes
    channel2.close();

    // Complete the resolve — should detect channel is closed and try node1
    pendingResolve.complete(
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(UUID.randomUUID())
            .build());

    // Then — should fall through to node1 with channel3
    factoryHelper.waitForCall(node1);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel3));
    // channelOpened(node1) fires twice: once during init, once during reconnect fallback
    verify(eventBus, timeout(500).times(2)).fire(ChannelEvent.channelOpened(node1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_handle_channel_failure_if_closed_during_reconnection() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel1Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .pending(node1, channel1Future)
            .success(node2, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // the channel fails and a reconnection is scheduled
    channel1.close();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelClosed(node1));
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    // channel1 starts initializing (but the future is not completed yet)
    factoryHelper.waitForCall(node1);

    // When
    // the control connection gets closed before channel1 initialization fails
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();
    channel1Future.completeExceptionally(new Exception("mock failure"));

    // Then
    // should never try channel2 because the reconnection has detected that it can stop after the
    // first failure
    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_via_contact_point_fallback_and_resolve() throws Exception {
    // Given — init normally with node1
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);

    // Contact point node (no hostId) for reconnection fallback
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    UUID resolvedHostId = UUID.randomUUID();

    // The resolved metadata node that registerNode will return
    DefaultNode resolvedNode = TestNodeFactory.newNode(2, resolvedHostId, context);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(contactPoint, channel2)
            .build();

    // Init with node1
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // Mock topology monitor to return resolved info for the contact point channel
    NodeInfo resolvedInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(resolvedHostId)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(resolvedInfo));

    // registerNode should return the resolved node and add it to metadata
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            invocation -> {
              registeredNodes.put(resolvedNode.getHostId(), resolvedNode);
              return CompletableFuture.completedFuture(resolvedNode);
            });

    // When — channel goes down, reconnect query plan returns the contact point
    mockQueryPlan(contactPoint);
    channel1.close();

    // Then
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(contactPoint);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));

    // resolveChannelNodeIfNeeded was called (via topology monitor)
    verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(channel2);
    // registerNode was called with the resolved info (verify content, not just any)
    ArgumentCaptor<NodeInfo> nodeInfoCaptor = ArgumentCaptor.forClass(NodeInfo.class);
    verify(metadataManager, VERIFY_TIMEOUT).registerNode(nodeInfoCaptor.capture());
    assertThat(nodeInfoCaptor.getValue().getHostId()).isEqualTo(resolvedHostId);
    assertThat(nodeInfoCaptor.getValue().getEndPoint()).isEqualTo(channel2.getEndPoint());
    // The channelOpened event fires for the resolved node, not the contact point
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(resolvedNode));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_try_next_node_if_identified_node_became_ignored_during_resolve() {
    should_try_next_node_if_event_during_contact_point_resolve(true);
  }

  @Test
  public void should_try_next_node_if_identified_node_was_forced_down_during_resolve() {
    should_try_next_node_if_event_during_contact_point_resolve(false);
  }

  /**
   * A contact point's identity is only known once its resolve completes, and the event names the
   * registered metadata node while the placeholder we dialled is what sits in {@code pending} --
   * two different instances by construction. So the event cannot be matched while the resolve is in
   * flight, and the check has to run again on the node finally identified. Before that check
   * existed the control connection settled on a node it had just been told to abandon, and nothing
   * replayed the event.
   */
  private void should_try_next_node_if_event_during_contact_point_resolve(boolean ignored) {
    // Given -- init on node1, then a reconnection that goes through a contact point
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    UUID resolvedHostId = UUID.randomUUID();
    DefaultNode resolvedNode = TestNodeFactory.newNode(2, resolvedHostId, context);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint, channel2) // reconnect through the contact point
            .success(node1, channel3) // next node, once channel2 is dropped
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    // Hold the resolve open so the event lands while the contact point is still unidentified
    CompletableFuture<NodeInfo> pendingResolve = new CompletableFuture<>();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2)).thenReturn(pendingResolve);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            invocation -> {
              registeredNodes.put(resolvedNode.getHostId(), resolvedNode);
              return CompletableFuture.completedFuture(resolvedNode);
            });

    mockQueryPlan(contactPoint, node1);
    channel1.close();
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(contactPoint);

    // When -- the node behind that contact point is taken out of service mid-resolve
    if (ignored) {
      eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, resolvedNode));
    } else {
      eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, resolvedNode));
    }
    pendingResolve.complete(
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(resolvedHostId)
            .build());

    // Then -- channel2 is dropped rather than adopted, and the next node is tried
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    factoryHelper.waitForCall(node1);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel3));
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(resolvedNode));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_report_dropped_node_in_failure_when_no_candidate_is_usable() {
    // One contact point in the plan, and the node behind it turns out to be ignored, so there is
    // nothing left to try. The failure has to name it: an operator otherwise sees the control
    // connection refusing to come up with nothing saying a channel was opened and deliberately
    // closed.
    DriverChannel channel1 = newMockDriverChannel(1);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(1, context);
    UUID resolvedHostId = UUID.randomUUID();
    DefaultNode resolvedNode = TestNodeFactory.newNode(1, resolvedHostId, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel1).build();
    mockQueryPlan(contactPoint);

    // Hold the resolve open so the node can be taken out of service while it is unidentified.
    CompletableFuture<NodeInfo> pendingResolve = new CompletableFuture<>();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel1)).thenReturn(pendingResolve);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            invocation -> {
              registeredNodes.put(resolvedNode.getHostId(), resolvedNode);
              return CompletableFuture.completedFuture(resolvedNode);
            });

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, resolvedNode));
    pendingResolve.complete(
        DefaultNodeInfo.builder()
            .withEndPoint(channel1.getEndPoint())
            .withHostId(resolvedHostId)
            .build());

    assertThatStage(initFuture)
        .isFailed(
            error -> {
              // NoNodeAvailableException is also an AllNodesFailedException, so the errors are
              // what discriminates: it carries none.
              assertThat(error).isInstanceOf(AllNodesFailedException.class);
              assertThat(((AllNodesFailedException) error).getAllErrors())
                  .containsOnlyKeys(contactPoint);
            });
    verify(channel1, VERIFY_TIMEOUT).forceClose();
    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_resolve_contact_point_to_existing_metadata_node_on_reconnect() {
    // Given — a contact point with no hostId, and an existing metadata node with a known hostId
    UUID knownHostId = UUID.randomUUID();
    node1 = TestNodeFactory.newContactPoint(1, context);
    node2 = TestNodeFactory.newNode(2, knownHostId, context);

    mockQueryPlan(node1);

    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // Mock getChannelNodeInfo to return a NodeInfo with the same hostId as node2.
    // Pre-evaluate channel1.getEndPoint() to avoid nested mock calls inside when().
    NodeInfo resolvedInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel1.getEndPoint())
            .withHostId(knownHostId)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel1))
        .thenReturn(CompletableFuture.completedFuture(resolvedInfo));

    // registerNode atomically checks for existing nodes — mock it to return node2
    // (simulating that metadata already has a node with this hostId)
    when(metadataManager.registerNode(any())).thenReturn(CompletableFuture.completedFuture(node2));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    // Then — the control connection should resolve to the existing metadata node (node2)
    assertThatStage(initFuture).isSuccess();
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_return_control_node_after_init() {
    // Given
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // Before init, controlNode should be null
    assertThat(controlConnection.controlNode()).isNull();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    // Then — after init, controlNode should be set
    assertThatStage(initFuture).isSuccess();
    await().untilAsserted(() -> assertThat(controlConnection.controlNode()).isNotNull());
    assertThat(controlConnection.controlNode().getHostId()).isEqualTo(node1.getHostId());

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_clear_control_node_on_channel_close_and_restore_after_reconnect() {
    // Given
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .failure(node1, "mock failure")
            .pending(node2, channel2Future)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    await().untilAsserted(() -> assertThat(controlConnection.controlNode()).isNotNull());
    assertThat(controlConnection.controlNode().getHostId()).isEqualTo(node1.getHostId());

    // When — channel closes, reconnection starts but pending on node2
    channel1.close();
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(node1); // fails
    factoryHelper.waitForCall(node2); // pending

    // Then — controlNode should be null during reconnection window
    assertThat(controlConnection.controlNode()).isNull();

    // Complete the pending reconnection
    channel2Future.complete(channel2);

    // After reconnection, controlNode should be set to node2
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    await().untilAsserted(() -> assertThat(controlConnection.controlNode()).isNotNull());
    assertThat(controlConnection.controlNode().getHostId()).isEqualTo(node2.getHostId());

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_not_reconnect_on_event_for_non_control_node() {
    // Given — init with node1 (has hostId)
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    await().untilAsserted(() -> assertThat(controlConnection.controlNode()).isNotNull());

    // When — fire a distance event for node2 (not the control node)
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));

    // Then — should NOT trigger reconnection (channel stays the same)
    await()
        .during(Duration.ofMillis(200))
        .untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_reconnect_when_control_node_removed_from_metadata_after_reconnect() {
    // Given — init with node1
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(node2, channel2) // first reconnect
            .failure(node2, "decommissioned") // second reconnect: node2 fails
            .success(node1, channel3) // second reconnect: falls back to node1
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // Remove node2 from metadata before triggering reconnect
    // (simulating node2 was decommissioned during the outage)
    registeredNodes.remove(node2.getHostId());

    // When — channel1 goes down, reconnects to node2
    mockQueryPlan(node2, node1);
    channel1.close();

    // Then — onSuccessfulReconnect detects node2 is gone → triggers second reconnection
    // which eventually connects to node1 (channel3)
    factoryHelper.waitForCall(node2); // first reconnect

    // onSuccessfulReconnect detects node2 is removed from metadata → force-closes channel2
    verify(channel2, VERIFY_TIMEOUT).forceClose();

    factoryHelper.waitForCall(node2); // second reconnect: node2 fails
    factoryHelper.waitForCall(node1); // second reconnect: node1 succeeds
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel3));
    // channelOpened(node1) fires twice: once during init, once during second reconnect
    verify(eventBus, timeout(500).times(2)).fire(ChannelEvent.channelOpened(node1));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_not_call_getChannelNodeInfo_for_metadata_node_with_hostId() {
    // Given — node1 already has a hostId (it's a metadata node, not a contact point)
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    // Then — resolveChannelNodeIfNeeded should short-circuit
    assertThatStage(initFuture).isSuccess();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    verify(topologyMonitor, never()).getChannelNodeInfo(any(DriverChannel.class));

    factoryHelper.verifyNoMoreCalls();
  }

  // ---- Contact-point expansion: a hostname is resolved to every address it maps to, and each one
  // is tried as its own temporary node before the rest of the plan.

  private static final InetSocketAddress HOSTNAME =
      InetSocketAddress.createUnresolved("cluster.example.com", 9042);

  /** A contact point given as a hostname, as `basic.contact-points` produces under the defaults. */
  private DefaultNode hostnameContactPoint() {
    return DefaultNode.newContactPoint(new DefaultEndPoint(HOSTNAME), context);
  }

  /** What the resolver answers with: a resolved loopback address, {@code 127.0.0.<lastByte>}. */
  private static SocketAddress resolvedAddress(int lastByte) {
    return new InetSocketAddress("127.0.0." + lastByte, 9042);
  }

  private void mockResolveAll(SocketAddress... answer) {
    when(channelFactory.resolveAll(HOSTNAME))
        .thenReturn(CompletableFuture.completedFuture(ImmutableList.copyOf(answer)));
  }

  /** Matches the temporary node the expansion mints for {@code 127.0.0.<lastByte>}. */
  private static ArgumentMatcher<Node> nodeAt(int lastByte) {
    return node -> {
      // Null-safe: Mockito evaluates existing matchers against null while registering the next
      // stub.
      if (node == null) {
        return false;
      }
      SocketAddress address = node.getEndPoint().resolve();
      return address instanceof InetSocketAddress
          && !((InetSocketAddress) address).isUnresolved()
          && ((InetSocketAddress) address)
              .getAddress()
              .getHostAddress()
              .equals("127.0.0." + lastByte);
    };
  }

  private void mockConnectSuccess(ArgumentMatcher<Node> node, DriverChannel channel) {
    when(channelFactory.connect(argThat(node), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));
  }

  private void mockConnectFailure(ArgumentMatcher<Node> node, String message) {
    CompletableFuture<DriverChannel> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException(message));
    when(channelFactory.connect(argThat(node), any(DriverChannelOptions.class))).thenReturn(failed);
  }

  /**
   * Every node {@code connect()} was called with, in call order, once {@code expected} calls
   * happened.
   */
  private List<Node> connectedNodes(int expected) {
    ArgumentCaptor<Node> captor = ArgumentCaptor.forClass(Node.class);
    verify(channelFactory, timeout(500).times(expected))
        .connect(captor.capture(), any(DriverChannelOptions.class));
    return captor.getAllValues();
  }

  private static String labelled(int lastByte) {
    return "cluster.example.com/127.0.0." + lastByte + ":9042";
  }

  @Test
  public void should_try_every_address_a_contact_point_resolves_to() {
    // Given -- the name maps to two addresses, the first of which is dead
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(resolvedAddress(1), resolvedAddress(2));
    mockConnectFailure(nodeAt(1), "dead record");
    DriverChannel channel2 = newMockDriverChannel(2);
    mockConnectSuccess(nodeAt(2), channel2);
    // Resolver order is kept for this test: a two-element shuffle with this seed is the identity.
    controlConnection = new ControlConnection(context, new Random(1));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- the second address rescued the contact point
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(channelFactory).resolveAll(HOSTNAME);
    List<Node> tried = connectedNodes(2);
    // Each attempt is its own temporary node, labelled with the name and the address it dialled,
    // so TLS and authentication see the configured name and a failure says which address failed.
    assertThat(tried)
        .extracting(node -> node.getEndPoint().toString())
        .containsExactly(labelled(1), labelled(2));
    for (Node node : tried) {
      assertThat(node).isNotSameAs(contactPoint);
      assertThat(node.getHostId()).isNull();
      InetSocketAddress address = (InetSocketAddress) node.getEndPoint().resolve();
      assertThat(address.isUnresolved()).isFalse();
      assertThat(address.getHostString()).isEqualTo("cluster.example.com");
    }
    // The retained contact point itself was never dialled and never changed.
    verify(channelFactory, never()).connect(same(contactPoint), any(DriverChannelOptions.class));
    assertThat(((InetSocketAddress) contactPoint.getEndPoint().resolve()).isUnresolved()).isTrue();
  }

  @Test
  public void should_report_each_address_of_a_contact_point_that_failed() {
    // Given
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(resolvedAddress(1), resolvedAddress(2));
    mockConnectFailure(nodeAt(1), "dead record 1");
    mockConnectFailure(nodeAt(2), "dead record 2");

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- one entry per address, under the temporary node that dialled it
    assertThatStage(initFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(AllNodesFailedException.class);
              AllNodesFailedException allFailed = (AllNodesFailedException) error;
              assertThat(allFailed.getAllErrors()).hasSize(2);
              assertThat(allFailed.getAllErrors().keySet())
                  .extracting(node -> node.getEndPoint().toString())
                  .containsExactlyInAnyOrder(labelled(1), labelled(2));
              assertThat(allFailed.getAllErrors().values())
                  .allSatisfy(
                      errors ->
                          assertThat(errors)
                              .hasSize(1)
                              .allSatisfy(t -> assertThat(t).hasMessageContaining("dead record")));
            });
    // and the round is reported per attempt, as for any contact point
    for (Node node : connectedNodes(2)) {
      verify(eventBus).fire(ChannelEvent.controlConnectionFailed(node));
    }
  }

  @Test
  public void should_deduplicate_the_addresses_a_contact_point_resolves_to() {
    // Given -- a duplicated record (#989)
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(resolvedAddress(1), resolvedAddress(1), resolvedAddress(2));
    mockConnectFailure(nodeAt(1), "dead record");
    mockConnectFailure(nodeAt(2), "dead record");

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- the duplicate is not dialled twice
    assertThatStage(initFuture).isFailed();
    assertThat(connectedNodes(2))
        .extracting(node -> node.getEndPoint().toString())
        .containsExactlyInAnyOrder(labelled(1), labelled(2));
  }

  @Test
  public void should_shuffle_the_addresses_a_contact_point_resolves_to() {
    // Given -- five records, all dead, and a seeded source of randomness
    long seed = 7;
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(
        resolvedAddress(1),
        resolvedAddress(2),
        resolvedAddress(3),
        resolvedAddress(4),
        resolvedAddress(5));
    for (int i = 1; i <= 5; i++) {
      mockConnectFailure(nodeAt(i), "dead record");
    }
    controlConnection = new ControlConnection(context, new Random(seed));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- dialled in the order that seed shuffles the resolver's answer into, not the
    // resolver's own, so that a dead first record is not dead for every session
    assertThatStage(initFuture).isFailed();
    List<String> resolverOrder = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      resolverOrder.add(labelled(i));
    }
    List<String> expectedOrder = new ArrayList<>(resolverOrder);
    Collections.shuffle(expectedOrder, new Random(seed));
    assertThat(expectedOrder).isNotEqualTo(resolverOrder); // or the test would prove nothing
    assertThat(connectedNodes(5))
        .extracting(node -> node.getEndPoint().toString())
        .containsExactlyElementsOf(expectedOrder);
  }

  @Test
  public void should_try_at_most_max_candidate_addresses_of_a_contact_point() {
    // Given -- seven records, a cap of two
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(2);
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    SocketAddress[] answer = new SocketAddress[7];
    for (int i = 0; i < 7; i++) {
      answer[i] = resolvedAddress(i + 1);
      mockConnectFailure(nodeAt(i + 1), "dead record");
    }
    mockResolveAll(answer);

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- two attempts, two entries, and the round ends there
    assertThatStage(initFuture)
        .isFailed(error -> assertThat(((AllNodesFailedException) error).getAllErrors()).hasSize(2));
    assertThat(connectedNodes(2)).hasSize(2);
    verify(channelFactory, times(2)).connect(any(Node.class), any(DriverChannelOptions.class));
  }

  @Test
  public void should_try_at_least_one_address_when_the_cap_is_zero() {
    // Given
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(0);
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(resolvedAddress(1), resolvedAddress(2));
    DriverChannel channel = newMockDriverChannel(1);
    mockConnectSuccess(nodeAt(1), channel);
    mockConnectSuccess(nodeAt(2), channel);

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture).isSuccess();
    verify(channelFactory, times(1)).connect(any(Node.class), any(DriverChannelOptions.class));
  }

  @Test
  public void should_not_expand_a_contact_point_given_as_an_ip_literal() {
    // Given -- under resolve-contact-points = false an IP literal is kept unresolved too; there is
    // nothing to expand in it
    DefaultNode literal =
        DefaultNode.newContactPoint(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("127.0.0.9", 9042)), context);
    mockQueryPlan(literal);
    DriverChannel channel = newMockDriverChannel(9);
    when(channelFactory.connect(same(literal), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- dialled as it is, the way it always was
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel));
    verify(channelFactory, never()).resolveAll(any(SocketAddress.class));
  }

  @Test
  public void should_not_expand_a_resolved_contact_point() {
    // Given -- resolve-contact-points = true, or a programmatic resolved InetSocketAddress
    DefaultNode resolved = TestNodeFactory.newContactPoint(2, context);
    mockQueryPlan(resolved);
    DriverChannel channel = newMockDriverChannel(2);
    when(channelFactory.connect(same(resolved), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture).isSuccess();
    verify(channelFactory, never()).resolveAll(any(SocketAddress.class));
  }

  @Test
  public void should_not_expand_an_identified_node_with_a_hostname_endpoint() {
    // Given -- an address translator can hand an identified node a hostname on purpose, to be
    // re-resolved by Netty on every connect; that node is one server, not a set of addresses
    DefaultNode identified =
        TestNodeFactory.newNode(
            DefaultNodeInfo.builder()
                .withEndPoint(new DefaultEndPoint(HOSTNAME))
                .withHostId(UUID.randomUUID())
                .build(),
            context);
    mockQueryPlan(identified);
    DriverChannel channel = newMockDriverChannel(3);
    when(channelFactory.connect(same(identified), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture).isSuccess();
    verify(channelFactory, never()).resolveAll(any(SocketAddress.class));
  }

  @Test
  public void should_not_expand_a_custom_endpoint() {
    // Given -- a custom EndPoint keeps its own semantics, whatever resolve() returns
    EndPoint custom = mock(EndPoint.class);
    when(custom.resolve()).thenReturn(HOSTNAME);
    when(custom.asMetricPrefix()).thenReturn("custom");
    DefaultNode contactPoint = DefaultNode.newContactPoint(custom, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(4);
    when(channelFactory.connect(same(contactPoint), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture).isSuccess();
    verify(channelFactory, never()).resolveAll(any(SocketAddress.class));
  }

  @Test
  public void should_try_the_contact_point_as_is_when_resolution_fails() {
    // Given
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    CompletableFuture<List<SocketAddress>> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("resolver down"));
    when(channelFactory.resolveAll(HOSTNAME)).thenReturn(failed);
    DriverChannel channel = newMockDriverChannel(5);
    when(channelFactory.connect(same(contactPoint), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then -- exactly what happened before expansion existed: Netty resolves inside the connect
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel));
    verify(channelFactory, times(1)).connect(any(Node.class), any(DriverChannelOptions.class));
  }

  @Test
  public void should_try_the_contact_point_as_is_when_resolution_answers_nothing_usable() {
    // Given -- a resolver that declines (NoopAddressResolverGroup, say) hands the name back as is
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(HOSTNAME);
    DriverChannel channel = newMockDriverChannel(6);
    when(channelFactory.connect(same(contactPoint), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);

    // Then
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel));
    verify(channelFactory, times(1)).connect(any(Node.class), any(DriverChannelOptions.class));
  }

  @Test
  public void should_expand_a_contact_point_reached_by_the_reconnection_fallback()
      throws Exception {
    // Given -- initialized on node1
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));

    // the reconnection plan falls back to the contact point, whose name now maps elsewhere
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    mockResolveAll(resolvedAddress(7));
    DriverChannel channel7 = newMockDriverChannel(7);
    mockConnectSuccess(nodeAt(7), channel7);

    // When
    channel1.close();

    // Then -- the fallback went through the expansion too, and the control connection moved to
    // the address the name resolved to
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel7));
    verify(channelFactory).resolveAll(HOSTNAME);
    verify(channelFactory, never()).connect(same(contactPoint), any(DriverChannelOptions.class));
  }

  @Test
  public void should_complete_the_round_when_closed_during_resolution() {
    // Given -- initialized on node1, reconnecting through a contact point whose resolution hangs
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();
    DefaultNode contactPoint = hostnameContactPoint();
    mockQueryPlan(contactPoint);
    CompletableFuture<List<SocketAddress>> pendingResolution = new CompletableFuture<>();
    when(channelFactory.resolveAll(HOSTNAME)).thenReturn(pendingResolution);
    channel1.close();
    verify(channelFactory, VERIFY_TIMEOUT).resolveAll(HOSTNAME);

    // When -- the control connection is closed while the name is still being resolved
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();
    pendingResolution.complete(ImmutableList.of(resolvedAddress(8)));

    // Then -- the late answer opens nothing, and the round is over rather than left pending
    verify(channelFactory, never()).connect(argThat(nodeAt(8)), any(DriverChannelOptions.class));
    verify(channelFactory, never()).connect(same(contactPoint), any(DriverChannelOptions.class));
  }
}
