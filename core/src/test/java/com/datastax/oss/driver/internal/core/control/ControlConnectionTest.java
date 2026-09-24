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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DefaultNodeInfo;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

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
}
