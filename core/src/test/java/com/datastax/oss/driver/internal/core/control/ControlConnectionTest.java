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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.ConnectHook;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DefaultNodeInfo;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

@RunWith(DataProviderRunner.class)
public class ControlConnectionTest extends ControlConnectionTestBase {

  @Mock private Appender<ILoggingEvent> appender;

  @Test
  public void should_close_successfully_if_it_was_never_init() {
    // When
    CompletionStage<Void> closeFuture = controlConnection.forceCloseAsync();

    // Then
    assertThatStage(closeFuture).isSuccess();
  }

  @Test
  public void should_arm_connect_hook_for_a_node_whose_host_id_is_unknown() {
    // Given — a contact point: DefaultNode.newContactPoint leaves hostId null, and that is the one
    // case with something to learn.
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel).build();

    // When
    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // Then — the identity read happens through the connect hook, inside the factory's candidate
    // loop, where an address that cannot identify itself is rejected while the hostname's other
    // addresses are still on hand.
    DriverChannelOptions options = connectOptions(contactPoint);
    assertThat(options.connectHook).isNotNull();
    assertThat(options.connectHookTimeout).isNotNull();
  }

  @Test
  public void should_reject_candidate_when_hook_reads_no_host_id() {
    // Given — the hook armed for a contact point, and a topology monitor that answers the identity
    // read with a NodeInfo carrying no host id (a bootstrapping node).
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    NodeInfo anonymousInfo = mock(NodeInfo.class);
    when(anonymousInfo.getHostId()).thenReturn(null);
    when(topologyMonitor.getChannelNodeInfo(channel))
        .thenReturn(CompletableFuture.completedFuture(anonymousInfo));

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // When — the factory invokes the hook against the candidate channel.
    CompletionStage<Void> vetted = connectOptions(contactPoint).connectHook.onConnect(channel);

    // Then — the candidate is rejected, so the factory can move on to the next address; accepting
    // it would fail later in registerNode and cost the whole plan entry.
    assertThatStage(vetted).isFailed(error -> assertThat(error).hasMessageContaining("host id"));
  }

  @Test
  public void should_read_each_candidates_columns_from_scratch() {
    // The identity read teaches DefaultTopologyMonitor the system.local column projection, and
    // until this hook existed it only ever ran against the channel the control connection kept --
    // so what it learned was by construction the accepted node's. It now runs once per candidate
    // address, and the projection is an intersection: it can only shrink. Whichever candidate the
    // loop ends up keeping, the projection has to be that node's.
    //
    // Which is why the cache is cleared *before* the read rather than undone after a rejection.
    // Undoing it on the rejection paths cannot work: none of them sees what a previous candidate
    // left behind, so a first address refused after its read would still narrow what a second,
    // accepted one reports -- and two of those paths (a REGISTER rejection, a hook abandoned on
    // its timeout) are ChannelFactory's, invisible from here. Clearing first makes every read a
    // SELECT * that re-learns from whoever answered.
    //
    // The ordering is the assertion. A reset after the read would leave no projection at all.
    // Which candidate's answer survives is a separate question -- the reads are ordered, the
    // *responses* are not -- and the two tests below cover two of the three ways they can
    // interleave. See DefaultTopologyMonitor#toLocalNodeInfo for the third, which is deferred.
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    // Hoisted: calling a mock inside when(...) leaves Mockito with an unfinished stubbing.
    EndPoint channelEndPoint = channel.getEndPoint();
    NodeInfo nodeInfo = mock(NodeInfo.class);
    when(nodeInfo.getHostId()).thenReturn(UUID.randomUUID());
    when(nodeInfo.getEndPoint()).thenReturn(channelEndPoint);
    when(topologyMonitor.getChannelNodeInfo(channel))
        .thenReturn(CompletableFuture.completedFuture(nodeInfo));

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // When -- the factory invokes the hook against the candidate channel.
    assertThatStage(connectOptions(contactPoint).connectHook.onConnect(channel)).isSuccess();

    // Then
    InOrder inOrder = inOrder(topologyMonitor);
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).resetLocalColumnCache();
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(channel);
    // And only the local projection. The hook reads system.local, so that is the only cache it can
    // have narrowed; discarding the peer projections too would cost a SELECT * over every peer row
    // on the next refresh, to recover from one address of one contact point.
    verify(topologyMonitor, never()).resetColumnCaches();
  }

  @Test
  public void should_not_let_a_refused_candidate_narrow_the_next_ones_projection() {
    // Two addresses of one contact point: the first identifies nobody and is refused, the second is
    // accepted. Both reads must start from a cleared cache, or the refused node's columns are what
    // the session goes on using -- it read first, and the intersection only shrinks. This is the
    // case a per-rejection undo cannot reach, because the rejection that matters happens before the
    // read that would have to be corrected.
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel refused = newMockDriverChannel(3);
    DriverChannel accepted = newMockDriverChannel(4);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, accepted).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    EndPoint acceptedEndPoint = accepted.getEndPoint();
    NodeInfo anonymousInfo = mock(NodeInfo.class);
    when(anonymousInfo.getHostId()).thenReturn(null);
    when(topologyMonitor.getChannelNodeInfo(refused))
        .thenReturn(CompletableFuture.completedFuture(anonymousInfo));
    NodeInfo acceptedInfo = mock(NodeInfo.class);
    when(acceptedInfo.getHostId()).thenReturn(UUID.randomUUID());
    when(acceptedInfo.getEndPoint()).thenReturn(acceptedEndPoint);
    when(topologyMonitor.getChannelNodeInfo(accepted))
        .thenReturn(CompletableFuture.completedFuture(acceptedInfo));

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);
    ConnectHook hook = connectOptions(contactPoint).connectHook;

    // When -- the factory walks the two candidates through the one hook it was given.
    assertThatStage(hook.onConnect(refused))
        .isFailed(error -> assertThat(error).hasMessageContaining("host id"));
    assertThatStage(hook.onConnect(accepted)).isSuccess();

    // Then -- a clear before each read, so the accepted node's answer is the one left in the cache.
    InOrder inOrder = inOrder(topologyMonitor);
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).resetLocalColumnCache();
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(refused);
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).resetLocalColumnCache();
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(accepted);
    verify(topologyMonitor, never()).resetColumnCaches();
  }

  @Test
  public void should_not_arm_connect_hook_for_a_node_that_is_already_identified() {
    // Given — node1 has a host id, so there is nothing to learn and nothing to vet: its
    // connections keep the exact exchange they had before.
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(node1, channel1).build();

    // When
    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);

    // Then
    assertThat(connectOptions(node1).connectHook).isNull();
  }

  @Test
  public void should_use_node_info_captured_by_hook_without_reading_again() {
    // Given — a contact point whose connect hook has run against the winning channel, capturing
    // the identity it read.
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(3);
    CompletableFuture<DriverChannel> channelFuture = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .pending(contactPoint, channelFuture)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    EndPoint channelEndPoint = channel.getEndPoint();
    NodeInfo nodeInfo = mock(NodeInfo.class);
    when(nodeInfo.getHostId()).thenReturn(UUID.randomUUID());
    when(nodeInfo.getEndPoint()).thenReturn(channelEndPoint);
    when(topologyMonitor.getChannelNodeInfo(channel))
        .thenReturn(CompletableFuture.completedFuture(nodeInfo));
    DefaultNode registered = TestNodeFactory.newNode(3, context);
    when(metadataManager.registerNode(nodeInfo))
        .thenReturn(CompletableFuture.completedFuture(registered));

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // When — the hook runs once (as the factory would against the accepted candidate), then the
    // connect completes with that same channel.
    assertThatStage(connectOptions(contactPoint).connectHook.onConnect(channel)).isSuccess();
    channelFuture.complete(channel);

    // Then — what the hook captured is what gets registered, with no second identity read: the
    // monitor was asked exactly once, by the hook itself.
    verify(metadataManager, VERIFY_TIMEOUT).registerNode(nodeInfo);
    verify(topologyMonitor, times(1)).getChannelNodeInfo(channel);
  }

  @Test
  public void should_clear_the_projection_before_falling_back_to_a_direct_read() {
    // The other interleaving. A candidate abandoned on the hook timeout is not cancelled, so its
    // system.local answer can land *after* the accepted candidate's -- and then it is that late
    // write the holder carries, the projection in the cache is the refused node's, and clearing
    // before the next read cannot help because there is no next hook read. What saves it is the
    // same late write: pairing the capture with its channel makes getFor miss, and the fallback
    // read that follows has to start from a cleared cache like any other. Without the clear, the
    // node the driver is keeping is identified through a projection intersected against one it
    // refused, which is the whole failure the reset exists to prevent.
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel accepted = newMockDriverChannel(3);
    DriverChannel stranded = newMockDriverChannel(4);
    CompletableFuture<DriverChannel> channelFuture = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .pending(contactPoint, channelFuture)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    EndPoint acceptedEndPoint = accepted.getEndPoint();
    NodeInfo acceptedInfo = mock(NodeInfo.class);
    when(acceptedInfo.getHostId()).thenReturn(UUID.randomUUID());
    when(acceptedInfo.getEndPoint()).thenReturn(acceptedEndPoint);
    when(topologyMonitor.getChannelNodeInfo(accepted))
        .thenReturn(CompletableFuture.completedFuture(acceptedInfo));
    EndPoint strandedEndPoint = stranded.getEndPoint();
    NodeInfo strandedInfo = mock(NodeInfo.class);
    when(strandedInfo.getHostId()).thenReturn(UUID.randomUUID());
    when(strandedInfo.getEndPoint()).thenReturn(strandedEndPoint);
    when(topologyMonitor.getChannelNodeInfo(stranded))
        .thenReturn(CompletableFuture.completedFuture(strandedInfo));
    DefaultNode registered = TestNodeFactory.newNode(3, context);
    when(metadataManager.registerNode(acceptedInfo))
        .thenReturn(CompletableFuture.completedFuture(registered));

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);
    ConnectHook hook = connectOptions(contactPoint).connectHook;

    // When — the hook runs against the candidate the loop keeps, and then the abandoned one
    // answers, overwriting the capture. The connect completes with the accepted channel.
    assertThatStage(hook.onConnect(accepted)).isSuccess();
    assertThatStage(hook.onConnect(stranded)).isSuccess();
    channelFuture.complete(accepted);

    // Then -- the stale capture is not used, and the read that replaces it is preceded by its own
    // clear. The last of the three resets is the one this test is about: drop it and the fallback
    // read reuses whatever the stranded candidate taught the monitor.
    InOrder inOrder = inOrder(topologyMonitor);
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).resetLocalColumnCache();
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(accepted);
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).resetLocalColumnCache();
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(stranded);
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).resetLocalColumnCache();
    inOrder.verify(topologyMonitor, VERIFY_TIMEOUT).getChannelNodeInfo(accepted);
    verify(metadataManager, VERIFY_TIMEOUT).registerNode(acceptedInfo);
    verify(metadataManager, never()).registerNode(strandedInfo);
    verify(topologyMonitor, never()).resetColumnCaches();
  }

  /** The options {@code ChannelFactory} was asked to connect {@code node} with. */
  private DriverChannelOptions connectOptions(Node node) {
    ArgumentCaptor<DriverChannelOptions> captor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    verify(channelFactory, VERIFY_TIMEOUT).connect(eq(node), captor.capture());
    return captor.getValue();
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
  public void should_advance_to_next_node_when_a_listener_throws_on_connection_failure() {
    // EventBus.fire() has no try/catch of its own, and RunOrSchedule.on(adminExecutor, ..) runs
    // listeners inline when the firing thread is already the admin loop -- so a NodeStateListener
    // or metrics chain that throws propagates out of the fire() call. It sits immediately before
    // the recursive connect() that advances the query plan, inside an outer catch that only logs,
    // so without the guard the round neither advances nor completes: initFuture stays pending
    // forever (SessionBuilder.build() blocks on it) and no failure is ever reported.
    //
    // Modelled by making the bus itself throw at that call, which is what an inline listener does.
    DriverChannel channel2 = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node1, "mock failure")
            .success(node2, channel2)
            .build();
    doThrow(new IllegalStateException("listener blew up"))
        .when(eventBus)
        .fire(ChannelEvent.controlConnectionFailed(node1));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    factoryHelper.waitForCall(node2);

    // Then the listener's failure is absorbed and the plan still advances.
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel2));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node2));

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
  public void should_keep_the_control_channel_pinned_when_it_adopts_a_nodes_endpoint() {
    // Given — a Cloud/SNI contact point. The channel's endpoint is pinned to the one proxy IP the
    // connection reached; the node's own endpoint is the unpinned form, which resolves to the
    // *unresolved* proxy address that every SniEndPoint in the cluster shares.
    InetSocketAddress proxy = InetSocketAddress.createUnresolved("proxy.example.com", 9042);
    EndPoint unpinned = new SniEndPoint(proxy, node2.getHostId().toString());
    InetSocketAddress reachedProxyIp = new InetSocketAddress("10.0.0.5", 9042);
    EndPoint pinned = ((SniEndPoint) unpinned).pinTo(reachedProxyIp);

    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    DriverChannel channel1 = newMockDriverChannel(1);
    when(channel1.getEndPoint()).thenReturn(pinned);
    mockQueryPlan(contactPoint);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel1).build();

    NodeInfo resolvedInfo =
        DefaultNodeInfo.builder().withEndPoint(unpinned).withHostId(node2.getHostId()).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel1))
        .thenReturn(CompletableFuture.completedFuture(resolvedInfo));
    when(metadataManager.registerNode(any())).thenReturn(CompletableFuture.completedFuture(node2));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);
    assertThatStage(initFuture).isSuccess();

    // Then — the channel adopts the node's identity but keeps the address it is actually connected
    // to. Adopting the unpinned form would make channel.getEndPoint().resolve() the shared proxy
    // name, which every other node's endpoint equals -- so isControlNode()'s resolve() comparison
    // would answer true for any node in the cluster during the resolution window, and JAVA-2303's
    // self-peer guard (broadcastRpcAddress.equals(localEndPoint.resolve())) would stop matching,
    // an unresolved address never equalling a resolved one.
    ArgumentCaptor<EndPoint> adopted = ArgumentCaptor.forClass(EndPoint.class);
    verify(channel1, VERIFY_TIMEOUT).setEndPoint(adopted.capture());
    assertThat(adopted.getValue().resolve()).isEqualTo(reachedProxyIp);
    // Still the node's endpoint by identity: only the pin differs.
    assertThat(adopted.getValue()).isEqualTo(unpinned);
    assertThat(adopted.getValue().asMetricPrefix()).isEqualTo(unpinned.asMetricPrefix());

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_refuse_a_contact_point_that_resolves_to_a_removed_node() {
    // Given — node2 has been removed by the topology monitor, and a contact point whose DNS record
    // still lists it. This is what the reconnection fallback makes reachable: the plan is
    // exhausted,
    // the contact points are re-offered, and one of them leads back to a node the cluster no longer
    // has. Registering it would publish it back into Metadata#getNodes() and fire onAdd for it, and
    // the driver would then keep its control connection on a decommissioned node -- whose own
    // system.peers view is what the next refresh would adopt.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint, channel2) // reconnect via the fallback: reaches removed node2
            .success(node1, channel3) // ... and moves on to the next plan entry
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // node2 is removed: the event names the instance, and it leaves metadata.
    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(node2.getHostId());

    NodeInfo removedNodeInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(node2.getHostId())
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(removedNodeInfo));

    // When
    mockQueryPlan(contactPoint, node1);
    channel1.close();

    // Then — the channel to the removed node is closed and the plan advances. Crucially the node is
    // never registered: the instance-keyed guards cannot answer for a node registerNode would mint
    // fresh, so the decision has to be taken on the host id, before registration.
    factoryHelper.waitForCall(contactPoint);
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    factoryHelper.waitForCall(node1);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel3));

    verify(metadataManager, never()).registerNode(any());
    assertThat(registeredNodes).doesNotContainKey(node2.getHostId());
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node2));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_not_count_a_wholly_excluded_contact_point_as_a_connection_failure() {
    // Given — a contact point every one of whose addresses the connect hook refused on its host id.
    // ChannelFactory reports that as a single failure for the name, and it arrives wrapped twice:
    // the hook's stage fails with a CompletionException, and finishCandidate() puts a
    // ConnectionInitException around that before the candidate loop sees it. Matching on one fixed
    // shape would read this as a node that could not be reached.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel3 = newMockDriverChannel(3);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    Throwable asChannelFactoryReportsIt =
        new ConnectionInitException(
            "Connect hook rejected the channel",
            new CompletionException(
                new ControlConnection.ExcludedNodeException("node was removed")));
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .failure(contactPoint, asChannelFactoryReportsIt) // reconnect via the fallback
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // When
    mockQueryPlan(contactPoint, node1);
    channel1.close();

    // Then — the plan advances, but nothing failed to connect, so no controlConnectionFailed event
    // is fired for the contact point. Firing one would count a refusal as a connection failure in
    // the node metrics and warn the operator about a deployment that is in fact reachable.
    factoryHelper.waitForCall(contactPoint);
    factoryHelper.waitForCall(node1);
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel3));
    verify(eventBus, never()).fire(ChannelEvent.controlConnectionFailed(contactPoint));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_refuse_a_removed_host_id_inside_the_connect_hook() {
    // Given — the same situation as above, but looked at one address earlier. A contact point is a
    // *name*: ChannelFactory expands it and tries the addresses in turn, and only the first one to
    // be accepted becomes the connection. Deciding the host id after that -- which is where
    // #resolveChannelNodeIfNeeded still decides it, on live state -- is a decision taken when the
    // other candidates have already been discarded, so a name whose first shuffled address happens
    // to be the removed node writes off every other address behind it for that round.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint, channel2) // reconnect via the fallback
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(node2.getHostId());

    NodeInfo removedNodeInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(node2.getHostId())
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(removedNodeInfo));

    mockQueryPlan(contactPoint, node1);
    channel1.close();
    factoryHelper.waitForCall(contactPoint);

    // When — the factory runs the hook against a candidate that turns out to be the removed node.
    CompletionStage<Void> vetted = connectOptions(contactPoint).connectHook.onConnect(channel2);

    // Then — that one candidate is rejected, and with a cause the factory does not treat as
    // node-wide, so the loop moves on to the hostname's remaining addresses.
    assertThatStage(vetted)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(ControlConnection.ExcludedNodeException.class);
              assertThat(error).hasMessageContaining("node was removed");
            });
  }

  @Test
  public void should_accept_a_host_id_nothing_is_known_against_inside_the_connect_hook() {
    // The other side of the same gate, and the reason it cannot simply refuse anything unfamiliar:
    // a host id in neither metadata nor the removed set is a node that joined while the driver was
    // disconnected, which is exactly what the contact-point fallback exists to reach.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(contactPoint, channel2)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(node2.getHostId());

    NodeInfo newcomerInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(UUID.randomUUID())
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(newcomerInfo));

    mockQueryPlan(contactPoint, node1);
    channel1.close();
    factoryHelper.waitForCall(contactPoint);

    assertThatStage(connectOptions(contactPoint).connectHook.onConnect(channel2)).isSuccess();
  }

  @Test
  public void should_stop_refusing_removed_nodes_once_a_whole_reconnection_round_has_failed() {
    // The removal set is only ever cleared by a node state event, and those arrive from a metadata
    // refresh -- which needs the very control connection this is trying to restore. So a host id
    // recorded as removed on stale information (a node transiently missing from a peers table)
    // would otherwise refuse the only reachable address for the rest of the session, and the
    // contact-point fallback could never recover from it. A round that reaches nothing at all is
    // the signal that this knowledge is no longer worth acting on.
    UUID hostId = node2.getHostId();
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .failure(contactPoint, "mock failure") // a round that reaches nothing
            .success(contactPoint, channel2) // the next round, no longer refused
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // node2 is removed, and never reports a state again -- nothing can un-remove it the usual way.
    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);

    NodeInfo removedNodeInfo =
        DefaultNodeInfo.builder().withEndPoint(channel2.getEndPoint()).withHostId(hostId).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(removedNodeInfo));
    // As the real MetadataManager does for a host id absent from metadata: mint a fresh node and
    // publish it. Both matter here -- returning the *same* instance would keep it in the
    // instance-keyed lastNodeState map and hit the separate post-registration guard, and not
    // publishing it would have the control connection drop the node again for being absent and
    // reconnect in a loop. Either would be a property of the stub, not of the code under test.
    DefaultNode reRegistered = TestNodeFactory.newNode(2, hostId, context);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            i -> {
              registeredNodes.put(hostId, reRegistered);
              return CompletableFuture.completedFuture(reRegistered);
            });

    // When -- the contact point is all that is left, and the first round against it fails outright.
    mockQueryPlan(contactPoint);
    channel1.close();

    // Then -- the round that follows reaches the same host id and is allowed through this time,
    // rather than being refused forever on the strength of a removal the driver can no longer
    // confirm or retract.
    factoryHelper.waitForCalls(contactPoint, 2);
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(reRegistered));
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel2));
  }

  @Test
  public void should_keep_refusing_a_removed_node_while_the_refusal_is_within_its_budget() {
    // The counterpart to the test above. That clear exists for a round that could reach nothing, at
    // which point the driver's view of who was removed is as stale as its view of everything else.
    // A round drained purely by refusals is not that: every node in it answered, and the refusal is
    // this very set doing its job. Clearing on it would undo the protection on the round that just
    // enforced it, handing the next round the node it had only just turned away.
    //
    // The refusal is budgeted rather than unconditional (see the test below), so the schedule here
    // hands out two quick delays and then a very long one: exactly two rounds run, both inside the
    // budget, and nothing races the assertions.
    UUID hostId = node2.getHostId();
    when(reconnectionSchedule.nextDelay())
        .thenReturn(Duration.ofNanos(1), Duration.ofNanos(1), Duration.ofDays(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint, channel2) // round 1: connects, then is refused by host id
            .success(contactPoint, channel3) // round 2: refused again, the set having survived
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);

    // Both rounds reach the same removed host id: the contact point's DNS still lists it. Built
    // before the stubbing, since getEndPoint() is itself a mock call. A fresh channel per round,
    // because forceClose() completes the mock's close future and a reused one would come back as
    // "channel closed during endpoint resolve" -- a connectivity failure, which clears the set for
    // an entirely different reason and would make this test pass without testing anything.
    NodeInfo round1Info =
        DefaultNodeInfo.builder().withEndPoint(channel2.getEndPoint()).withHostId(hostId).build();
    NodeInfo round2Info =
        DefaultNodeInfo.builder().withEndPoint(channel3.getEndPoint()).withHostId(hostId).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(round1Info));
    when(topologyMonitor.getChannelNodeInfo(channel3))
        .thenReturn(CompletableFuture.completedFuture(round2Info));
    // Stubbed so that a round which wrongly stopped refusing would go on to succeed rather than
    // NPE: without the gate this is what round 2 reaches, and the assertions below would then be
    // failing on the behaviour rather than on the stub.
    DefaultNode reRegistered = TestNodeFactory.newNode(2, hostId, context);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            i -> {
              registeredNodes.put(hostId, reRegistered);
              return CompletableFuture.completedFuture(reRegistered);
            });

    mockQueryPlan(contactPoint);
    channel1.close();

    factoryHelper.waitForCalls(contactPoint, 2);
    // Waited for on a terminal signal rather than asserted on the spot: connect() parks the new
    // channel on the control connection *before* the identity read and only nulls it once the host
    // id comes back refused, so controlConnection.channel() is transiently the refused channel.
    // Closing it is the refusal itself, and cannot be observed early.
    verify(channel3, VERIFY_TIMEOUT).forceClose();
    // Neither round got as far as adopting the node: the host id is checked before registerNode,
    // so a refusal leaves the driver's view of it untouched.
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(reRegistered));
    assertThat(registeredNodes).doesNotContainKey(hostId);
  }

  @Test
  public void should_keep_refusing_a_removed_node_when_a_sibling_address_was_unreachable() {
    // A contact point whose addresses went [refused, unreachable]. ChannelFactory reports one
    // failure for the name and chooses which half speaks, so this is the ordering where the
    // refusal surfaces and the connect failure rides along as suppressed -- the other ordering is
    // the same round seen from the other side.
    //
    // That round must not discard the removal set. Clearing exists for a round that could reach
    // nothing, at which point the driver's view of who was removed is as stale as its view of
    // everything else; this round handshaked with a live server and read its host id, which is the
    // opposite of stale. Letting one firewalled sibling record undo the refusal its neighbour just
    // earned is how the removed node comes back on the very next round.
    UUID hostId = node2.getHostId();
    when(reconnectionSchedule.nextDelay())
        .thenReturn(Duration.ofNanos(1), Duration.ofNanos(1), Duration.ofDays(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel3 = newMockDriverChannel(3);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    Throwable refusedThenUnreachable =
        new ConnectionInitException(
            "Connect hook rejected the channel",
            new CompletionException(
                new ControlConnection.ExcludedNodeException("node was removed")));
    refusedThenUnreachable.addSuppressed(new ConnectException("connection refused"));
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .failure(contactPoint, refusedThenUnreachable) // round 1: one refused, one unreachable
            .success(contactPoint, channel3) // round 2: the set must have survived round 1
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);

    // Round 2 reaches the removed host id again: the contact point's DNS still lists it.
    NodeInfo round2Info =
        DefaultNodeInfo.builder().withEndPoint(channel3.getEndPoint()).withHostId(hostId).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel3))
        .thenReturn(CompletableFuture.completedFuture(round2Info));
    // So that a round which wrongly stopped refusing succeeds rather than NPEs, and the assertions
    // below fail on the behaviour rather than on the stub.
    DefaultNode reRegistered = TestNodeFactory.newNode(2, hostId, context);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            i -> {
              registeredNodes.put(hostId, reRegistered);
              return CompletableFuture.completedFuture(reRegistered);
            });

    mockQueryPlan(contactPoint);
    channel1.close();

    factoryHelper.waitForCalls(contactPoint, 2);
    verify(channel3, VERIFY_TIMEOUT).forceClose();
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(reRegistered));
    assertThat(registeredNodes).doesNotContainKey(hostId);
  }

  @Test
  public void should_stop_refusing_a_removed_node_after_enough_rounds_of_pure_refusals() {
    // Enforcing the refusal is one thing; enforcing it for the life of the session on evidence that
    // can never be rechecked is another. Every other exit from the removal set needs something this
    // situation does not have -- a NodeStateEvent comes from a metadata refresh, which needs the
    // control connection being restored here, and the LRU cap only evicts after 256 *further*
    // removals. So if the refused host id is the only address the plan has, an unbounded version of
    // the rule above would never let the session recover. After MAX_ALL_EXCLUDED_ROUNDS rounds that
    // reached a live server and threw it away, the driver stops trusting the removal and lets the
    // next round find out for itself.
    UUID hostId = node2.getHostId();
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    // One per round, for the reason given in the test above -- a reused channel arrives already
    // closed and is recorded as unreachable, which would clear the set early and by accident.
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);
    DriverChannel channel5 = newMockDriverChannel(5);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint, channel2) // round 1: refused, budget 1 of 3
            .success(contactPoint, channel3) // round 2: refused, budget 2 of 3
            .success(contactPoint, channel4) // round 3: refused, budget spent -- set cleared
            .success(contactPoint, channel5) // round 4: nothing left refusing it
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // node2 is removed and never reports a state again, so nothing can un-remove it the usual way.
    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);

    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    for (DriverChannel channel : new DriverChannel[] {channel2, channel3, channel4, channel5}) {
      NodeInfo info =
          DefaultNodeInfo.builder().withEndPoint(channel.getEndPoint()).withHostId(hostId).build();
      when(topologyMonitor.getChannelNodeInfo(channel))
          .thenReturn(CompletableFuture.completedFuture(info));
    }
    // As the real MetadataManager does for a host id absent from metadata: mint a fresh node and
    // publish it, so the accepted round does not immediately drop the node again for being absent.
    DefaultNode reRegistered = TestNodeFactory.newNode(2, hostId, context);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            i -> {
              registeredNodes.put(hostId, reRegistered);
              return CompletableFuture.completedFuture(reRegistered);
            });

    mockQueryPlan(contactPoint);
    channel1.close();

    // Then -- the fourth round is let through, rather than the session being stuck refusing the one
    // address it can still reach.
    factoryHelper.waitForCalls(contactPoint, 4);
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(reRegistered));
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel5));
    // And the rounds before it really were refused, rather than the set having been dropped early.
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    verify(channel3, VERIFY_TIMEOUT).forceClose();
    verify(channel4, VERIFY_TIMEOUT).forceClose();
  }

  @Test
  public void should_not_spend_the_refusal_budget_on_rounds_that_tried_nothing() {
    // The budget above is spent by a round that reached a live server and threw it away: repeating
    // that learns nothing, so after enough of them the driver stops trusting the removal. A round
    // whose plan was empty to begin with is not that round. It reached nothing and refused nothing,
    // which is exactly why #anyNodeUnreachable declines to clear on it -- it has no standing either
    // way -- and counting it against the budget would discard the set on the one kind of evidence
    // both branches agree confers none.
    //
    // Empty plans are reachable, and they arrive here looking like every other failure: reconnect()
    // hands connect() a null error list, the empty queue polls null immediately, and
    // AllNodesFailedException.fromErrors(null) answers a NoNodeAvailableException whose error map
    // is empty. Both predicates walk that map, so both see nothing.
    UUID hostId = node2.getHostId();
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .success(contactPoint, channel2) // the single round that has anything to try
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // node2 is removed and never reports a state again, so nothing can un-remove it the usual way.
    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);

    NodeInfo refusedInfo =
        DefaultNodeInfo.builder().withEndPoint(channel2.getEndPoint()).withHostId(hostId).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(refusedInfo));
    // As the real MetadataManager would, were the node ever allowed through -- so that a failure
    // here shows up as the node being accepted, not as a stub falling over.
    DefaultNode reRegistered = TestNodeFactory.newNode(2, hostId, context);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            i -> {
              registeredNodes.put(hostId, reRegistered);
              return CompletableFuture.completedFuture(reRegistered);
            });

    // Comfortably more empty rounds than the budget, and the contact point offered on exactly one
    // round -- every round after it is empty again, so nothing races the assertions below.
    int emptyRoundsBefore = 5;
    AtomicInteger round = new AtomicInteger();
    when(loadBalancingPolicyWrapper.newControlReconnectionQueryPlan())
        .thenAnswer(
            i -> {
              ConcurrentLinkedQueue<Node> plan = new ConcurrentLinkedQueue<>();
              if (round.getAndIncrement() == emptyRoundsBefore) {
                plan.offer(contactPoint);
              }
              return plan;
            });

    channel1.close();

    // Then -- the refusal outlived all of those empty rounds. Had they spent the budget, the set
    // would have been cleared long before this round and the node let through.
    factoryHelper.waitForCalls(contactPoint, 1);
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(reRegistered));
    assertThat(registeredNodes).doesNotContainKey(hostId);
  }

  @Test
  public void should_accept_a_contact_point_that_resolves_to_a_re_added_node() {
    // The mirror case: a removal is not a life sentence. A host id that reports any state again is
    // no longer removed, so a contact point may lead back to it -- otherwise a node that bounced
    // out
    // and back would be blocked for the rest of the session.
    UUID hostId = node2.getHostId();
    DriverChannel channel1 = newMockDriverChannel(1);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    mockQueryPlan(contactPoint);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel1).build();

    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);
    // ... and back: the driver sees it again, which clears the removal.
    eventBus.fire(NodeStateEvent.added(node2));

    NodeInfo resolvedInfo =
        DefaultNodeInfo.builder().withEndPoint(channel1.getEndPoint()).withHostId(hostId).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel1))
        .thenReturn(CompletableFuture.completedFuture(resolvedInfo));
    when(metadataManager.registerNode(any())).thenReturn(CompletableFuture.completedFuture(node2));

    // When
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // Then
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

  @Test
  public void should_report_auth_failure_when_every_contact_point_failed_on_authentication() {
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(
                new SimpleEntry<>(node1, authError(node1)),
                new SimpleEntry<>(node2, authError(node2))));

    assertThat(ControlConnection.isAuthFailure(error)).isTrue();
  }

  @Test
  public void should_not_report_auth_failure_when_an_address_failed_on_something_else() {
    // One entry no longer means one address: ChannelFactory expands a contact-point hostname to
    // every address it resolves to and reports one failure for the name, with the other addresses'
    // failures suppressed. Reading only the top-level throwable would call this an authentication
    // failure and tell the operator their credentials are wrong, when the name's other records are
    // simply unreachable.
    Throwable withUnreachableSibling = authError(node1);
    withUnreachableSibling.addSuppressed(new ConnectException("connection refused"));
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(new SimpleEntry<>(node1, withUnreachableSibling)));

    assertThat(ControlConnection.isAuthFailure(error)).isFalse();
  }

  @Test
  public void should_report_auth_failure_even_when_a_node_was_excluded() {
    // An excluded node was never asked for credentials, so it is evidence of nothing. Letting it
    // veto the verdict would hide a genuine credential problem behind one node that happened to be
    // ignored or forced down -- and every exclusion path records an entry now, so this is the
    // ordinary shape of a round rather than a corner.
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(
                new SimpleEntry<>(node1, authError(node1)),
                new SimpleEntry<>(
                    node2, new ControlConnection.ExcludedNodeException("node became ignored"))));

    assertThat(ControlConnection.isAuthFailure(error)).isTrue();
  }

  @Test
  public void should_report_auth_failure_when_one_contact_point_went_excluded_then_auth() {
    // One entry, one contact-point name, two addresses: the first belongs to a node this
    // connection may not use and is refused inside the connect hook, the second reaches a server
    // that rejects the credentials. ChannelFactory surfaces the authentication failure with the
    // refusal attached as suppressed -- wrapped the way finishCandidate wraps a hook rejection.
    //
    // Skipping the entry outright would drop the only evidence the round has, so it is tested
    // rather than skipped; and testing it has to set the exclusion aside on that side too, or the
    // suppressed refusal answers the question instead and this one contact point vetoes the
    // verdict for the whole round.
    Throwable withExcludedSibling = authError(node1);
    withExcludedSibling.addSuppressed(
        new ConnectionInitException(
            "Connect hook rejected the channel",
            new CompletionException(
                new ControlConnection.ExcludedNodeException("node was removed"))));
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(new SimpleEntry<>(node1, withExcludedSibling)));

    assertThat(ControlConnection.isAuthFailure(error)).isTrue();
  }

  @Test
  public void should_not_report_auth_failure_when_a_contact_point_went_refused_then_auth() {
    // The same shape, but the sibling was unreachable rather than refused. That is a real
    // connectivity failure and must still veto: "authentication is what is wrong here" would be
    // false for half of what the name resolves to.
    Throwable withUnreachableSibling = authError(node1);
    withUnreachableSibling.addSuppressed(
        new ConnectionInitException(
            "Connect failed", new CompletionException(new ConnectException("connection refused"))));
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(new SimpleEntry<>(node1, withUnreachableSibling)));

    assertThat(ControlConnection.isAuthFailure(error)).isFalse();
  }

  @Test
  public void should_not_report_auth_failure_when_every_node_was_excluded() {
    // Nothing was tried, so there is no verdict to report.
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(
                new SimpleEntry<>(
                    node1, new ControlConnection.ExcludedNodeException("node became ignored")),
                new SimpleEntry<>(
                    node2,
                    new CompletionException(
                        new ControlConnection.ExcludedNodeException("node was removed")))));

    assertThat(ControlConnection.isAuthFailure(error)).isFalse();
  }

  @Test
  public void should_not_report_auth_failure_when_a_node_failed_on_something_else() {
    AllNodesFailedException error =
        AllNodesFailedException.fromErrors(
            ImmutableList.of(
                new SimpleEntry<>(node1, authError(node1)),
                new SimpleEntry<>(node2, new ConnectException("connection refused"))));

    assertThat(ControlConnection.isAuthFailure(error)).isFalse();
  }

  @Test
  public void should_try_next_node_if_the_node_behind_a_contact_point_is_excluded() {
    // Given — init on node1, then a reconnection through a contact point, the way the fallback
    // plan appends the original contact points to every reconnection round.
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);

    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    UUID resolvedHostId = UUID.randomUUID();
    DefaultNode resolvedNode = TestNodeFactory.newNode(2, resolvedHostId, context);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1)
            .success(contactPoint, channel2)
            .success(node1, channel3)
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture)
        .isSuccess(v -> assertThat(controlConnection.channel()).isEqualTo(channel1));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node1));

    NodeInfo resolvedInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel2.getEndPoint())
            .withHostId(resolvedHostId)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2))
        .thenReturn(CompletableFuture.completedFuture(resolvedInfo));
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            invocation -> {
              registeredNodes.put(resolvedNode.getHostId(), resolvedNode);
              return CompletableFuture.completedFuture(resolvedNode);
            });

    // The backend that contact point reaches is one the policy has excluded. The event names the
    // metadata node, because that is the only instance events ever name: the contact point the
    // plan offered is an ephemeral object that is never their subject, so the guards that run
    // before the handshake have nothing to match it against.
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, resolvedNode));

    // When
    mockQueryPlan(contactPoint, node1);
    channel1.close();

    // Then — the channel is abandoned once the handshake says which node is behind it, and the
    // next node is tried. Parking here instead would be permanent: an unchanged distance fires no
    // further event, so nothing downstream would ever dislodge it.
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    factoryHelper.waitForCall(contactPoint);
    verify(channel2, VERIFY_TIMEOUT).forceClose();
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(resolvedNode));
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_not_report_auth_failure_for_a_throwable_with_no_per_node_breakdown() {
    // Only an AllNodesFailedException carries the per-node errors this question is answered from.
    // Anything else has nothing to say about the credentials, and answering yes would send an
    // operator whose connection was refused off to check their authentication configuration.
    assertThat(ControlConnection.isAuthFailure(new ConnectException("connection refused")))
        .isFalse();
  }

  @Test
  public void should_treat_a_lone_authentication_failure_as_auth_only() {
    assertThat(ChannelFactory.isAuthOnly(authError(node1))).isTrue();
  }

  @Test
  public void
      should_not_treat_an_authentication_failure_with_other_suppressed_causes_as_auth_only() {
    // What the per-node log line is decided on. ChannelFactory#surfacedFailure deliberately
    // promotes
    // an authentication failure over transport ones, so the top-level throwable of a hostname whose
    // records failed [refused, refused, auth] is the auth error -- and reporting that as "an
    // authentication error" would say nothing about two thirds of the deployment being unreachable.
    Throwable withUnreachableSibling = authError(node1);
    withUnreachableSibling.addSuppressed(new ConnectException("connection refused"));

    assertThat(ChannelFactory.isAuthOnly(withUnreachableSibling)).isFalse();
  }

  @Test
  public void should_not_treat_a_non_authentication_failure_as_auth_only() {
    assertThat(ChannelFactory.isAuthOnly(new ConnectException("connection refused"))).isFalse();
  }

  @Test
  public void should_never_pair_captured_node_info_with_a_channel_it_did_not_come_from() {
    // The candidate loop can leave a stranded hook behind: an attempt abandoned on the hook timeout
    // whose system.local response arrives anyway, and writes to the holder after the accepted
    // candidate has. Held in two separate volatile fields, that late write lands between the
    // accepted candidate's two writes, and a reader in that window takes the rejected candidate's
    // node info as the accepted channel's -- registering the wrong host id and endpoint for the
    // node
    // the control connection is actually talking to.
    //
    // Published as one reference, that window does not exist: a read either sees a whole (channel,
    // info) pair or the previous whole pair, so the worst a late write can do is make the read miss
    // and fall back to querying the open channel again. Asserted directly rather than raced at,
    // because a race cannot demonstrate the absence of an interleaving -- and a spinning writer
    // against a property that holds structurally is a test that cannot fail.
    ControlConnection.NodeInfoHolder holder = new ControlConnection.NodeInfoHolder();
    DriverChannel accepted = newMockDriverChannel(1);
    DriverChannel abandoned = newMockDriverChannel(2);
    NodeInfo acceptedInfo = mock(NodeInfo.class);
    NodeInfo abandonedInfo = mock(NodeInfo.class);

    assertThat(holder.getFor(accepted)).isNull();

    holder.set(accepted, acceptedInfo);
    assertThat(holder.getFor(accepted)).isSameAs(acceptedInfo);
    // Never the other channel's, even though it is the only info the holder has.
    assertThat(holder.getFor(abandoned)).isNull();

    // A stranded hook's late write replaces the pair wholesale, so the accepted channel's read now
    // misses -- which is the fallback path, not a mispairing.
    holder.set(abandoned, abandonedInfo);
    assertThat(holder.getFor(abandoned)).isSameAs(abandonedInfo);
    assertThat(holder.getFor(accepted)).isNull();
  }

  @Test
  public void should_report_why_it_gave_up_when_the_node_behind_a_contact_point_is_excluded() {
    // Given -- a plan with nothing but a contact point whose backend turns out to be excluded. The
    // event names the metadata node, because that is the only instance events ever name.
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(2);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel).build();
    UUID resolvedHostId = UUID.randomUUID();
    DefaultNode resolvedNode = TestNodeFactory.newNode(2, resolvedHostId, context);
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    // Built outside the when(): calling a mock inside an unfinished stubbing confuses Mockito.
    NodeInfo resolvedInfo =
        DefaultNodeInfo.builder()
            .withEndPoint(channel.getEndPoint())
            .withHostId(resolvedHostId)
            .build();
    when(topologyMonitor.getChannelNodeInfo(channel))
        .thenReturn(CompletableFuture.completedFuture(resolvedInfo));
    when(metadataManager.registerNode(any()))
        .thenReturn(CompletableFuture.completedFuture(resolvedNode));
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, resolvedNode));

    // When -- init, with no other node to fall back to.
    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // Then -- the exclusion is recorded as this node's error, so the operator is told why rather
    // than getting a bare NoNodeAvailableException with no cause at all.
    assertThatStage(initFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(AllNodesFailedException.class);
              assertThat(((AllNodesFailedException) error).getAllErrors())
                  .hasEntrySatisfying(
                      resolvedNode,
                      errors -> assertThat(errors.get(0)).hasMessageContaining("ignored"));
            });
  }

  @Test
  public void should_reconnect_when_the_node_a_pending_channel_reached_becomes_excluded() {
    // Given -- a contact point that is a hostname, so the channel's endpoint (bound by
    // ChannelFactory to the address it actually reached) is the only thing that says which node is
    // at the other end while the identity read is still in flight.
    DefaultNode contactPoint =
        DefaultNode.newContactPoint(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("db.example.invalid", 9042)),
            context);
    mockQueryPlan(contactPoint);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(contactPoint, channel2)
            .success(node1, channel1)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2)).thenReturn(new CompletableFuture<>());

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // When -- a metadata node at the address that channel is connected to is forced down, while the
    // identity read has not come back yet.
    mockQueryPlan(node1);
    eventBus.fire(
        NodeStateEvent.changed(
            NodeState.UP, NodeState.FORCED_DOWN, TestNodeFactory.newNode(2, context)));

    // Then -- the control connection recognizes that as the node it is sitting on and moves. It
    // cannot compare host ids yet, and comparing the two endpoints with equals() would resolve the
    // contact-point hostname inline -- a blocking DNS lookup on the admin thread, answered by
    // whichever address the name lists first, which need not even be this node.
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_reconnect_when_a_pending_proxy_channel_reached_an_excluded_node() {
    // Given -- a Cloud-style deployment. The contact point and the metadata node are SniEndPoints
    // over the same proxy and server name, and the channel carries the copy ChannelFactory pinned
    // to
    // the proxy IP it actually reached. resolve() therefore answers an unresolved proxy name on the
    // node side and a concrete IP on the channel side, and an InetSocketAddress carrying an
    // InetAddress never equals one that does not -- so comparing resolve() results here would say
    // "not the control node" for every Cloud, client-route and unresolved-translator deployment.
    InetSocketAddress proxy = InetSocketAddress.createUnresolved("proxy.example.invalid", 9142);
    String serverName = "1e9a4d0c-0000-0000-0000-00000000002a";
    SniEndPoint proxyEndPoint = new SniEndPoint(proxy, serverName);
    DefaultNode contactPoint = DefaultNode.newContactPoint(proxyEndPoint, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel2 = newMockDriverChannel(2);
    when(channel2.getEndPoint())
        .thenReturn(proxyEndPoint.pinTo(new InetSocketAddress("127.0.0.2", 9142)));
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(contactPoint, channel2)
            .success(node1, channel1)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2)).thenReturn(new CompletableFuture<>());

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // When -- the metadata node behind that same proxy is forced down, while the identity read has
    // not come back yet.
    mockQueryPlan(node1);
    eventBus.fire(
        NodeStateEvent.changed(
            NodeState.UP,
            NodeState.FORCED_DOWN,
            new DefaultNode(new SniEndPoint(proxy, serverName), context)));

    // Then -- the control connection recognizes that as the node it is sitting on and moves. The
    // proxy endpoint's identity does not key on the address at all (proxy + server name), and its
    // metric prefix is derived from exactly those two, so comparing prefixes answers this without
    // resolving anything.
    factoryHelper.waitForCall(node1);
  }

  @Test
  public void should_bound_the_connect_hook_beyond_the_deadline_of_the_query_it_wraps() {
    // The hook wraps a system.local read, and AdminRequestHandler already times that read out on
    // CONTROL_CONNECTION_TIMEOUT -- from a value DefaultTopologyMonitor snapshotted in its
    // constructor, while this reads the option live. Giving the hook the same number leaves the two
    // deadlines with no defined order, and which one fires decides what the operator is told:
    // "Connect hook timed out" names the wrapper, the inner DriverTimeoutException names the query
    // and the node. The hook is the backstop for a stage that never completes at all -- which a
    // custom TopologyMonitor can produce and the built-in one cannot -- so it has to be the looser
    // of the two.
    Duration configured = Duration.ofSeconds(3);
    when(defaultProfile.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(configured);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(3, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).success(contactPoint, channel).build();

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    assertThat(connectOptions(contactPoint).connectHookTimeout).isGreaterThan(configured);
  }

  @Test
  public void should_spend_the_refusal_budget_when_the_exclusion_is_only_suppressed() {
    // The budget above was reachable only when the refusal was the failure the round surfaced. It
    // usually is not. A contact point reports one failure for the whole name with the other
    // addresses' failures attached as suppressed, and ChannelFactory#surfacedFailure ranks an
    // authentication failure above a refusal -- so [excluded, bad-credentials] arrives as the
    // AuthenticationException with the refusal suppressed, deterministically, every round.
    //
    // Both readers of the round have to look equally deep at that. #anyNodeUnreachable does, and
    // correctly declines to call such a round unreachable; a shallower #anyNodeExcluded would
    // decline too, and the round would fall between them -- clearing nothing, spending nothing, for
    // the life of the session. Which is the deadlock the budget exists to break, reintroduced.
    UUID hostId = node2.getHostId();
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel5 = newMockDriverChannel(5);
    DefaultNode contactPoint = TestNodeFactory.newContactPoint(2, context);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node1, channel1) // init
            .failure(contactPoint, excludedThenAuth(contactPoint)) // round 1: budget 1 of 3
            .failure(contactPoint, excludedThenAuth(contactPoint)) // round 2: budget 2 of 3
            .failure(contactPoint, excludedThenAuth(contactPoint)) // round 3: spent -- set cleared
            .success(contactPoint, channel5) // round 4: nothing left refusing it
            .build();

    CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
    factoryHelper.waitForCall(node1);
    assertThatStage(initFuture).isSuccess();

    // node2 is removed and never reports a state again, so nothing can un-remove it the usual way.
    eventBus.fire(NodeStateEvent.removed(node2));
    registeredNodes.remove(hostId);

    NodeInfo acceptedInfo =
        DefaultNodeInfo.builder().withEndPoint(channel5.getEndPoint()).withHostId(hostId).build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel5))
        .thenReturn(CompletableFuture.completedFuture(acceptedInfo));
    DefaultNode reRegistered = TestNodeFactory.newNode(2, hostId, context);
    when(metadataManager.registerNode(any()))
        .thenAnswer(
            i -> {
              registeredNodes.put(hostId, reRegistered);
              return CompletableFuture.completedFuture(reRegistered);
            });

    mockQueryPlan(contactPoint);
    channel1.close();

    // Then -- the fourth round is let through, so the three rounds before it really did count.
    factoryHelper.waitForCalls(contactPoint, 4);
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(reRegistered));
    await().untilAsserted(() -> assertThat(controlConnection.channel()).isEqualTo(channel5));
  }

  @Test
  public void should_warn_about_credentials_when_something_else_is_what_surfaced() {
    // advanced.connection.warn-on-init-error is false here, which is the setting that makes this
    // observable: it mutes the noise of nodes that cannot be reached, and was never a switch for
    // "your credentials are wrong". So a round that includes a credential rejection has to warn
    // whatever else it includes -- and which of the set arrives here is surfacedFailure's choice,
    // not the operator's. It ranks a node-wide failure above an authentication one, so the
    // rejected login commonly travels as a suppressed exception, and a test on the type of what
    // arrived would send exactly this case down the muted path.
    Logger logger = (Logger) LoggerFactory.getLogger(ControlConnection.class);
    Level levelBefore = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
    try {
      DriverChannel channel1 = newMockDriverChannel(1);
      MockChannelFactoryHelper factoryHelper =
          MockChannelFactoryHelper.builder(channelFactory)
              .failure(node1, authBehindNodeWideFailure(node1))
              .success(node2, channel1)
              .build();

      CompletionStage<Void> initFuture = controlConnection.init(false, false, false);
      factoryHelper.waitForCall(node1);
      factoryHelper.waitForCall(node2);
      assertThatStage(initFuture).isSuccess();

      ArgumentCaptor<ILoggingEvent> logs = ArgumentCaptor.forClass(ILoggingEvent.class);
      verify(appender, VERIFY_TIMEOUT.atLeastOnce()).doAppend(logs.capture());
      assertThat(logs.getAllValues())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage()).contains("authentication failed on some");
              });
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(levelBefore);
    }
  }

  @Test
  public void should_reconnect_when_a_pending_channel_is_pinned_to_a_hostname_contact_point() {
    // Given -- a plain hostname contact point, which is now kept unresolved, and a channel carrying
    // the copy ChannelFactory pinned to the one address it reached. So resolve() answers an
    // unresolved name on the node side and a concrete IP on the channel side: the resolve()
    // comparison cannot match, and equals() is the one thing that must not be asked, because
    // DefaultEndPoint#equals resolves the unresolved side -- a blocking DNS lookup on the admin
    // thread, on whichever address the name happens to list first (issue #1006).
    InetSocketAddress hostname = InetSocketAddress.createUnresolved("db.example.invalid", 9042);
    DefaultEndPoint hostnameEndPoint = new DefaultEndPoint(hostname);
    DefaultNode contactPoint = DefaultNode.newContactPoint(hostnameEndPoint, context);
    mockQueryPlan(contactPoint);
    DriverChannel channel2 = newMockDriverChannel(2);
    when(channel2.getEndPoint())
        .thenReturn(hostnameEndPoint.pinTo(new InetSocketAddress("127.0.0.2", 9042)));
    DriverChannel channel1 = newMockDriverChannel(1);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(contactPoint, channel2)
            .success(node1, channel1)
            .build();
    TopologyMonitor topologyMonitor = context.getTopologyMonitor();
    when(topologyMonitor.getChannelNodeInfo(channel2)).thenReturn(new CompletableFuture<>());

    controlConnection.init(false, false, false);
    factoryHelper.waitForCall(contactPoint);

    // When -- that same contact point is forced down while the identity read has not come back, so
    // its node still holds the unresolved name.
    mockQueryPlan(node1);
    eventBus.fire(
        NodeStateEvent.changed(
            NodeState.UP, NodeState.FORCED_DOWN, new DefaultNode(hostnameEndPoint, context)));

    // Then -- the control connection recognizes that as the node it is sitting on and moves. A pin
    // is excluded from the metric identity by PinnableEndPoint's contract, so the name and the
    // channel pinned to one of its addresses carry the same prefix, and comparing those answers
    // this without resolving anything.
    factoryHelper.waitForCall(node1);
  }

  /**
   * One contact-point name, two addresses: the first belongs to a node this connection may not use
   * and is refused inside the connect hook, the second reaches a server that rejects the
   * credentials. Wrapped the way {@code ChannelFactory#finishCandidate} wraps a hook rejection, and
   * surfaced the way {@code ChannelFactory#surfacedFailure} surfaces this pair.
   */
  private static Throwable excludedThenAuth(Node node) {
    Throwable error = authError(node);
    error.addSuppressed(
        new ConnectionInitException(
            "Connect hook rejected the channel",
            new CompletionException(
                new ControlConnection.ExcludedNodeException("node was removed"))));
    return error;
  }

  /**
   * The same shape with the ranking that hides it: the surfaced failure is node-wide, which {@code
   * ChannelFactory#surfacedFailure} promotes over an authentication one, so the rejected login is
   * reachable only through {@link Throwable#getSuppressed()}.
   */
  private static Throwable authBehindNodeWideFailure(Node node) {
    Throwable error =
        new ConnectionInitException(
            "Server does not support the CLIENT_ROUTES_CHANGE event type", null);
    error.addSuppressed(authError(node));
    return error;
  }

  private static Throwable authError(Node node) {
    return new AuthenticationException(node.getEndPoint(), "mock authentication failure");
  }
}
