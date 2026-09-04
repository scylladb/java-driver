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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy.DistanceReporter;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoadBalancingPolicyWrapperTest {

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;

  private Set<DefaultNode> contactPoints;
  private Queue<Node> defaultPolicyQueryPlan;

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;
  @Mock private LoadBalancingPolicy policy1;
  @Mock private LoadBalancingPolicy policy2;
  @Mock private LoadBalancingPolicy policy3;
  private EventBus eventBus;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;
  @Mock private TopologyMonitor topologyMonitor;
  @Mock protected MetricsFactory metricsFactory;
  @Captor private ArgumentCaptor<Map<UUID, Node>> initNodesCaptor;

  private LoadBalancingPolicyWrapper wrapper;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);
    node3 = TestNodeFactory.newNode(3, context);

    contactPoints = ImmutableSet.of(node1, node2);
    Map<UUID, Node> allNodes =
        ImmutableMap.of(
            Objects.requireNonNull(node1.getHostId()), node1,
            Objects.requireNonNull(node2.getHostId()), node2,
            Objects.requireNonNull(node3.getHostId()), node3);
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getNodes()).thenReturn(allNodes);
    when(metadataManager.getContactPoints()).thenReturn(contactPoints);
    when(context.getMetadataManager()).thenReturn(metadataManager);
    when(context.getTopologyMonitor()).thenReturn(topologyMonitor);

    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS))
        .thenReturn(false);

    // Use a real built-in QueryPlan (not a mutable LinkedList): its add()/addAll() throw, so the
    // control-reconnection plan must compose rather than mutate it (see CompositeQueryPlan usage).
    defaultPolicyQueryPlan = new SimpleQueryPlan(node3, node2, node1);
    when(policy1.newQueryPlan(null, null)).thenReturn(defaultPolicyQueryPlan);

    eventBus = spy(new EventBus("test"));
    when(context.getEventBus()).thenReturn(eventBus);

    wrapper =
        new LoadBalancingPolicyWrapper(
            context,
            ImmutableMap.of(
                DriverExecutionProfile.DEFAULT_NAME,
                policy1,
                "profile1",
                policy1,
                "profile2",
                policy2,
                "profile3",
                policy3));
  }

  @Test
  public void should_build_control_connection_query_plan_from_contact_points_before_init() {
    // When — before init, the control-reconnection plan is built straight from the contact points
    // (bypassing the load balancing policies), so each hostname can be tried on the first connect.
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then — query plan contains the contact points, and no policy was consulted
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy, never()).newQueryPlan(null, null);
    }
    assertThat(queryPlan).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_build_query_plan_from_contact_points_before_init() {
    // When — before init, the query plan is built straight from the contact points (bypassing the
    // load balancing policies)
    Queue<Node> queryPlan = wrapper.newQueryPlan(null, DriverExecutionProfile.DEFAULT_NAME, null);

    // Then — query plan contains the contact points, and no policy was consulted
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy, never()).newQueryPlan(null, null);
    }
    assertThat(queryPlan).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_fetch_query_plan_from_policy_after_init() {
    // Given
    wrapper.init();
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).init(anyMap(), any(DistanceReporter.class));
    }

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then
    // no-arg newQueryPlan() uses the default profile
    verify(policy1).newQueryPlan(null, null);
    assertThat(queryPlan).isEqualTo(defaultPolicyQueryPlan);
  }

  @Test
  public void should_fetch_control_connection_query_plan_from_policy_after_init() {
    // Given
    wrapper.init();
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).init(anyMap(), any(DistanceReporter.class));
    }

    // When
    Queue<Node> queryPlan = wrapper.newQueryPlan(null, DriverExecutionProfile.DEFAULT_NAME, null);

    // Then
    // no-arg newQueryPlan() uses the default profile
    verify(policy1).newQueryPlan(null, null);
    assertThat(queryPlan).isEqualTo(defaultPolicyQueryPlan);
  }

  @Test
  public void
      should_append_contact_points_to_query_plan_when_reconnect_contact_points_is_enabled() {
    // Given
    when(defaultProfile.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS))
        .thenReturn(true);
    wrapper.init();

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then
    // 3 policy nodes + 2 contact point nodes
    assertThat(queryPlan.size()).isEqualTo(5);
    // First nodes come from the policy query plan (node3, node2, node1)
    assertThat(queryPlan.poll()).isEqualTo(node3);
    assertThat(queryPlan.poll()).isEqualTo(node2);
    assertThat(queryPlan.poll()).isEqualTo(node1);
    // Remaining nodes are the original contact points appended at the end. DefaultNode does not
    // override equals, so comparing against the retained instances is an identity check.
    assertThat(queryPlan).containsExactlyInAnyOrderElementsOf(contactPoints);
  }

  @Test
  public void should_reuse_the_retained_contact_point_nodes_rather_than_minting_copies() {
    // Given — the flag now defaults to true, so this plan is built on every control-connection
    // reconnection round for every user, not only for those who opted in.
    when(defaultProfile.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS))
        .thenReturn(true);
    when(topologyMonitor.reresolvesNodeAddresses()).thenReturn(true);
    wrapper.init();
    when(policy1.newQueryPlan(null, null)).thenReturn(QueryPlan.EMPTY);

    // When — two rounds, as a reconnection sequence would produce.
    Queue<Node> firstPlan = wrapper.newControlReconnectionQueryPlan();
    Queue<Node> secondPlan = wrapper.newControlReconnectionQueryPlan();

    // Then — both plans hand out the very objects MetadataManager retains, not per-plan copies.
    // A copy would be invisible to the metric updater registered for the real node and would fire
    // its own controlConnectionFailed event, once per contact point per round, for as long as
    // reconnection lasts.
    //
    // The size is asserted first, deliberately: iterating an empty plan would satisfy the loops
    // below without checking anything, so a regression that stopped appending the fallback at all
    // would pass. DefaultNode does not override equals, so containment is an identity check.
    assertThat(firstPlan).hasSize(2);
    assertThat(secondPlan).hasSize(2);
    for (Node node : firstPlan) {
      assertThat(contactPoints).contains((DefaultNode) node);
    }
    for (Node node : secondPlan) {
      assertThat(contactPoints).contains((DefaultNode) node);
    }
    // And the retained set itself is untouched by the shuffle.
    assertThat(contactPoints).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_not_duplicate_contact_points_before_init() {
    // Given — the wrapper hasn't been init()-ed yet (state=BEFORE_INIT), so newQueryPlan() already
    // builds the regular plan directly from the contact points. The reconnect-contact-points flag
    // doesn't matter here: newControlReconnectionQueryPlan() short-circuits on state before even
    // reading it, since appending contact points again pre-init would just duplicate every entry in
    // the plan.

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then — the contact points are read only once (not once for the regular plan and again for a
    // redundant "fallback" append), and the plan has no duplicate entries.
    verify(metadataManager, times(1)).getContactPoints();
    assertThat(queryPlan).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void
      should_not_append_contact_points_to_query_plan_when_reconnect_contact_points_is_disabled() {
    // Given — the flag defaults to false in the test setup (see @Before)
    wrapper.init();

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then
    // Only the policy query plan is returned; no contact points are appended.
    assertThat(queryPlan).isEqualTo(defaultPolicyQueryPlan);
  }

  @Test
  public void
      should_not_append_contact_points_to_query_plan_when_topology_monitor_reresolves_addresses() {
    // Given — the flag is enabled, but the topology monitor re-resolves node addresses on its own
    // (e.g. a proxy-based monitor such as client routes or the cloud SNI proxy).
    when(defaultProfile.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS))
        .thenReturn(true);
    when(topologyMonitor.reresolvesNodeAddresses()).thenReturn(true);
    wrapper.init();

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then
    // Contact points must not be appended: the monitor keeps addresses fresh, and appending raw
    // contact points could resurrect nodes it has authoritatively removed.
    assertThat(queryPlan).isEqualTo(defaultPolicyQueryPlan);
  }

  @Test
  public void
      should_append_contact_points_when_query_plan_empty_even_if_topology_monitor_reresolves() {
    // Given — the flag is enabled and the topology monitor re-resolves node addresses on its own,
    // but the live-node query plan is empty. With no node to try, reconnection can only recover
    // through the contact-point fallback, so it must be appended despite the re-resolving monitor.
    when(defaultProfile.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS))
        .thenReturn(true);
    when(topologyMonitor.reresolvesNodeAddresses()).thenReturn(true);
    wrapper.init();
    when(policy1.newQueryPlan(null, null)).thenReturn(QueryPlan.EMPTY);

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then — the retained contact-point instances are appended, in some order.
    assertThat(queryPlan).containsExactlyInAnyOrderElementsOf(contactPoints);
  }

  @Test
  public void should_return_contact_points_when_query_plan_empty_and_flag_enabled() {
    // Given
    when(defaultProfile.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS))
        .thenReturn(true);
    wrapper.init();
    // Make the policy return an empty query plan (QueryPlan.EMPTY, as the real policies do)
    when(policy1.newQueryPlan(null, null)).thenReturn(QueryPlan.EMPTY);

    // When
    Queue<Node> queryPlan = wrapper.newControlReconnectionQueryPlan();

    // Then
    // Should get the retained contact-point instances themselves.
    assertThat(queryPlan).containsExactlyInAnyOrderElementsOf(contactPoints);
  }

  @Test
  public void should_init_policies_with_all_nodes() {
    // Given
    node1.state = NodeState.UP;
    node2.state = NodeState.UNKNOWN;
    node3.state = NodeState.DOWN;

    // When
    wrapper.init();

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).init(initNodesCaptor.capture(), any(DistanceReporter.class));
      Map<UUID, Node> initNodes = initNodesCaptor.getValue();
      assertThat(initNodes.values()).containsOnly(node1, node2, node3);
    }
  }

  @Test
  public void should_propagate_distances_from_policies() {
    // Given
    wrapper.init();
    ArgumentCaptor<DistanceReporter> captor1 = ArgumentCaptor.forClass(DistanceReporter.class);
    verify(policy1).init(anyMap(), captor1.capture());
    DistanceReporter distanceReporter1 = captor1.getValue();
    ArgumentCaptor<DistanceReporter> captor2 = ArgumentCaptor.forClass(DistanceReporter.class);
    verify(policy2).init(anyMap(), captor2.capture());
    DistanceReporter distanceReporter2 = captor1.getValue();
    ArgumentCaptor<DistanceReporter> captor3 = ArgumentCaptor.forClass(DistanceReporter.class);
    verify(policy3).init(anyMap(), captor3.capture());
    DistanceReporter distanceReporter3 = captor3.getValue();

    InOrder inOrder = inOrder(eventBus);

    // When
    distanceReporter1.setDistance(node1, NodeDistance.REMOTE);

    // Then
    // first event defines the distance
    inOrder.verify(eventBus).fire(new DistanceEvent(NodeDistance.REMOTE, node1));

    // When
    distanceReporter2.setDistance(node1, NodeDistance.REMOTE);

    // Then
    // event is ignored if the node is already at this distance
    inOrder.verify(eventBus, times(0)).fire(any(DistanceEvent.class));

    // When
    distanceReporter2.setDistance(node1, NodeDistance.LOCAL);

    // Then
    // event is applied if it sets a smaller distance
    inOrder.verify(eventBus).fire(new DistanceEvent(NodeDistance.LOCAL, node1));

    // When
    distanceReporter3.setDistance(node1, NodeDistance.IGNORED);

    // Then
    // event is ignored if the node is already at a closer distance
    inOrder.verify(eventBus, times(0)).fire(any(DistanceEvent.class));
  }

  @Test
  public void should_not_propagate_node_states_to_policies_until_init() {
    // When
    eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.UP, node1));

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy, never()).onUp(node1);
    }
  }

  @Test
  public void should_propagate_node_states_to_policies_after_init() {
    // Given
    wrapper.init();

    // When
    eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.UP, node1));

    // Then
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).onUp(node1);
    }
  }

  @Test
  public void should_accumulate_events_during_init_and_replay() throws InterruptedException {
    // Given
    // Hack to obtain concurrency: the main thread releases another thread and blocks; then the
    // other thread fires an event on the bus and unblocks the main thread.
    CountDownLatch eventLatch = new CountDownLatch(1);
    CountDownLatch initLatch = new CountDownLatch(1);

    // When
    Runnable runnable =
        () -> {
          try {
            eventLatch.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          eventBus.fire(NodeStateEvent.changed(NodeState.UNKNOWN, NodeState.DOWN, node1));
          initLatch.countDown();
        };
    Thread thread = new Thread(runnable);
    thread.start();
    wrapper.init();

    // Then
    // unblock the thread that will fire the event, and waits until it finishes
    eventLatch.countDown();
    boolean ok = initLatch.await(500, TimeUnit.MILLISECONDS);
    assertThat(ok).isTrue();
    for (LoadBalancingPolicy policy : ImmutableList.of(policy1, policy2, policy3)) {
      verify(policy).onDown(node1);
    }
    thread.join(500);
    assertThat(thread.isAlive()).isFalse();
  }
}
