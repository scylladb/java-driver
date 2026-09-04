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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AddNodeRefreshTest {

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;
  @Mock private ChannelFactory channelFactory;

  private DefaultNode node1;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(context.getChannelFactory()).thenReturn(channelFactory);
    node1 = TestNodeFactory.newNode(1, context);
  }

  @Test
  public void should_add_new_node() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    UUID newHostId = Uuids.random();
    DefaultEndPoint newEndPoint = TestNodeFactory.newEndPoint(2);
    UUID newSchemaVersion = Uuids.random();
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(newHostId)
            .withEndPoint(newEndPoint)
            .withDatacenter("dc1")
            .withRack("rack2")
            .withSchemaVersion(newSchemaVersion)
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(newNodeInfo);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(node1.getHostId(), newHostId);
    Node node2 = newNodes.get(newHostId);
    assertThat(node2.getEndPoint()).isEqualTo(newEndPoint);
    assertThat(node2.getDatacenter()).isEqualTo("dc1");
    assertThat(node2.getRack()).isEqualTo("rack2");
    assertThat(node2.getHostId()).isEqualTo(newHostId);
    assertThat(node2.getSchemaVersion()).isEqualTo(newSchemaVersion);
    assertThat(result.events).containsExactly(NodeStateEvent.added((DefaultNode) node2));
  }

  @Test
  public void should_not_add_existing_node_with_same_id_and_endpoint() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    // Carrying the broadcast RPC address it would really arrive with: that is what the guard
    // compares, and findInPeers never builds a NodeInfo without one.
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(node1.getHostId())
            .withEndPoint(node1.getEndPoint())
            .withBroadcastRpcAddress(
                node1.getBroadcastRpcAddress().orElseThrow(AssertionError::new))
            .withDatacenter("dc1")
            .withRack("rack2")
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(newNodeInfo);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    assertThat(result.newMetadata.getNodes()).containsOnlyKeys(node1.getHostId());
    // Info is not copied over:
    assertThat(node1.getDatacenter()).isNull();
    assertThat(node1.getRack()).isNull();
    assertThat(result.events).isEmpty();
  }

  @Test
  public void should_add_existing_node_with_same_id_but_different_endpoint() {
    // Given
    DefaultMetadata oldMetadata =
        new DefaultMetadata(
            ImmutableMap.of(node1.getHostId(), node1), Collections.emptyMap(), null, null);
    DefaultEndPoint newEndPoint = TestNodeFactory.newEndPoint(2);
    InetSocketAddress newBroadcastRpcAddress = newEndPoint.resolve();
    UUID newSchemaVersion = Uuids.random();
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(node1.getHostId())
            .withEndPoint(newEndPoint)
            .withDatacenter("dc1")
            .withRack("rack2")
            .withSchemaVersion(newSchemaVersion)
            .withBroadcastRpcAddress(newBroadcastRpcAddress)
            .build();
    AddNodeRefresh refresh = new AddNodeRefresh(newNodeInfo);

    // When
    MetadataRefresh.Result result = refresh.compute(oldMetadata, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).hasSize(1).containsEntry(node1.getHostId(), node1);
    assertThat(node1.getEndPoint()).isEqualTo(newEndPoint);
    assertThat(node1.getDatacenter()).isEqualTo("dc1");
    assertThat(node1.getRack()).isEqualTo("rack2");
    assertThat(node1.getSchemaVersion()).isEqualTo(newSchemaVersion);
    assertThat(result.events).containsExactly(TopologyEvent.suggestUp(newBroadcastRpcAddress));
  }

  @Test
  public void should_not_add_existing_node_whose_endpoint_only_changed_representation() {
    // The same node described two ways by two of the driver's own code paths.
    // DefaultTopologyMonitor#connectedNodeEndPoint gives the control node an identity built from
    // the address its channel reached, while that node's peers row would give it whatever the
    // configured AddressTranslator returns -- an unresolved name, under a translator with
    // resolve-addresses = false. Cassandra reports a restart as a NEW_NODE even when the host id
    // did not change, so those two forms are compared here, and nothing about the node's addressing
    // has moved.
    //
    // Comparing endpoints reports a change on every such event: EndPoint#equals would resolve the
    // unresolved side (a blocking lookup on the admin event loop, answered by whichever address the
    // resolver lists first -- issue #1006) and PinnableEndPoint#sameIdentity compares metric
    // identity, which is precisely what differs. Either one copies the peers-derived endpoint in,
    // flipping the node's metric prefix and clearing its series, and asks for a pool bounce.
    //
    // Derived from one InetAddress and spelled as its own literal, so that nothing here resolves:
    // rebuilding a literal parses.
    InetAddress loopback = InetAddress.getLoopbackAddress();
    InetSocketAddress broadcastRpcAddress = new InetSocketAddress(loopback, 9042);
    DefaultNode existing = new DefaultNode(new DefaultEndPoint(broadcastRpcAddress), context);
    UUID hostId = Uuids.random();
    existing.hostId = hostId;
    existing.broadcastRpcAddress = broadcastRpcAddress;
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(hostId, existing), Collections.emptyMap(), null, null);

    DefaultEndPoint asName =
        new DefaultEndPoint(InetSocketAddress.createUnresolved(loopback.getHostAddress(), 9042));
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(hostId)
            .withEndPoint(asName)
            .withBroadcastRpcAddress(broadcastRpcAddress)
            .build();

    MetadataRefresh.Result result =
        new AddNodeRefresh(newNodeInfo).compute(oldMetadata, false, context);

    assertThat(result.events).isEmpty();
    assertThat(existing.getEndPoint()).isEqualTo(new DefaultEndPoint(broadcastRpcAddress));
  }

  @Test
  public void should_ignore_new_node_info_without_a_broadcast_rpc_address() {
    // TopologyMonitor#getNewNodeInfo is an extension point, and nothing stops an implementation
    // returning a NodeInfo with no broadcast RPC address -- findInPeers always supplies one, but a
    // custom monitor need not. An absent address satisfies the inequality against the existing
    // node's present one, so before the guard this reached Optional#get() and threw
    // NoSuchElementException out of MetadataRefresh#compute, which MetadataManager#apply does not
    // catch. An `assert` cannot hold that line: it is not enabled in production.
    InetAddress loopback = InetAddress.getLoopbackAddress();
    InetSocketAddress broadcastRpcAddress = new InetSocketAddress(loopback, 9042);
    DefaultNode existing = new DefaultNode(new DefaultEndPoint(broadcastRpcAddress), context);
    UUID hostId = Uuids.random();
    existing.hostId = hostId;
    existing.broadcastRpcAddress = broadcastRpcAddress;
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(hostId, existing), Collections.emptyMap(), null, null);

    // No withBroadcastRpcAddress(...) call, and a different endpoint, so nothing else could make
    // this a no-op.
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(hostId)
            .withEndPoint(new DefaultEndPoint(new InetSocketAddress(loopback, 9043)))
            .build();

    MetadataRefresh.Result result =
        new AddNodeRefresh(newNodeInfo).compute(oldMetadata, false, context);

    assertThat(result.events).isEmpty();
    // Not adopted either: there is nothing to compare, so the refresh changes nothing at all.
    assertThat(existing.getEndPoint()).isEqualTo(new DefaultEndPoint(broadcastRpcAddress));
  }

  @Test
  public void should_add_existing_node_that_moved_behind_a_shared_translator_name() {
    // The mirror, and the direction the endpoint comparison lost outright. A translator that hands
    // back a name maps every node it covers to the same endpoint -- FixedHostNameAddressTranslator
    // returns one advertised hostname for the whole cluster -- so a node that really did change its
    // broadcast RPC address arrives with an endpoint identical to the one it already had. Comparing
    // endpoints calls that "unchanged" and the node's pool is never told to reconnect, on the one
    // event that exists to tell it.
    DefaultEndPoint advertised =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("db.example.com", 9042));
    DefaultNode existing = new DefaultNode(advertised, context);
    UUID hostId = Uuids.random();
    existing.hostId = hostId;
    existing.broadcastRpcAddress = new InetSocketAddress("127.0.0.1", 9042);
    DefaultMetadata oldMetadata =
        new DefaultMetadata(ImmutableMap.of(hostId, existing), Collections.emptyMap(), null, null);

    InetSocketAddress movedTo = new InetSocketAddress("127.0.0.2", 9042);
    DefaultNodeInfo newNodeInfo =
        DefaultNodeInfo.builder()
            .withHostId(hostId)
            .withEndPoint(advertised)
            .withBroadcastRpcAddress(movedTo)
            .withRack("rack2")
            .build();

    MetadataRefresh.Result result =
        new AddNodeRefresh(newNodeInfo).compute(oldMetadata, false, context);

    assertThat(result.events).containsExactly(TopologyEvent.suggestUp(movedTo));
    assertThat(existing.getRack()).isEqualTo("rack2");
  }
}
