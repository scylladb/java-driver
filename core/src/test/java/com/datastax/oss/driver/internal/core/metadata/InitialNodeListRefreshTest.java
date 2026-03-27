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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InitialNodeListRefreshTest {

  @Mock private InternalDriverContext context;
  @Mock protected MetricsFactory metricsFactory;
  @Mock private ChannelFactory channelFactory;
  @Mock private MetadataManager metadataManager;

  private EndPoint endPoint1;
  private EndPoint endPoint2;
  private EndPoint endPoint3;
  private UUID hostId1;
  private UUID hostId2;
  private UUID hostId3;
  private UUID hostId4;
  private UUID hostId5;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(context.getChannelFactory()).thenReturn(channelFactory);

    endPoint1 = TestNodeFactory.newEndPoint(1);
    endPoint2 = TestNodeFactory.newEndPoint(2);
    endPoint3 = TestNodeFactory.newEndPoint(3);
    hostId1 = UUID.randomUUID();
    hostId2 = UUID.randomUUID();
    hostId3 = UUID.randomUUID();
    hostId4 = UUID.randomUUID();
    hostId5 = UUID.randomUUID();
  }

  @Test
  public void should_create_new_nodes_for_all_endpoints() {
    // Given
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder().withEndPoint(endPoint1).withHostId(hostId1).build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint2).withHostId(hostId2).build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint3).withHostId(hostId3).build(),
            DefaultNodeInfo.builder()
                // address translator can translate node addresses to the same endpoints
                .withEndPoint(endPoint2)
                .withHostId(hostId4)
                .build(),
            DefaultNodeInfo.builder()
                // address translator can translate node addresses to the same endpoints
                .withEndPoint(endPoint3)
                .withHostId(hostId5)
                .build());
    InitialNodeListRefresh refresh = new InitialNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1, hostId2, hostId3, hostId4, hostId5);
    assertThat(newNodes.get(hostId1).getEndPoint()).isEqualTo(endPoint1);
    assertThat(newNodes.get(hostId1).getHostId()).isEqualTo(hostId1);
    assertThat(newNodes.get(hostId2).getEndPoint()).isEqualTo(endPoint2);
    assertThat(newNodes.get(hostId2).getHostId()).isEqualTo(hostId2);
    assertThat(newNodes.get(hostId3).getEndPoint()).isEqualTo(endPoint3);
    assertThat(newNodes.get(hostId3).getHostId()).isEqualTo(hostId3);
    assertThat(newNodes.get(hostId4).getEndPoint()).isEqualTo(endPoint2);
    assertThat(newNodes.get(hostId4).getHostId()).isEqualTo(hostId4);
    assertThat(newNodes.get(hostId5).getEndPoint()).isEqualTo(endPoint3);
    assertThat(newNodes.get(hostId5).getHostId()).isEqualTo(hostId5);
    assertThat(result.events)
        .containsExactlyInAnyOrder(
            NodeStateEvent.added((DefaultNode) newNodes.get(hostId1)),
            NodeStateEvent.added((DefaultNode) newNodes.get(hostId2)),
            NodeStateEvent.added((DefaultNode) newNodes.get(hostId3)),
            NodeStateEvent.added((DefaultNode) newNodes.get(hostId4)),
            NodeStateEvent.added((DefaultNode) newNodes.get(hostId5)));
  }

  @Test
  public void should_add_all_nodes() {
    // Given
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder().withEndPoint(endPoint1).withHostId(hostId1).build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint2).withHostId(hostId2).build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint3).withHostId(hostId3).build());
    InitialNodeListRefresh refresh = new InitialNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1, hostId2, hostId3);
    assertThat(newNodes.get(hostId1).getEndPoint()).isEqualTo(endPoint1);
    assertThat(newNodes.get(hostId2).getEndPoint()).isEqualTo(endPoint2);
    assertThat(newNodes.get(hostId3).getEndPoint()).isEqualTo(endPoint3);
  }

  @Test
  public void should_ignore_duplicate_host_ids() {
    // Given
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(endPoint1)
                .withHostId(hostId1)
                .withDatacenter("dc1")
                .build(),
            DefaultNodeInfo.builder()
                .withEndPoint(endPoint1)
                .withDatacenter("dc2")
                .withHostId(hostId1)
                .build());
    InitialNodeListRefresh refresh = new InitialNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(DefaultMetadata.EMPTY, false, context);

    // Then
    // only the first nodeInfo should have been used
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1);
    assertThat(newNodes.get(hostId1).getEndPoint()).isEqualTo(endPoint1);
    assertThat(newNodes.get(hostId1).getHostId()).isEqualTo(hostId1);
    assertThat(((DefaultNode) newNodes.get(hostId1)).getDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_reuse_control_node_from_metadata() {
    // Given — the control connection registered this node in metadata before the refresh
    DefaultNode controlNode = TestNodeFactory.newNode(1, hostId1, context);
    controlNode.openConnections = 1; // simulate control connection
    DefaultMetadata metadataWithControlNode =
        new DefaultMetadata(
            ImmutableMap.of(hostId1, controlNode), Collections.emptyMap(), null, null);

    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder()
                .withEndPoint(controlNode.getEndPoint())
                .withHostId(hostId1)
                .withDatacenter("dc1")
                .build(),
            DefaultNodeInfo.builder().withEndPoint(endPoint2).withHostId(hostId2).build());
    InitialNodeListRefresh refresh = new InitialNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(metadataWithControlNode, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1, hostId2);
    // The control node instance should be reused (same object)
    assertThat(newNodes.get(hostId1)).isSameAs(controlNode);
    // Connection state should be preserved
    assertThat(newNodes.get(hostId1).getOpenConnections()).isEqualTo(1);
    // But metadata should be populated from NodeInfo
    assertThat(((DefaultNode) newNodes.get(hostId1)).getDatacenter()).isEqualTo("dc1");
    // No added event for the reused control node; only for the new node
    assertThat(result.events)
        .containsExactly(NodeStateEvent.added((DefaultNode) newNodes.get(hostId2)));
  }

  @Test
  public void should_emit_removed_event_when_control_node_not_in_discovered_list() {
    // Given — the control connection registered this node in metadata before the refresh
    DefaultNode controlNode = TestNodeFactory.newNode(1, hostId1, context);
    controlNode.openConnections = 1;
    DefaultMetadata metadataWithControlNode =
        new DefaultMetadata(
            ImmutableMap.of(hostId1, controlNode), Collections.emptyMap(), null, null);

    // The discovered node list does NOT contain the control node's hostId
    Iterable<NodeInfo> newInfos =
        ImmutableList.of(
            DefaultNodeInfo.builder().withEndPoint(endPoint2).withHostId(hostId2).build());
    InitialNodeListRefresh refresh = new InitialNodeListRefresh(newInfos);

    // When
    MetadataRefresh.Result result = refresh.compute(metadataWithControlNode, false, context);

    // Then
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId2);
    assertThat(newNodes).doesNotContainKey(hostId1);
    // Should emit removed for the control node and added for the new node
    assertThat(result.events)
        .containsExactlyInAnyOrder(
            NodeStateEvent.added((DefaultNode) newNodes.get(hostId2)),
            NodeStateEvent.removed(controlNode));
  }
}
