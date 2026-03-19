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

  private EndPoint endPoint1;
  private EndPoint endPoint2;
  private EndPoint endPoint3;
  private UUID hostId1;
  private UUID hostId2;
  private UUID hostId3;

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
  }

  @Test
  public void should_create_nodes_from_node_infos() {
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
    assertThat(newNodes.get(hostId1).getHostId()).isEqualTo(hostId1);
    assertThat(newNodes.get(hostId2).getEndPoint()).isEqualTo(endPoint2);
    assertThat(newNodes.get(hostId3).getEndPoint()).isEqualTo(endPoint3);
    assertThat(result.events).hasSize(3);
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

    // Then only the first nodeInfo should have been used
    Map<UUID, Node> newNodes = result.newMetadata.getNodes();
    assertThat(newNodes).containsOnlyKeys(hostId1);
    assertThat(newNodes.get(hostId1).getDatacenter()).isEqualTo("dc1");
  }
}
