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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The first node list refresh: creates new nodes from the discovered node list, reusing any node
 * already in metadata (e.g. the control node registered by the control connection) so that
 * connection state is preserved.
 */
@ThreadSafe
class InitialNodeListRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(InitialNodeListRefresh.class);

  @VisibleForTesting final Iterable<NodeInfo> nodeInfos;

  InitialNodeListRefresh(Iterable<NodeInfo> nodeInfos) {
    this.nodeInfos = nodeInfos;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {

    String logPrefix = context.getSessionName();
    TokenFactoryRegistry tokenFactoryRegistry = context.getTokenFactoryRegistry();

    TokenFactory tokenFactory = null;

    Map<UUID, DefaultNode> existingByHostId = new HashMap<>();
    for (Node n : oldMetadata.getNodes().values()) {
      existingByHostId.put(n.getHostId(), (DefaultNode) n);
    }

    Map<UUID, DefaultNode> newNodes = new HashMap<>();
    ImmutableList.Builder<Object> eventsBuilder = ImmutableList.builder();

    for (NodeInfo nodeInfo : nodeInfos) {
      UUID hostId = nodeInfo.getHostId();
      if (newNodes.containsKey(hostId)) {
        LOG.warn(
            "[{}] Found duplicate entries with host_id {} in system.peers, "
                + "keeping only the first one {}",
            logPrefix,
            hostId,
            newNodes.get(hostId));
      } else {
        DefaultNode node;
        DefaultNode existing = existingByHostId.get(hostId);
        if (existing != null) {
          node = existing;
          LOG.debug("[{}] Reusing existing node {}", logPrefix, node);
        } else {
          node = new DefaultNode(nodeInfo.getEndPoint(), context);
          LOG.debug("[{}] Adding new node {}", logPrefix, node);
          eventsBuilder.add(NodeStateEvent.added(node));
        }
        if (tokenMapEnabled && tokenFactory == null && nodeInfo.getPartitioner() != null) {
          tokenFactory = tokenFactoryRegistry.tokenFactoryFor(nodeInfo.getPartitioner());
        }
        copyInfos(nodeInfo, node, context);
        newNodes.put(hostId, node);
      }
    }

    for (Map.Entry<UUID, DefaultNode> entry : existingByHostId.entrySet()) {
      if (!newNodes.containsKey(entry.getKey())) {
        LOG.warn(
            "[{}] Pre-registered node {} was not found in the node list refresh",
            logPrefix,
            entry.getValue());
        eventsBuilder.add(NodeStateEvent.removed(entry.getValue()));
      }
    }

    return new Result(
        oldMetadata.withNodes(
            ImmutableMap.copyOf(newNodes), tokenMapEnabled, true, tokenFactory, context),
        eventsBuilder.build());
  }
}
