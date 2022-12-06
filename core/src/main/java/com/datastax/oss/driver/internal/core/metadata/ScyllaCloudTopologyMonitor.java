/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

/**
 * Topology monitor used for connecting to serverless Scylla clusters.
 *
 * <p>Extension of {@link DefaultTopologyMonitor} with a few tweaks. Still reliant on {@link
 * ControlConnection}.
 */
public class ScyllaCloudTopologyMonitor extends DefaultTopologyMonitor {

  private final InetSocketAddress cloudProxyAddress;
  private final String nodeDomain;

  public ScyllaCloudTopologyMonitor(
      InternalDriverContext context, InetSocketAddress cloudProxyAddress, String nodeDomain) {
    super(context);
    this.cloudProxyAddress = cloudProxyAddress;
    this.nodeDomain = nodeDomain;
  }

  @NonNull
  @Override
  protected EndPoint buildNodeEndPoint(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    UUID hostId = Objects.requireNonNull(row.getUuid("host_id"));
    return new SniEndPoint(cloudProxyAddress, hostId + "." + nodeDomain);
  }

  // Perform usual init with extra steps. After establishing connection we need to replace
  // endpoint that randomizes target node with concrete endpoint to the specific node.
  @Override
  public CompletionStage<Void> init() {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    return controlConnection
        .init(true, reconnectOnInit, true)
        .thenCompose(
            v -> {
              return query(
                      controlConnection.channel(),
                      "SELECT host_id FROM system.local",
                      Collections.emptyMap())
                  .toCompletableFuture();
            })
        .thenApply(
            adminResult -> {
              AdminRow localRow = adminResult.iterator().next();
              UUID hostId = localRow.getUuid("host_id");
              EndPoint newEndpoint = new SniEndPoint(cloudProxyAddress, hostId + "." + nodeDomain);
              // Replace initial contact point with specified endpoint, so that reconnections won't
              // choose different random node
              context.getMetadataManager().addContactPoints(ImmutableSet.of(newEndpoint));
              controlConnection.channel().setEndPoint(newEndpoint);
              return null;
            });
  }
}
