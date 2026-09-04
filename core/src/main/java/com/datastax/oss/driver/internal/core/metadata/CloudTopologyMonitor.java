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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

public class CloudTopologyMonitor extends DefaultTopologyMonitor {

  private final InetSocketAddress cloudProxyAddress;

  public CloudTopologyMonitor(InternalDriverContext context, InetSocketAddress cloudProxyAddress) {
    super(context);
    // Snapshot the proxy's host string once, here, instead of letting every buildNodeEndPoint()
    // re-derive it. SniEndPoint stores the proxy address unresolved, and for a *resolved* input it
    // does that by reading getHostString() -- which, when the InetSocketAddress was built from an
    // InetAddress rather than from a name, renders that address's mutable, lazily-populated
    // hostName field. Anything that calls getHostName() on the instance fills it in, and
    // DefaultSslEngineFactory does exactly that under the default allow-dns-reverse-lookup-san, so
    // every SniEndPoint built afterwards would get a different equals/hashCode/asMetricPrefix from
    // the ones built before. Since this monitor rebuilds every node's endpoint on every topology
    // refresh, that moves all of the cluster's per-node metrics at once, mid-session.
    //
    // Only that spelling drifts, and it is worth being precise about which, so nobody reads this
    // guard as redundant and deletes it. new InetSocketAddress("proxy.example.com", 9042) is safe:
    // getByName populates the InetAddress's hostName eagerly, so getHostString() answers the same
    // string before and after any getHostName() call. new InetSocketAddress(
    // InetAddress.getByAddress(bytes), 9042) is the one that moves, from the IP literal to whatever
    // the reverse lookup finds. The bundle path never produces either -- CloudConfigFactory
    // #getSniProxyAddress already returns createUnresolved(), which the branch below passes
    // through untouched -- so what this protects is the programmatic
    // SessionBuilder#withCloudProxyAddress, where the caller chooses the constructor.
    this.cloudProxyAddress =
        cloudProxyAddress.isUnresolved()
            ? cloudProxyAddress
            : InetSocketAddress.createUnresolved(
                cloudProxyAddress.getHostString(), cloudProxyAddress.getPort());
  }

  @NonNull
  @Override
  protected EndPoint buildNodeEndPoint(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    UUID hostId = Objects.requireNonNull(row.getUuid("host_id"));
    return new SniEndPoint(cloudProxyAddress, hostId.toString());
  }

  @Override
  public boolean reresolvesNodeAddresses() {
    // Every node is reached through the cloud SNI proxy, and SniEndPoint hands the proxy hostname
    // over unresolved, so the connection layer re-expands it on every connection attempt (see
    // ChannelFactory#resolveCandidates). Addresses therefore stay current on their own: appending
    // the original contact points as a DNS re-resolution fallback would add nothing, and could
    // resurrect nodes this monitor has authoritatively removed.
    return true;
  }
}
