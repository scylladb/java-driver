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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.UUID;

public class ClientRoutesEndPoint implements EndPoint {
  private final UUID hostId;
  private final ClientRoutesTopologyMonitor topologyMonitor;
  private final String metricPrefix;
  @NonNull private final EndPoint fallbackEndPoint;

  /**
   * @param topologyMonitor the topology monitor used to resolve the endpoint address on demand.
   * @param hostId the host UUID identifying this node in the cluster.
   * @param broadcastInetAddress the node's broadcast address (from system.peers or system.local),
   *     used to build a stable metric prefix. May be {@code null} if the address could not be
   *     determined, in which case the hostId is used as the metric prefix instead.
   * @param fallbackEndPoint the default endpoint to fall back to when {@code
   *     topologyMonitor.resolve()} returns {@code null}, i.e. when this node is not accessed via a
   *     cloud private endpoint. Must not be {@code null}.
   */
  public ClientRoutesEndPoint(
      @NonNull ClientRoutesTopologyMonitor topologyMonitor,
      @NonNull UUID hostId,
      @Nullable InetAddress broadcastInetAddress,
      @NonNull EndPoint fallbackEndPoint) {
    this.topologyMonitor =
        Objects.requireNonNull(topologyMonitor, "Topology monitor cannot be null");
    this.hostId = Objects.requireNonNull(hostId, "HOST uuid cannot be null");
    this.fallbackEndPoint =
        Objects.requireNonNull(fallbackEndPoint, "Fallback endpoint cannot be null");
    this.metricPrefix = buildMetricPrefix(broadcastInetAddress, hostId);
  }

  @NonNull
  public UUID getHostId() {
    return hostId;
  }

  @NonNull
  @Override
  public SocketAddress resolve() {
    try {
      InetSocketAddress address = topologyMonitor.resolve(hostId);
      if (address != null) {
        return address;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("DNS resolution failed for host_id=" + hostId, e);
    }
    return fallbackEndPoint.resolve();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ClientRoutesEndPoint) {
      ClientRoutesEndPoint that = (ClientRoutesEndPoint) other;
      return this.hostId.equals(that.hostId);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostId);
  }

  @Override
  public String toString() {
    return "ClientRoutesEndPoint(" + hostId + ")";
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(@Nullable InetAddress address, @NonNull UUID hostId) {
    if (address == null) {
      return hostId.toString();
    }
    // getHostAddress() returns clean IP without leading slash:
    //   IPv4: "127.0.0.1"   IPv6: "0:0:0:0:0:0:0:1"
    // Replace dots for IPv4; colons are kept for IPv6 (consistent with DefaultEndPoint)
    return address.getHostAddress().replace('.', '_') + '_' + hostId;
  }
}
