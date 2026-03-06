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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;

public class ClientRoutesEndPoint implements EndPoint {

  private final UUID hostID;
  private final ClientRoutesTopologyMonitor topologyMonitor;
  private final String metricPrefix;

  /**
   * @param topologyMonitor the topology monitor used to resolve the endpoint address on demand.
   * @param hostID the host UUID identifying this node in the cluster.
   * @param broadcastInetAddress the node's broadcast address (from system.peers or system.local),
   *     used to build a stable metric prefix. May be {@code null} if the address could not be
   *     determined, in which case the hostID is used as the metric prefix instead.
   */
  public ClientRoutesEndPoint(
      @NonNull ClientRoutesTopologyMonitor topologyMonitor,
      @NonNull UUID hostID,
      @Nullable InetAddress broadcastInetAddress) {
    this.topologyMonitor =
        Objects.requireNonNull(topologyMonitor, "Topology monitor cannot be null");
    this.hostID = Objects.requireNonNull(hostID, "HOST uuid cannot be null");
    this.metricPrefix = buildMetricPrefix(broadcastInetAddress, hostID);
  }

  @NonNull
  public UUID getHostID() {
    return hostID;
  }

  @NonNull
  @Override
  public InetSocketAddress resolve() {
    try {
      return topologyMonitor.resolve(hostID);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ClientRoutesEndPoint) {
      ClientRoutesEndPoint that = (ClientRoutesEndPoint) other;
      return this.hostID.equals(that.hostID);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostID);
  }

  @Override
  public String toString() {
    // Note that this uses the original proxy address, so if there are multiple A-records it won't
    // show which one was selected. If that turns out to be a problem for debugging, we might need
    // to store the result of resolve() in Connection and log that instead of the endpoint.
    return hostID.toString();
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(@Nullable InetAddress address, @NonNull UUID hostID) {
    if (address == null) {
      return hostID.toString();
    }
    // getHostAddress() returns clean IP without leading slash:
    //   IPv4: "127.0.0.1"   IPv6: "0:0:0:0:0:0:0:1"
    // Replace dots for IPv4; colons are kept for IPv6 (consistent with DefaultEndPoint)
    return address.getHostAddress().replace('.', '_') + '_' + hostID;
  }
}
