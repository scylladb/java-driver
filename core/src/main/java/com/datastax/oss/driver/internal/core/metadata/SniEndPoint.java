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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.util.Objects;

public class SniEndPoint extends HostNameEndPoint {

  private final String serverName;

  /**
   * @param proxyAddress the address of the proxy. Each call to {@link #resolve()} will re-resolve
   *     its hostname, fetch all of its A-records, and if there are more than 1 pick one in a
   *     round-robin fashion.
   * @param serverName the SNI server name. In the context of Cloud, this is the string
   *     representation of the host id.
   */
  public SniEndPoint(InetSocketAddress proxyAddress, String serverName) {
    super(
        Objects.requireNonNull(proxyAddress, "SNI address cannot be null").getHostName(),
        proxyAddress.getPort());
    this.serverName = Objects.requireNonNull(serverName, "SNI Server name cannot be null");
  }

  public String getServerName() {
    return serverName;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof SniEndPoint) {
      SniEndPoint that = (SniEndPoint) other;
      return this.hostName.equals(that.hostName)
          && this.port == that.port
          && this.serverName.equals(that.serverName);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostName, port, serverName);
  }

  @Override
  public String toString() {
    // Note that this uses the hostname rather than a specific resolved IP, so if there are multiple
    // A-records it won't show which one was selected.
    return hostName + ":" + port + ":" + serverName;
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return hostName.replace('.', '_') + ':' + port + '_' + serverName;
  }
}
