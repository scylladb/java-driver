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
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UnresolvedEndPoint implements EndPoint, Serializable {
  private final String metricPrefix;
  String host;
  int port;

  private final List<EndPoint> EMPTY = new ArrayList<>();

  public UnresolvedEndPoint(String host, int port) {
    this.host = host;
    this.port = port;
    this.metricPrefix = buildMetricPrefix(host, port);
  }

  @NonNull
  @Override
  public SocketAddress resolve() {
    throw new RuntimeException(
        String.format(
            "This endpoint %s should never been resolved, but it happened, it somehow leaked to downstream code.",
            this));
  }

  @NonNull
  @Override
  public List<EndPoint> resolveAll() {
    try {
      InetAddress[] inetAddresses = InetAddress.getAllByName(host);
      Set<EndPoint> result = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        result.add(new DefaultEndPoint(new InetSocketAddress(inetAddress, port)));
      }
      return new ArrayList<>(result);
    } catch (UnknownHostException e) {
      return EMPTY;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other instanceof UnresolvedEndPoint) {
      UnresolvedEndPoint that = (UnresolvedEndPoint) other;
      return this.host.equals(that.host) && this.port == that.port;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return host.toLowerCase().hashCode() + port;
  }

  @Override
  public String toString() {
    return host + ":" + port;
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(String host, int port) {
    // Append the port since Cassandra 4 supports nodes with different ports
    return host.replace('.', '_') + ':' + port;
  }
}
