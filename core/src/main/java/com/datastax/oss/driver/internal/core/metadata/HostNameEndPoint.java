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
import com.datastax.oss.driver.shaded.guava.common.primitives.UnsignedBytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link EndPoint} implementation for hostname-based contact points that re-resolves DNS on
 * every {@link #resolve()} call. This allows the driver to automatically pick up a new IP address
 * after a node replacement updates the DNS entry, without requiring a driver restart.
 *
 * <p>Contrast with {@link DefaultEndPoint}, which caches the resolved IP at construction time and
 * never re-queries DNS regardless of how long the driver has been running.
 *
 * <p>Note that the JVM maintains its own DNS cache on top of the OS resolver. By default, when no
 * security manager is installed (the common case in containerized deployments), successful lookups
 * are cached for 30 seconds ({@code networkaddress.cache.ttl} security property). The effective
 * recovery time after a DNS change is {@code max(networkaddress.cache.ttl, reconnection backoff)},
 * so lowering the TTL below the reconnection base delay yields no benefit. Setting it to {@code 0}
 * disables the JVM DNS cache entirely, causing a live DNS query on every connection attempt.
 */
public class HostNameEndPoint implements EndPoint {

  private static final AtomicLong OFFSET = new AtomicLong();

  protected final String hostName;
  protected final int port;

  public HostNameEndPoint(String hostName, int port) {
    this.hostName =
        Objects.requireNonNull(hostName, "hostName can't be null").toLowerCase(Locale.ROOT);
    this.port = port;
  }

  @NonNull
  @Override
  public InetSocketAddress resolve() {
    try {
      InetAddress[] addresses = InetAddress.getAllByName(hostName);
      if (addresses.length == 0) {
        // Probably never happens, but the JDK docs don't explicitly say so
        throw new IllegalArgumentException("Could not resolve contact point hostname " + hostName);
      }
      // Sort by IP for a true round-robin (order of getAllByName results is unspecified)
      Arrays.sort(addresses, IP_COMPARATOR);
      int index =
          (addresses.length == 1)
              ? 0
              : (int) Math.floorMod(OFFSET.getAndIncrement(), (long) addresses.length);
      return new InetSocketAddress(addresses[index], port);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Could not resolve contact point hostname " + hostName, e);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof HostNameEndPoint) {
      HostNameEndPoint that = (HostNameEndPoint) other;
      return this.hostName.equals(that.hostName) && this.port == that.port;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostName, port);
  }

  @Override
  public String toString() {
    return hostName + ":" + port;
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return hostName.replace('.', '_') + ':' + port;
  }

  @SuppressWarnings("UnnecessaryLambda")
  private static final Comparator<InetAddress> IP_COMPARATOR =
      (InetAddress address1, InetAddress address2) ->
          UnsignedBytes.lexicographicalComparator()
              .compare(address1.getAddress(), address2.getAddress());
}
