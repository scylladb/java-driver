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
package com.datastax.oss.driver.internal.core.clientroutes;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.UUID;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ResolvedClientRoute {
  private static final Logger LOG = LoggerFactory.getLogger(ResolvedClientRoute.class);

  private final UUID hostId;
  private final InetAddress resolvedAddress;
  private final int nativeTransportPort;
  private final Integer nativeTransportPortSsl;
  private final long resolvedAtNanos;

  public ResolvedClientRoute(
      @NonNull UUID hostId,
      @NonNull InetAddress resolvedAddress,
      int nativeTransportPort,
      @Nullable Integer nativeTransportPortSsl,
      long resolvedAtNanos) {
    this.hostId = Objects.requireNonNull(hostId);
    this.resolvedAddress = Objects.requireNonNull(resolvedAddress);
    this.nativeTransportPort = nativeTransportPort;
    this.nativeTransportPortSsl = nativeTransportPortSsl;
    this.resolvedAtNanos = resolvedAtNanos;
  }

  @NonNull
  public UUID getHostId() {
    return hostId;
  }

  @NonNull
  public InetAddress getResolvedAddress() {
    return resolvedAddress;
  }

  public int getNativeTransportPort() {
    return nativeTransportPort;
  }

  @Nullable
  public Integer getNativeTransportPortSsl() {
    return nativeTransportPortSsl;
  }

  public long getResolvedAtNanos() {
    return resolvedAtNanos;
  }

  @NonNull
  public InetSocketAddress toSocketAddress(boolean useSsl) {
    int port;
    if (useSsl) {
      if (nativeTransportPortSsl != null) {
        port = nativeTransportPortSsl;
      } else {
        // SSL requested but not configured for this route - fall back to non-SSL port
        LOG.warn(
            "SSL requested for host_id={} ({}:{}) but tls_port is not configured in client routes. "
                + "Falling back to non-SSL port {}. This may indicate a configuration issue.",
            hostId,
            resolvedAddress.getHostAddress(),
            nativeTransportPort,
            nativeTransportPort);
        port = nativeTransportPort;
      }
    } else {
      port = nativeTransportPort;
    }
    return new InetSocketAddress(resolvedAddress, port);
  }
}
