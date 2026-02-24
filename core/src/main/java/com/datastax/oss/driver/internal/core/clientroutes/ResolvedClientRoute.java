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
  private final String hostname;
  private final Integer nativeTransportPort;
  private final Integer nativeTransportPortSsl;

  public ResolvedClientRoute(
      @NonNull UUID hostId,
      @NonNull String hostname,
      @Nullable Integer nativeTransportPort,
      @Nullable Integer nativeTransportPortSsl) {
    this.hostId = Objects.requireNonNull(hostId, "hostId must not be null");
    this.hostname = Objects.requireNonNull(hostname, "hostname must not be null");
    this.nativeTransportPort = nativeTransportPort;
    this.nativeTransportPortSsl = nativeTransportPortSsl;
  }

  @NonNull
  public UUID getHostId() {
    return hostId;
  }

  @NonNull
  public String getHostname() {
    return hostname;
  }

  @Nullable
  public Integer getNativeTransportPort() {
    return nativeTransportPort;
  }

  @Nullable
  public Integer getNativeTransportPortSsl() {
    return nativeTransportPortSsl;
  }

  /**
   * Converts this route to an InetSocketAddress, resolving DNS through the provided resolver.
   *
   * <p>The DNS resolver handles caching, so this method can be called on every connection attempt
   * without causing a DNS storm. DNS resolution happens at connection time, not at route discovery
   * time, which ensures the driver uses fresh DNS entries even when system.client_routes updates
   * happen between metadata refreshes.
   *
   * @param useSsl whether to use the SSL port
   * @param dnsResolver the DNS resolver to use for hostname resolution
   * @return an InetSocketAddress with the resolved IP and selected port
   * @throws IllegalStateException if no port is configured for this route
   * @throws java.net.UnknownHostException if the hostname cannot be resolved
   */
  @NonNull
  public InetSocketAddress toSocketAddress(boolean useSsl, @NonNull DnsResolver dnsResolver)
      throws java.net.UnknownHostException {
    Objects.requireNonNull(dnsResolver, "dnsResolver must not be null");

    // Select port based on SSL configuration
    Integer port;
    if (useSsl) {
      if (nativeTransportPortSsl != null) {
        port = nativeTransportPortSsl;
      } else {
        // SSL requested but not configured for this route - fall back to non-SSL port
        LOG.warn(
            "SSL requested for host_id={} ({}:{}) but tls_port is not configured in client routes. "
                + "Falling back to non-SSL port {}. This may indicate a configuration issue.",
            hostId,
            hostname,
            nativeTransportPort,
            nativeTransportPort);
        port = nativeTransportPort;
      }
    } else {
      port = nativeTransportPort;
    }

    // Validate port is configured
    if (port == null) {
      throw new IllegalStateException(
          String.format(
              "No port configured for host_id=%s, hostname=%s. "
                  + "The system.client_routes table may be incomplete.",
              hostId, hostname));
    }

    // Resolve DNS at connection time (resolver handles caching)
    InetAddress resolvedAddress = dnsResolver.resolve(hostname);

    return new InetSocketAddress(resolvedAddress, port);
  }

  @Override
  public String toString() {
    return "ResolvedClientRoute{"
        + "hostId="
        + hostId
        + ", hostname='"
        + hostname
        + '\''
        + ", nativeTransportPort="
        + nativeTransportPort
        + ", nativeTransportPortSsl="
        + nativeTransportPortSsl
        + '}';
  }
}
