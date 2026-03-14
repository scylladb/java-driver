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
package com.datastax.oss.driver.api.core.config;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.regex.Pattern;
import net.jcip.annotations.Immutable;

/**
 * Represents a client routes endpoint for cloud private-endpoint deployments (e.g. AWS PrivateLink,
 * Azure Private Link, GCP Private Service Connect).
 *
 * <p>Each endpoint corresponds to a connection ID in the {@code system.client_routes} table, with
 * an optional connection address (DNS name or IP) that overrides the {@code address} column from
 * the table for the matching connection ID.
 */
@Immutable
public final class ClientRouteProxy {

  /**
   * Pattern for valid hostnames and IP addresses: alphanumeric characters, dots, hyphens,
   * underscores, colons (for IPv6), and brackets (for bracketed IPv6 notation). Strings that don't
   * match will be rejected early rather than failing with a confusing DNS resolution error later.
   */
  private static final Pattern VALID_HOSTNAME_OR_IP = Pattern.compile("^[a-zA-Z0-9._:\\[\\]-]+$");

  private final String connectionId;
  private final String connectionAddr;

  /**
   * Creates a new endpoint with the given connection ID and no connection address.
   *
   * @param connectionId the connection ID (must not be null).
   */
  public ClientRouteProxy(@NonNull String connectionId) {
    this(connectionId, null);
  }

  /**
   * Creates a new endpoint with the given connection ID and connection address.
   *
   * @param connectionId the connection ID (must not be null).
   * @param connectionAddr the DNS name or IP address used to override the {@code address} column
   *     from the {@code system.client_routes} table for the matching {@code connection_id} (may be
   *     null). Must be a plain hostname or IP address without a port (e.g. {@code
   *     "my-cluster.example.com"} or {@code "10.0.1.5"}).
   */
  public ClientRouteProxy(@NonNull String connectionId, @Nullable String connectionAddr) {
    this.connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
    if (connectionId.trim().isEmpty()) {
      throw new IllegalArgumentException("connectionId must not be empty");
    }
    if (connectionAddr != null) {
      if (connectionAddr.trim().isEmpty()) {
        throw new IllegalArgumentException(
            "connectionAddr must not be empty or blank when non-null");
      }
      if (containsPort(connectionAddr)) {
        throw new IllegalArgumentException(
            "connectionAddr must be a plain hostname or IP address without a port, got: "
                + connectionAddr);
      }
      if (!VALID_HOSTNAME_OR_IP.matcher(connectionAddr).matches()) {
        throw new IllegalArgumentException(
            "connectionAddr contains invalid characters "
                + "(expected hostname or IP address with only alphanumeric characters, "
                + "dots, hyphens, underscores, colons, or brackets): "
                + connectionAddr);
      }
    }
    this.connectionAddr = connectionAddr;
  }

  /**
   * Returns {@code true} if the address string appears to contain a port suffix. Handles plain
   * {@code host:port} and IPv6 bracketed {@code [host]:port} notation.
   */
  private static boolean containsPort(@NonNull String addr) {
    if (addr.startsWith("[")) {
      // IPv6 bracketed notation: [host]:port
      int closeBracket = addr.indexOf(']');
      return closeBracket > 0
          && closeBracket + 1 < addr.length()
          && addr.charAt(closeBracket + 1) == ':';
    }
    int colonIdx = addr.lastIndexOf(':');
    if (colonIdx < 0) {
      return false;
    }
    // Leading colon (e.g. ":9042") — treat as port-only, no valid hostname
    if (colonIdx == 0) {
      return true;
    }
    // Multiple colons means bare IPv6 address (e.g. "::1"), not host:port
    return addr.indexOf(':') == colonIdx;
  }

  /** Returns the connection ID for this endpoint. */
  @NonNull
  public String getConnectionId() {
    return connectionId;
  }

  /**
   * Returns the connection address for this endpoint, or null if not specified.
   *
   * <p>This is a plain hostname or IP address (e.g. {@code "my-cluster.example.com"} or {@code
   * "10.0.1.5"}). When provided, the {@code address} column from the {@code system.client_routes}
   * table is overridden with this value for the matching {@code connection_id}.
   */
  @Nullable
  public String getConnectionAddr() {
    return connectionAddr;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientRouteProxy)) {
      return false;
    }
    ClientRouteProxy that = (ClientRouteProxy) o;
    return connectionId.equals(that.connectionId)
        && Objects.equals(connectionAddr, that.connectionAddr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionId, connectionAddr);
  }

  @Override
  public String toString() {
    return "ClientRouteProxy{"
        + "connectionId='"
        + connectionId
        + "'"
        + (connectionAddr != null ? ", connectionAddr='" + connectionAddr + "'" : "")
        + "}";
  }
}
