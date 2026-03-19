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

import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import net.jcip.annotations.Immutable;

/**
 * Configuration for client routes, used in cloud private-endpoint deployments.
 *
 * <p>Client routes enable the driver to discover and connect to nodes through a load balancer (such
 * as AWS PrivateLink, Azure Private Link, or GCP Private Service Connect) by reading endpoint
 * mappings from the {@code system.client_routes} table. Each endpoint is identified by a connection
 * ID and maps to specific node addresses.
 *
 * <p>This configuration is mutually exclusive with a user-provided {@link
 * com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator}. If client routes are
 * configured, the driver will use its internal client routes handler for address translation.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ClientRoutesConfig config = ClientRoutesConfig.builder()
 *     .addEndpoint(new ClientRouteProxy(
 *         "12345678-1234-1234-1234-123456789012",
 *         "my-cluster-endpoint.example.com"))
 *     .build();
 *
 * CqlSession session = CqlSession.builder()
 *     .withClientRoutesConfig(config)
 *     .build();
 * }</pre>
 *
 * @see SessionBuilder#withClientRoutesConfig(ClientRoutesConfig)
 * @see ClientRouteProxy
 */
@Immutable
public final class ClientRoutesConfig {

  private static final String DEFAULT_TABLE_NAME = "system.client_routes";

  /**
   * Default native transport port used by Scylla/Cassandra nodes. This is used as the fallback port
   * for {@code broadcastRpcAddress} when the system tables do not include port information (e.g.
   * older Scylla versions without {@code system.peers_v2}). The {@code broadcastRpcAddress} is how
   * the driver matches TOPOLOGY_CHANGE events (node added/removed) to nodes in its metadata — it is
   * NOT used for opening connections. Connections go through {@code ClientRoutesEndPoint} which
   * resolves via the NLB proxy addresses in the routes cache.
   */
  public static final int DEFAULT_NATIVE_TRANSPORT_PORT = 9042;

  /**
   * Pattern for valid unquoted CQL table names: must start with a letter or underscore, followed by
   * letters, digits, or underscores. Optionally qualified with a keyspace prefix using the same
   * rules. Quoted identifiers (e.g. {@code "My-Table"}) are not supported.
   */
  private static final Pattern VALID_TABLE_NAME =
      Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)?");

  private final List<ClientRouteProxy> endpoints;
  private final String tableName;
  private final int nativeTransportPort;
  private final boolean shardAwarenessEnabled;

  private ClientRoutesConfig(
      List<ClientRouteProxy> endpoints,
      String tableName,
      int nativeTransportPort,
      boolean shardAwarenessEnabled) {
    if (endpoints == null || endpoints.isEmpty()) {
      throw new IllegalArgumentException("At least one endpoint must be specified");
    }
    Objects.requireNonNull(tableName, "tableName must not be null");
    if (!VALID_TABLE_NAME.matcher(tableName).matches()) {
      throw new IllegalArgumentException(
          "Invalid table name (must match [a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)?): "
              + tableName);
    }
    this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
    this.tableName = tableName;
    this.nativeTransportPort = nativeTransportPort;
    this.shardAwarenessEnabled = shardAwarenessEnabled;
  }

  /**
   * Returns the list of configured endpoints.
   *
   * @return an immutable list of endpoints.
   */
  @NonNull
  public List<ClientRouteProxy> getEndpoints() {
    return endpoints;
  }

  /**
   * Returns the name of the system table to query for client routes.
   *
   * @return the table name (defaults to {@code system.client_routes}).
   */
  @NonNull
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns the native transport port of the cluster nodes. This port is used as the fallback for
   * building {@code broadcastRpcAddress} when system tables lack port columns. The {@code
   * broadcastRpcAddress} is used only to match TOPOLOGY_CHANGE events to metadata nodes — it is NOT
   * used to open connections (those go through the NLB proxy endpoints from the routes cache).
   *
   * @return the native transport port (defaults to {@value #DEFAULT_NATIVE_TRANSPORT_PORT}).
   */
  public int getNativeTransportPort() {
    return nativeTransportPort;
  }

  /**
   * Returns whether shard awareness is enabled.
   *
   * <p>When {@code true}, the driver assumes the load balancer is configured to forward the
   * driver's original source port to ScyllaDB (e.g. via Proxy Protocol v2), enabling shard-aware
   * connection routing end-to-end through the load balancer. Requires {@code
   * advanced-shard-awareness.enabled} to be {@code true} (the default).
   *
   * @return {@code true} if shard awareness is enabled.
   */
  public boolean isShardAwarenessEnabled() {
    return shardAwarenessEnabled;
  }

  /**
   * Creates a new builder for constructing a {@link ClientRoutesConfig}.
   *
   * @return a new builder instance.
   */
  @NonNull
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientRoutesConfig)) {
      return false;
    }
    ClientRoutesConfig that = (ClientRoutesConfig) o;
    return endpoints.equals(that.endpoints)
        && tableName.equals(that.tableName)
        && nativeTransportPort == that.nativeTransportPort
        && shardAwarenessEnabled == that.shardAwarenessEnabled;
  }

  @Override
  public int hashCode() {
    return Objects.hash(endpoints, tableName, nativeTransportPort, shardAwarenessEnabled);
  }

  @Override
  public String toString() {
    return "ClientRoutesConfig{"
        + "endpoints="
        + endpoints
        + ", tableName='"
        + tableName
        + '\''
        + ", nativeTransportPort="
        + nativeTransportPort
        + ", shardAwareness="
        + shardAwarenessEnabled
        + '}';
  }

  /** Builder for {@link ClientRoutesConfig}. */
  public static final class Builder {
    private final List<ClientRouteProxy> endpoints = new ArrayList<>();
    private final Set<String> seenConnectionIds = new HashSet<>();
    private String tableName = DEFAULT_TABLE_NAME;
    private int nativeTransportPort = DEFAULT_NATIVE_TRANSPORT_PORT;
    private boolean shardAwareness = false;

    /**
     * Adds an endpoint to the configuration.
     *
     * @param endpoint the endpoint to add (must not be null).
     * @return this builder.
     * @throws IllegalArgumentException if an endpoint with the same connection ID has already been
     *     added.
     */
    @NonNull
    public Builder addEndpoint(@NonNull ClientRouteProxy endpoint) {
      Objects.requireNonNull(endpoint, "endpoint must not be null");
      if (!seenConnectionIds.add(endpoint.getConnectionId())) {
        throw new IllegalArgumentException(
            "Duplicate connection ID: " + endpoint.getConnectionId());
      }
      this.endpoints.add(endpoint);
      return this;
    }

    /**
     * Sets the endpoints for the configuration, replacing any previously added endpoints.
     *
     * @param endpoints the endpoints to set (must not be null or empty).
     * @return this builder.
     */
    @NonNull
    public Builder withEndpoints(@NonNull List<ClientRouteProxy> endpoints) {
      Objects.requireNonNull(endpoints, "endpoints must not be null");
      if (endpoints.isEmpty()) {
        throw new IllegalArgumentException("endpoints must not be empty");
      }
      this.endpoints.clear();
      this.seenConnectionIds.clear();
      for (ClientRouteProxy endpoint : endpoints) {
        addEndpoint(endpoint);
      }
      return this;
    }

    /**
     * Sets the name of the system table to query for client routes.
     *
     * <p>This is primarily useful for testing. If not set, the driver will use the default table
     * name from the configuration ({@code system.client_routes}).
     *
     * @param tableName the table name to use (must not be null).
     * @return this builder.
     * @throws NullPointerException if {@code tableName} is null.
     */
    @VisibleForTesting
    @NonNull
    Builder withTableName(@NonNull String tableName) {
      this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
      return this;
    }

    /**
     * Sets the native transport port of the cluster nodes. This is used as the fallback port for
     * {@code broadcastRpcAddress} when system tables do not include port information. Only needed
     * for clusters using a non-standard native transport port. Defaults to {@value
     * #DEFAULT_NATIVE_TRANSPORT_PORT}.
     *
     * @param port the native transport port.
     * @return this builder.
     */
    @NonNull
    public Builder withNativeTransportPort(int port) {
      this.nativeTransportPort = port;
      return this;
    }

    /**
     * Sets whether shard awareness is enabled.
     *
     * <p>When {@code true}, the driver assumes the load balancer is configured to forward the
     * driver's original source port to ScyllaDB (e.g. via Proxy Protocol v2), enabling shard-aware
     * connection routing end-to-end through the load balancer.
     *
     * <p>Requires {@code advanced-shard-awareness.enabled = true} (the default).
     *
     * @param shardAwareness {@code true} to enable shard awareness.
     * @return this builder.
     */
    @NonNull
    public Builder withShardAwareness(boolean shardAwareness) {
      this.shardAwareness = shardAwareness;
      return this;
    }

    /**
     * Builds the {@link ClientRoutesConfig} with the configured endpoints and table name.
     *
     * @return the new configuration instance.
     * @throws IllegalArgumentException if no endpoints have been added.
     */
    @NonNull
    public ClientRoutesConfig build() {
      return new ClientRoutesConfig(endpoints, tableName, nativeTransportPort, shardAwareness);
    }
  }
}
