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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * Configuration for client routes, used in PrivateLink-style deployments.
 *
 * <p>Client routes enable the driver to discover and connect to nodes through a load balancer (such
 * as AWS PrivateLink) by reading endpoint mappings from the {@code system.client_routes} table.
 * Each endpoint is identified by a connection ID and maps to specific node addresses.
 *
 * <p>This configuration is mutually exclusive with a user-provided {@link
 * com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator}. If client routes are
 * configured, the driver will use its internal client routes handler for address translation.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ClientRoutesConfig config = ClientRoutesConfig.builder()
 *     .addEndpoint(new ClientRoutesEndpoint(
 *         UUID.fromString("12345678-1234-1234-1234-123456789012"),
 *         "my-privatelink.us-east-1.aws.scylladb.com:9042"))
 *     .build();
 *
 * CqlSession session = CqlSession.builder()
 *     .withClientRoutesConfig(config)
 *     .build();
 * }</pre>
 *
 * @see SessionBuilder#withClientRoutesConfig(ClientRoutesConfig)
 * @see ClientRoutesEndpoint
 */
@Immutable
public class ClientRoutesConfig {

  private final List<ClientRoutesEndpoint> endpoints;
  private final String tableName;

  private ClientRoutesConfig(List<ClientRoutesEndpoint> endpoints, String tableName) {
    if (endpoints == null || endpoints.isEmpty()) {
      throw new IllegalArgumentException("At least one endpoint must be specified");
    }
    this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
    this.tableName = tableName;
  }

  /**
   * Returns the list of configured endpoints.
   *
   * @return an immutable list of endpoints.
   */
  @NonNull
  public List<ClientRoutesEndpoint> getEndpoints() {
    return endpoints;
  }

  /**
   * Returns the name of the system table to query for client routes.
   *
   * @return the table name, or null to use the default ({@code system.client_routes}).
   */
  @Nullable
  public String getTableName() {
    return tableName;
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
    return endpoints.equals(that.endpoints) && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(endpoints, tableName);
  }

  @Override
  public String toString() {
    return "ClientRoutesConfig{"
        + "endpoints="
        + endpoints
        + ", tableName='"
        + tableName
        + '\''
        + '}';
  }

  /** Builder for {@link ClientRoutesConfig}. */
  public static class Builder {
    private final List<ClientRoutesEndpoint> endpoints = new ArrayList<>();
    private String tableName;

    /**
     * Adds an endpoint to the configuration.
     *
     * @param endpoint the endpoint to add (must not be null).
     * @return this builder.
     */
    @NonNull
    public Builder addEndpoint(@NonNull ClientRoutesEndpoint endpoint) {
      this.endpoints.add(Objects.requireNonNull(endpoint, "endpoint must not be null"));
      return this;
    }

    /**
     * Sets the endpoints for the configuration, replacing any previously added endpoints.
     *
     * @param endpoints the endpoints to set (must not be null or empty).
     * @return this builder.
     */
    @NonNull
    public Builder withEndpoints(@NonNull List<ClientRoutesEndpoint> endpoints) {
      Objects.requireNonNull(endpoints, "endpoints must not be null");
      if (endpoints.isEmpty()) {
        throw new IllegalArgumentException("endpoints must not be empty");
      }
      this.endpoints.clear();
      for (ClientRoutesEndpoint endpoint : endpoints) {
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
     * @param tableName the table name to use.
     * @return this builder.
     */
    @NonNull
    public Builder withTableName(@Nullable String tableName) {
      this.tableName = tableName;
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
      return new ClientRoutesConfig(endpoints, tableName);
    }
  }
}
