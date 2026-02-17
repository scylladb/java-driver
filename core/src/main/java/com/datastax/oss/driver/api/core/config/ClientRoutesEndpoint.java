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
import java.util.UUID;
import net.jcip.annotations.Immutable;

/**
 * Represents a client routes endpoint for PrivateLink-style deployments.
 *
 * <p>Each endpoint corresponds to a connection ID in the {@code system.client_routes} table, with
 * an optional connection address that can be used as a seed host for initial connection.
 */
@Immutable
public class ClientRoutesEndpoint {

  private final UUID connectionId;
  private final String connectionAddr;

  /**
   * Creates a new endpoint with the given connection ID and no connection address.
   *
   * @param connectionId the connection ID (must not be null).
   */
  public ClientRoutesEndpoint(@NonNull UUID connectionId) {
    this(connectionId, null);
  }

  /**
   * Creates a new endpoint with the given connection ID and connection address.
   *
   * @param connectionId the connection ID (must not be null).
   * @param connectionAddr the connection address to use as a seed host (may be null).
   */
  public ClientRoutesEndpoint(@NonNull UUID connectionId, @Nullable String connectionAddr) {
    this.connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
    this.connectionAddr = connectionAddr;
  }

  /** Returns the connection ID for this endpoint. */
  @NonNull
  public UUID getConnectionId() {
    return connectionId;
  }

  /**
   * Returns the connection address for this endpoint, or null if not specified.
   *
   * <p>When provided and no explicit contact points are given to the session builder, this address
   * will be used as a seed host for the initial connection.
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
    if (!(o instanceof ClientRoutesEndpoint)) {
      return false;
    }
    ClientRoutesEndpoint that = (ClientRoutesEndpoint) o;
    return connectionId.equals(that.connectionId)
        && Objects.equals(connectionAddr, that.connectionAddr);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionId, connectionAddr);
  }

  @Override
  public String toString() {
    return "ClientRoutesEndpoint{"
        + "connectionId="
        + connectionId
        + ", connectionAddr='"
        + connectionAddr
        + '\''
        + '}';
  }
}
