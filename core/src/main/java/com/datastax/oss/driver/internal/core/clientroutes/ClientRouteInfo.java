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

/*
 * Copyright (C) 2025 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.clientroutes;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.UUID;
import net.jcip.annotations.Immutable;

/**
 * Represents a single row from the system.client_routes table.
 *
 * <p>Each row maps a connection_id + host_id pair to an address that must be DNS-resolved before
 * use.
 */
@Immutable
public class ClientRouteInfo {

  private final UUID connectionId;
  private final UUID hostId;
  private final String address;
  private final Integer nativeTransportPort;
  private final Integer nativeTransportPortSsl;

  public ClientRouteInfo(
      @NonNull UUID connectionId,
      @NonNull UUID hostId,
      @NonNull String address,
      @Nullable Integer nativeTransportPort,
      @Nullable Integer nativeTransportPortSsl) {
    this.connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
    this.hostId = Objects.requireNonNull(hostId, "hostId must not be null");
    this.address = Objects.requireNonNull(address, "address must not be null");
    this.nativeTransportPort = nativeTransportPort;
    this.nativeTransportPortSsl = nativeTransportPortSsl;
  }

  @NonNull
  public UUID getConnectionId() {
    return connectionId;
  }

  @NonNull
  public UUID getHostId() {
    return hostId;
  }

  @NonNull
  public String getAddress() {
    return address;
  }

  @Nullable
  public Integer getNativeTransportPort() {
    return nativeTransportPort;
  }

  @Nullable
  public Integer getNativeTransportPortSsl() {
    return nativeTransportPortSsl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientRouteInfo)) {
      return false;
    }
    ClientRouteInfo that = (ClientRouteInfo) o;
    return connectionId.equals(that.connectionId)
        && hostId.equals(that.hostId)
        && address.equals(that.address)
        && Objects.equals(nativeTransportPort, that.nativeTransportPort)
        && Objects.equals(nativeTransportPortSsl, that.nativeTransportPortSsl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectionId, hostId, address, nativeTransportPort, nativeTransportPortSsl);
  }

  @Override
  public String toString() {
    return "ClientRouteInfo{"
        + "connectionId="
        + connectionId
        + ", hostId="
        + hostId
        + ", address='"
        + address
        + '\''
        + ", nativeTransportPort="
        + nativeTransportPort
        + ", nativeTransportPortSsl="
        + nativeTransportPortSsl
        + '}';
  }
}
