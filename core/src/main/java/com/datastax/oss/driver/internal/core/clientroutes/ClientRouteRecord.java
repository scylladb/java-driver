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
import java.util.Objects;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class ClientRouteRecord {

  private final UUID hostId;
  private final String hostname;
  private final int port;

  public ClientRouteRecord(@NonNull UUID hostId, @NonNull String hostname, int port) {
    this.hostId = Objects.requireNonNull(hostId, "hostId must not be null");
    this.hostname = Objects.requireNonNull(hostname, "hostname must not be null");
    if (hostname.isEmpty()) {
      throw new IllegalArgumentException("hostname must not be empty");
    }
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("port must be between 1 and 65535, got: " + port);
    }
    this.port = port;
  }

  @NonNull
  public UUID getHostId() {
    return hostId;
  }

  @NonNull
  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ClientRouteRecord)) {
      return false;
    }
    ClientRouteRecord that = (ClientRouteRecord) o;
    return port == that.port && hostId.equals(that.hostId) && hostname.equals(that.hostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostId, hostname, port);
  }

  @Override
  public String toString() {
    return "ClientRouteRecord{"
        + "hostId="
        + hostId
        + ", hostname='"
        + hostname
        + '\''
        + ", port="
        + port
        + '}';
  }
}
