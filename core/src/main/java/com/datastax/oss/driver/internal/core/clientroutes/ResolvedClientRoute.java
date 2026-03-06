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
import java.util.Objects;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class ResolvedClientRoute {

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
