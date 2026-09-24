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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * Fired on the internal event bus when a {@code CLIENT_ROUTES_CHANGE} protocol event is received on
 * the control connection. The {@link ClientRoutesTopologyMonitor} listens for this event and
 * triggers a refresh of the client routes cache.
 *
 * <p>Carries the data from the protocol event:
 *
 * <ul>
 *   <li>{@link #changeType} — the type of change (e.g. {@code "UPDATED"})
 *   <li>{@link #connectionIds} — opaque string identifiers of the affected connections
 *   <li>{@link #hostIds} — UUIDs of the affected hosts
 * </ul>
 */
@Immutable
public class ClientRoutesUpdateEvent {

  private final String changeType;
  private final List<String> connectionIds;
  private final List<String> hostIds;

  public ClientRoutesUpdateEvent(
      @NonNull String changeType,
      @NonNull List<String> connectionIds,
      @NonNull List<String> hostIds) {
    this.changeType = Objects.requireNonNull(changeType, "changeType must not be null");
    this.connectionIds =
        Collections.unmodifiableList(
            new ArrayList<>(
                Objects.requireNonNull(connectionIds, "connectionIds must not be null")));
    this.hostIds =
        Collections.unmodifiableList(
            new ArrayList<>(Objects.requireNonNull(hostIds, "hostIds must not be null")));
  }

  /** Returns the type of change (e.g. {@code "UPDATED"}). */
  @NonNull
  public String getChangeType() {
    return changeType;
  }

  /** Returns the connection IDs affected by this change. */
  @NonNull
  public List<String> getConnectionIds() {
    return connectionIds;
  }

  /** Returns the host IDs affected by this change. */
  @NonNull
  public List<String> getHostIds() {
    return hostIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ClientRoutesUpdateEvent)) return false;
    ClientRoutesUpdateEvent that = (ClientRoutesUpdateEvent) o;
    return changeType.equals(that.changeType)
        && connectionIds.equals(that.connectionIds)
        && hostIds.equals(that.hostIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(changeType, connectionIds, hostIds);
  }

  @Override
  public String toString() {
    return "ClientRoutesUpdateEvent("
        + "changeType='"
        + changeType
        + "', connectionIds="
        + connectionIds
        + ", hostIds="
        + hostIds
        + ")";
  }
}
