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
import java.util.List;
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
 *   <li>{@link #connectionIds} — UUIDs of the affected connections
 *   <li>{@link #hostIds} — UUIDs of the affected hosts
 * </ul>
 */
@Immutable
public class ClientRoutesChangeEvent {

  public final String changeType;
  public final List<String> connectionIds;
  public final List<String> hostIds;

  public ClientRoutesChangeEvent(
      @NonNull String changeType,
      @NonNull List<String> connectionIds,
      @NonNull List<String> hostIds) {
    this.changeType = changeType;
    this.connectionIds = connectionIds;
    this.hostIds = hostIds;
  }

  @Override
  public String toString() {
    return "ClientRoutesChangeEvent("
        + "changeType='"
        + changeType
        + "', connectionIds="
        + connectionIds
        + ", hostIds="
        + hostIds
        + ")";
  }
}
