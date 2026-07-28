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
package com.datastax.oss.driver.internal.core.context;

import java.util.Map;

/**
 * Adds the client-configuration-reporting entries to a connection's CQL {@code STARTUP} options, so
 * ScyllaDB can store them in {@code system.clients.client_options} and operators can inspect a
 * client's effective driver settings while investigating incidents.
 *
 * <p>Two entries are produced, both governed by {@code advanced.driver-config-reporting.enabled}:
 *
 * <ul>
 *   <li>{@code SESSION_ID} — a unique-per-session identifier, added on <em>every</em> connection so
 *       the server can group all of a session's connections;
 *   <li>{@code DRIVER_CONFIG} — the full configuration JSON blob, added only on the control
 *       connection (pooled connections are correlated back to it via {@code SESSION_ID}).
 * </ul>
 */
public interface DriverConfigReporter {

  /**
   * Adds the reporting entries to the given startup options: {@code SESSION_ID} on every
   * connection, plus {@code DRIVER_CONFIG} when {@code reportDriverConfig} is true (the control
   * connection). Does nothing when configuration reporting is disabled.
   *
   * <p>Called from the protocol-initialization handler for every connection.
   *
   * <p><b>Implementations must not throw:</b> this runs on the connection initialization path, so a
   * failure to build the report must be swallowed (and logged) rather than propagated, otherwise it
   * would prevent the session from establishing or reconnecting.
   *
   * @param reportDriverConfig whether this connection should also carry the full {@code
   *     DRIVER_CONFIG} blob; true only for the control connection.
   */
  void populateStartupOptions(Map<String, String> startupOptions, boolean reportDriverConfig);
}
