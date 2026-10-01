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

import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.Channel;
import java.util.Map;

/**
 * Adds the {@code DRIVER_CONFIG} entry to the control connection's CQL {@code STARTUP} options, so
 * ScyllaDB can store it in {@code system.clients.client_options} and operators can inspect the
 * driver's effective settings while investigating incidents.
 *
 * <p>The blob describes the whole session, so only the control connection carries it — pooled
 * connections are correlated back to it through the {@link StartupOptionsBuilder#SESSION_ID_KEY
 * SESSION_ID} startup option, which the driver sends on every connection unconditionally and
 * independently of this reporter.
 *
 * <p>Governed by {@code advanced.driver-config-reporting.enabled} (enabled by default).
 */
public interface DriverConfigReporter {

  /**
   * Adds the {@code DRIVER_CONFIG} blob to the given startup options, unless configuration
   * reporting is disabled.
   *
   * <p>Called from the protocol-initialization handler for the control connection only.
   *
   * <p><b>Implementations must not throw:</b> this runs on the connection initialization path, so a
   * failure to build the report must be swallowed (and logged) rather than propagated, otherwise it
   * would prevent the session from establishing or reconnecting.
   *
   * <p>The report describes the driver's own configuration and the effective SSL state of the
   * control connection. It does not depend on which backend answered, but the SSL handler must
   * already be installed on {@code channel}.
   *
   * @param startupOptions startup options to add the report to
   * @param channel control connection whose effective SSL state is reported
   */
  void populateControlConnectionOptions(
      @NonNull Map<String, String> startupOptions, @NonNull Channel channel);
}
