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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link DriverConfigReporter}: serializes the driver configuration to the cross-driver
 * {@code DRIVER_CONFIG} JSON shape and adds it to the control connection's {@code STARTUP} options.
 *
 * <p>The blob is (re)built on demand every time the control connection initializes, so it always
 * reflects the current (possibly reloaded) configuration without any caching.
 */
@ThreadSafe
public class DefaultDriverConfigReporter implements DriverConfigReporter {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDriverConfigReporter.class);

  /** STARTUP option key under which the config JSON is sent. */
  public static final String DRIVER_CONFIG_KEY = "DRIVER_CONFIG";

  /** STARTUP option key under which the per-session identifier is sent. */
  public static final String SESSION_ID_KEY = "SESSION_ID";

  /**
   * Major schema version. Adding keys is backward-compatible and does not bump this; only
   * changing/removing the meaning of an existing key does.
   */
  static final int SCHEMA_VERSION = 1;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final InternalDriverContext context;

  // Dedicated, driver-generated identifier for this session. Not derived from the (user-settable,
  // Insights-oriented) CLIENT_ID, so that it is guaranteed unique per session as the grouping key
  // requires. The reporter is a per-session singleton (built once via LazyReference), so this value
  // is stable and shared across all of the session's connections.
  private final UUID sessionId = Uuids.random();

  public DefaultDriverConfigReporter(InternalDriverContext context) {
    this.context = context;
  }

  @Override
  public void populateStartupOptions(
      Map<String, String> startupOptions, boolean reportDriverConfig) {
    // Configuration reporting is a best-effort diagnostic aid: it runs on the connection
    // initialization path, so any failure here (a bad config read, a misbehaving policy while
    // introspecting, a serialization error) must be swallowed rather than allowed to break the
    // connection — which would prevent the session from establishing or reconnecting.
    try {
      if (!isEnabled()) {
        return;
      }
      // SESSION_ID on every connection so the server can group a session's connections.
      startupOptions.put(SESSION_ID_KEY, sessionId.toString());
      // DRIVER_CONFIG blob only on the control connection.
      if (reportDriverConfig) {
        String json = buildJson();
        if (json != null) {
          startupOptions.put(DRIVER_CONFIG_KEY, json);
        }
      }
    } catch (RuntimeException e) {
      LOG.warn(
          "Error while building the driver configuration report; skipping driver config reporting",
          e);
    }
  }

  private boolean isEnabled() {
    return context
        .getConfig()
        .getDefaultProfile()
        .getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, false);
  }

  /**
   * Builds the compact, single-line JSON configuration report.
   *
   * <p>Stage 1 emits only the schema {@code version}; the individual configuration groups are
   * populated in {@link #populateConfig(ObjectNode, DriverExecutionProfile)} in a later stage.
   */
  protected String buildJson() {
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    root.put("version", SCHEMA_VERSION);
    populateConfig(root, context.getConfig().getDefaultProfile());
    try {
      return OBJECT_MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      // An in-memory node tree should never fail to serialize; never let it break connection setup.
      LOG.warn("Failed to serialize driver configuration report; skipping DRIVER_CONFIG", e);
      return null;
    }
  }

  /**
   * Populates the configuration groups onto the report root. Placeholder in Stage 1; Stage 2 fills
   * in {@code connection}, {@code socket}, the policy groups, {@code query_defaults}, {@code tls},
   * etc.
   */
  protected void populateConfig(ObjectNode root, DriverExecutionProfile config) {
    // Stage 2: populate configuration groups from `config` and the context's policies.
  }
}
