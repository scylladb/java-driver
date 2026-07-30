/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link DriverConfigReporter}: serializes the driver configuration to the cross-driver
 * {@code DRIVER_CONFIG} JSON shape, which {@link Connection.Factory} then sends in the control
 * connection's {@code STARTUP} options.
 *
 * <p>The report is built once, when the {@link Cluster} initializes, and the resulting string is
 * reused for the lifetime of that {@code Cluster} — it is never rebuilt while the session is in
 * flight, so a control-connection reconnect costs nothing and always reports the same
 * configuration.
 */
public class DefaultDriverConfigReporter implements DriverConfigReporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDriverConfigReporter.class);

  /** STARTUP option key under which the config JSON is sent. */
  public static final String DRIVER_CONFIG_KEY = "DRIVER_CONFIG";

  /**
   * Major schema version. Adding keys is backward-compatible and does not bump this; only
   * changing/removing the meaning of an existing key does.
   */
  static final int SCHEMA_VERSION = 1;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected final Configuration configuration;

  public DefaultDriverConfigReporter(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String buildReport() {
    // Configuration reporting is a best-effort diagnostic aid, so any failure here (a bad config
    // read, a misbehaving policy while introspecting, a serialization error) must be swallowed
    // rather than allowed to propagate: it is built on the Cluster-initialization path, which must
    // not fail because of a diagnostic.
    try {
      return buildJson();
    } catch (RuntimeException e) {
      LOGGER.warn(
          "Error while building the driver configuration report; skipping driver config reporting",
          e);
      return null;
    }
  }

  /**
   * Builds the compact, single-line JSON configuration report.
   *
   * <p>Stage 1 emits only the schema {@code version}; the individual configuration groups are
   * populated in {@link #populateConfig(ObjectNode)} in a later stage.
   */
  protected String buildJson() {
    ObjectNode root = OBJECT_MAPPER.createObjectNode();
    root.put("version", SCHEMA_VERSION);
    populateConfig(root);
    try {
      return OBJECT_MAPPER.writeValueAsString(root);
    } catch (JsonProcessingException e) {
      // An in-memory node tree should never fail to serialize; never let it break connection setup.
      LOGGER.warn("Failed to serialize driver configuration report; skipping DRIVER_CONFIG", e);
      return null;
    }
  }

  /**
   * Populates the configuration groups onto the report root. Placeholder in Stage 1; Stage 2 fills
   * in {@code connection}, {@code socket}, the policy groups, {@code query-defaults}, {@code tls},
   * etc. from {@link #configuration}.
   */
  protected void populateConfig(ObjectNode root) {
    // Stage 2: populate configuration groups from `configuration`.
  }
}
