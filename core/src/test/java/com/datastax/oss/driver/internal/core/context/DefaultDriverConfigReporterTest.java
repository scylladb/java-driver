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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class DefaultDriverConfigReporterTest {

  private InternalDriverContext context;
  private DriverExecutionProfile profile;
  private DefaultDriverConfigReporter reporter;

  @Before
  public void setup() {
    context = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    profile = mock(DriverExecutionProfile.class);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    reporter = new DefaultDriverConfigReporter(context);
  }

  private void enableReporting(boolean enabled) {
    when(profile.getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, false))
        .thenReturn(enabled);
  }

  @Test
  public void should_add_nothing_when_disabled() {
    enableReporting(false);
    Map<String, String> options = new HashMap<>();
    reporter.populateStartupOptions(options, /* reportDriverConfig= */ true);
    assertThat(options).doesNotContainKey(DefaultDriverConfigReporter.SESSION_ID_KEY);
    assertThat(options).doesNotContainKey(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY);
  }

  @Test
  public void should_add_session_id_and_driver_config_on_control_connection() {
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporter.populateStartupOptions(options, /* reportDriverConfig= */ true);
    // SESSION_ID is a valid, driver-generated UUID.
    String sessionId = options.get(DefaultDriverConfigReporter.SESSION_ID_KEY);
    assertThat(sessionId).isNotNull();
    assertThat(UUID.fromString(sessionId)).isNotNull(); // does not throw => valid UUID
    // Stage 1 emits only the schema version; the value must be valid compact JSON.
    assertThat(options.get(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY))
        .isEqualTo("{\"version\":" + DefaultDriverConfigReporter.SCHEMA_VERSION + "}");
  }

  @Test
  public void should_add_session_id_only_on_pool_connection() {
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporter.populateStartupOptions(options, /* reportDriverConfig= */ false);
    assertThat(options).containsKey(DefaultDriverConfigReporter.SESSION_ID_KEY);
    assertThat(options).doesNotContainKey(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY);
  }

  @Test
  public void should_use_a_stable_session_id_across_connections() {
    enableReporting(true);
    Map<String, String> control = new HashMap<>();
    Map<String, String> pool = new HashMap<>();
    reporter.populateStartupOptions(control, true);
    reporter.populateStartupOptions(pool, false);
    assertThat(pool.get(DefaultDriverConfigReporter.SESSION_ID_KEY))
        .isEqualTo(control.get(DefaultDriverConfigReporter.SESSION_ID_KEY));
  }

  @Test
  public void should_use_a_distinct_session_id_per_reporter() {
    enableReporting(true);
    Map<String, String> first = new HashMap<>();
    reporter.populateStartupOptions(first, false);
    // A second session (new reporter instance) must get a different SESSION_ID.
    Map<String, String> second = new HashMap<>();
    new DefaultDriverConfigReporter(context).populateStartupOptions(second, false);
    assertThat(second.get(DefaultDriverConfigReporter.SESSION_ID_KEY))
        .isNotEqualTo(first.get(DefaultDriverConfigReporter.SESSION_ID_KEY));
  }

  /** Reporting must never break the connection: a failed config read is swallowed entirely. */
  @Test
  public void should_not_throw_when_reading_the_flag_fails() {
    when(profile.getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, false))
        .thenThrow(new IllegalStateException("config blew up"));
    Map<String, String> options = new HashMap<>();
    reporter.populateStartupOptions(options, true); // must not throw
    assertThat(options).doesNotContainKey(DefaultDriverConfigReporter.SESSION_ID_KEY);
    assertThat(options).doesNotContainKey(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY);
  }

  /**
   * Reporting must never break the connection: a failure while building the config groups (as a
   * Stage 2 policy introspection might) is swallowed. SESSION_ID is still emitted (it is added
   * before, and independently of, the DRIVER_CONFIG blob); only DRIVER_CONFIG is omitted.
   */
  @Test
  public void should_keep_session_id_but_skip_driver_config_when_building_config_groups_fails() {
    enableReporting(true);
    DefaultDriverConfigReporter throwingReporter =
        new DefaultDriverConfigReporter(context) {
          @Override
          protected void populateConfig(ObjectNode root, DriverExecutionProfile config) {
            throw new IllegalStateException("policy introspection blew up");
          }
        };
    Map<String, String> options = new HashMap<>();
    throwingReporter.populateStartupOptions(options, true); // must not throw
    assertThat(options).containsKey(DefaultDriverConfigReporter.SESSION_ID_KEY);
    assertThat(options).doesNotContainKey(DefaultDriverConfigReporter.DRIVER_CONFIG_KEY);
  }
}
