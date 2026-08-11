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
import org.junit.Before;
import org.junit.Test;

// SESSION_ID is not this class's concern: it is an innate startup option built by
// StartupOptionsBuilder and sent on every connection regardless of these settings, so it is covered
// by StartupOptionsBuilderTest instead.
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
    reporter.populateControlConnectionOptions(options);
    assertThat(options).isEmpty();
  }

  @Test
  public void should_add_driver_config_when_enabled() {
    enableReporting(true);
    Map<String, String> options = new HashMap<>();
    reporter.populateControlConnectionOptions(options);
    // Stage 1 emits only the schema version; the value must be valid compact JSON.
    assertThat(options)
        .hasSize(1)
        .containsEntry(
            DefaultDriverConfigReporter.DRIVER_CONFIG_KEY,
            "{\"version\":" + DefaultDriverConfigReporter.SCHEMA_VERSION + "}");
  }

  /** Reporting must never break the connection: a failed config read is swallowed entirely. */
  @Test
  public void should_not_throw_when_reading_the_flag_fails() {
    when(profile.getBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, false))
        .thenThrow(new IllegalStateException("config blew up"));
    Map<String, String> options = new HashMap<>();
    reporter.populateControlConnectionOptions(options); // must not throw
    assertThat(options).isEmpty();
  }

  /**
   * Reporting must never break the connection: a failure while building the config groups (as a
   * Stage 2 policy introspection might) is swallowed, and DRIVER_CONFIG is simply omitted.
   */
  @Test
  public void should_skip_driver_config_when_building_config_groups_fails() {
    enableReporting(true);
    DefaultDriverConfigReporter throwingReporter =
        new DefaultDriverConfigReporter(context) {
          @Override
          protected void populateConfig(ObjectNode root, DriverExecutionProfile config) {
            throw new IllegalStateException("policy introspection blew up");
          }
        };
    Map<String, String> options = new HashMap<>();
    throwingReporter.populateControlConnectionOptions(options); // must not throw
    assertThat(options).isEmpty();
  }
}
