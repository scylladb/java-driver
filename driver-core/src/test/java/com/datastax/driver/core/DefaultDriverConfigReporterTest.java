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

import static org.assertj.core.api.Assertions.assertThat;

import org.testng.annotations.Test;

public class DefaultDriverConfigReporterTest {

  private static Configuration config() {
    return Configuration.builder().build();
  }

  @Test(groups = "unit")
  public void should_enable_driver_config_reporting_by_default() {
    assertThat(config().isDriverConfigReportingEnabled()).isTrue();
    assertThat(
            Cluster.builder()
                .addContactPoint("127.0.0.1")
                .getConfiguration()
                .isDriverConfigReportingEnabled())
        .isTrue();
  }

  @Test(groups = "unit")
  public void should_report_schema_version() {
    // Stage 1 emits only the schema version.
    assertThat(new DefaultDriverConfigReporter(config()).buildReport())
        .isEqualTo("{\"version\":1}");
  }

  @Test(groups = "unit")
  public void should_be_fail_safe_when_report_build_throws() {
    DefaultDriverConfigReporter reporter =
        new DefaultDriverConfigReporter(config()) {
          @Override
          protected String buildJson() {
            throw new RuntimeException("boom");
          }
        };

    // Must not propagate the failure (it runs on the cluster-initialization path); no report means
    // no DRIVER_CONFIG option is sent, and nothing else about the connection is affected.
    assertThat(reporter.buildReport()).isNull();
  }
}
