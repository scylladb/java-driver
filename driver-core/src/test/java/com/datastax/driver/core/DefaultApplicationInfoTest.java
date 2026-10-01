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

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class DefaultApplicationInfoTest {

  @Test(groups = "unit")
  public void should_add_all_options_when_values_are_set() {
    Map<String, String> options = addOptions("app", "1.2.3", "client-1");
    assertThat(options).hasSize(3);
    assertThat(options.get("APPLICATION_NAME")).isEqualTo("app");
    assertThat(options.get("APPLICATION_VERSION")).isEqualTo("1.2.3");
    assertThat(options.get("CLIENT_ID")).isEqualTo("client-1");
  }

  @Test(groups = "unit")
  public void should_add_no_options_when_values_are_null() {
    assertThat(addOptions(null, null, null)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_add_no_options_when_values_are_empty() {
    assertThat(addOptions("", "", "")).isEmpty();
  }

  @Test(groups = "unit")
  public void should_add_only_options_that_are_set() {
    assertThat(addOptions("app", null, "")).hasSize(1).containsEntry("APPLICATION_NAME", "app");
    assertThat(addOptions("", "1.0", null)).hasSize(1).containsEntry("APPLICATION_VERSION", "1.0");
    assertThat(addOptions(null, "", "id")).hasSize(1).containsEntry("CLIENT_ID", "id");
  }

  @Test(groups = "unit")
  public void should_keep_existing_options() {
    Map<String, String> options = new HashMap<String, String>();
    options.put("CQL_VERSION", "3.0.0");
    new DefaultApplicationInfo("app", null, null).addOption(options);
    assertThat(options).hasSize(2).containsEntry("CQL_VERSION", "3.0.0");
  }

  private static Map<String, String> addOptions(
      String applicationName, String applicationVersion, String clientId) {
    Map<String, String> options = new HashMap<String, String>();
    new DefaultApplicationInfo(applicationName, applicationVersion, clientId).addOption(options);
    return options;
  }
}
