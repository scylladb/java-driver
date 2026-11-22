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
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class CCMBridgeUpdateConfTest {

  private static Map<String, Object> sample() {
    Map<String, Object> m = new LinkedHashMap<String, Object>();
    m.put("start_rpc", false);
    m.put("storage_port", 7000);
    m.put("read_request_timeout_in_ms", 5000);
    m.put("enable_user_defined_functions", true);
    m.put("key_cache_size_in_kb", 10);
    m.put("client_encryption_options.enabled", true);
    return m;
  }

  @Test(groups = "unit")
  public void should_join_all_entries_into_one_argument_list_before_4_1() {
    assertThat(CCMBridge.buildUpdateConfArgs(sample(), VersionNumber.parse("3.11.0")))
        .isEqualTo(
            "start_rpc:false storage_port:7000 read_request_timeout_in_ms:5000"
                + " enable_user_defined_functions:true key_cache_size_in_kb:10"
                + " client_encryption_options.enabled:true");
  }

  @Test(groups = "unit")
  public void should_translate_keys_and_values_for_4_1_and_above() {
    assertThat(CCMBridge.buildUpdateConfArgs(sample(), VersionNumber.parse("4.1.0")))
        .isEqualTo(
            "start_rpc:false storage_port:7000 read_request_timeout:5000ms"
                + " user_defined_functions_enabled:true key_cache_size:10KiB"
                + " client_encryption_options.enabled:true");
  }

  @Test(groups = "unit")
  public void should_handle_single_and_empty_maps() {
    assertThat(
            CCMBridge.buildUpdateConfArgs(
                ImmutableMap.<String, Object>of("a", 1), VersionNumber.parse("3.0.0")))
        .isEqualTo("a:1");
    assertThat(
            CCMBridge.buildUpdateConfArgs(
                Collections.<String, Object>emptyMap(), VersionNumber.parse("3.0.0")))
        .isEmpty();
  }

  @Test(groups = "unit")
  public void should_invoke_updateConfig_once_with_all_translated_entries() {
    assertOneUpdateConf(
        "3.11.0",
        "start_rpc:false storage_port:7000 read_request_timeout_in_ms:5000"
            + " enable_user_defined_functions:true key_cache_size_in_kb:10"
            + " client_encryption_options.enabled:true");
    assertOneUpdateConf(
        "4.1.0",
        "start_rpc:false storage_port:7000 read_request_timeout:5000ms"
            + " user_defined_functions_enabled:true key_cache_size:10KiB"
            + " client_encryption_options.enabled:true");
  }

  @Test(groups = "unit")
  public void should_not_invoke_updateConfig_when_the_map_is_empty() {
    RecordingBridge bridge = new RecordingBridge(VersionNumber.parse("3.11.0"));
    try {
      bridge.updateConfig(Collections.<String, Object>emptyMap());
      assertThat(bridge.commands).isEmpty();
    } finally {
      bridge.getCcmDir().delete();
    }
  }

  private static void assertOneUpdateConf(String version, String translatedEntries) {
    RecordingBridge bridge = new RecordingBridge(VersionNumber.parse(version));
    try {
      bridge.updateConfig(sample());
      assertThat(bridge.commands).hasSize(1);
      assertThat(bridge.commands.get(0)).contains("updateconf " + translatedEntries);
    } finally {
      bridge.getCcmDir().delete();
    }
  }

  /** Records ccm commands from {@link CCMBridge#updateConfig} without starting ccm. */
  private static final class RecordingBridge extends CCMBridge {
    final List<String> commands = new ArrayList<String>();

    RecordingBridge(VersionNumber version) {
      super(
          "updateconf-unit",
          version,
          null,
          null,
          "127.0.0.",
          7000,
          9160,
          9042,
          new int[] {7199},
          null,
          new int[] {1});
    }

    @Override
    String execute(String command, Object... args) {
      commands.add(String.format(command, args));
      return "";
    }
  }
}
