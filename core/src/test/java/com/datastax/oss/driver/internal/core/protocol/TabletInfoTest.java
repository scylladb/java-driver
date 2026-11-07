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
package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TabletInfoTest {
  @Test
  public void should_prefer_v2_and_combine_with_metadata_ids() {
    ProtocolFeatureStore features =
        ProtocolFeatureStore.parseSupportedOptions(
            ImmutableMap.of(
                "TABLETS_ROUTING_V1", ImmutableList.of(""),
                "TABLETS_ROUTING_V2_EXPERIMENTAL", ImmutableList.of(""),
                "SCYLLA_USE_METADATA_ID", ImmutableList.of("")));
    Map<String, String> startup = new HashMap<>();
    features.populateStartupOptions(startup);
    assertThat(startup)
        .containsEntry("TABLETS_ROUTING_V2_EXPERIMENTAL", "")
        .doesNotContainKey("TABLETS_ROUTING_V1");
    assertThat(features.getProtocolFeatures().isScyllaTabletsRoutingV2()).isTrue();
    assertThat(features.getProtocolFeatures().isScyllaUseMetadataId()).isTrue();
  }

  @Test
  public void should_preserve_v1_and_disable_v2_when_not_advertised() {
    ProtocolFeatureStore features =
        ProtocolFeatureStore.parseSupportedOptions(
            ImmutableMap.of("TABLETS_ROUTING_V1", ImmutableList.of("")));
    Map<String, String> startup = new HashMap<>();
    features.populateStartupOptions(startup);
    assertThat(startup)
        .containsEntry("TABLETS_ROUTING_V1", "")
        .doesNotContainKey("TABLETS_ROUTING_V2_EXPERIMENTAL");
    assertThat(features.getProtocolFeatures().isScyllaTabletsRoutingV2()).isFalse();
    assertThat(ProtocolFeatureStore.EMPTY.getProtocolFeatures().isScyllaTabletsRoutingV2())
        .isFalse();
  }

  @Test
  public void should_prefer_v2_with_server_empty_supported_lists() {
    ProtocolFeatureStore features =
        ProtocolFeatureStore.parseSupportedOptions(
            ImmutableMap.of(
                "TABLETS_ROUTING_V1", ImmutableList.of(""),
                "TABLETS_ROUTING_V2_EXPERIMENTAL", ImmutableList.of()));
    Map<String, String> startup = new HashMap<>();
    features.populateStartupOptions(startup);
    assertThat(startup)
        .containsEntry("TABLETS_ROUTING_V2_EXPERIMENTAL", "")
        .doesNotContainKey("TABLETS_ROUTING_V1");
    assertThat(features.getProtocolFeatures().isScyllaTabletsRoutingV2()).isTrue();
  }

  @Test
  public void should_ignore_invalid_supported_value() {
    TabletInfo info =
        TabletInfo.loadFromSupportedOptions(
            ImmutableMap.of("TABLETS_ROUTING_V2_EXPERIMENTAL", ImmutableList.of("unsupported")));
    assertThat(info.isEnabled()).isFalse();
  }
}
