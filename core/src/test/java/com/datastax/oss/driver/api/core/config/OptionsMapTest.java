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
package com.datastax.oss.driver.api.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.internal.SerializationHelper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.function.Consumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OptionsMapTest {
  @Mock private Consumer<OptionsMap> mockListener;

  @Test
  public void should_serialize_and_deserialize() {
    // Given
    OptionsMap initial = OptionsMap.driverDefaults();
    Duration slowTimeout = Duration.ofSeconds(30);
    initial.put("slow", TypedDriverOption.REQUEST_TIMEOUT, slowTimeout);
    initial.addChangeListener(mockListener);

    // When
    OptionsMap deserialized = SerializationHelper.serializeAndDeserialize(initial);

    // Then
    assertThat(deserialized.get(TypedDriverOption.REQUEST_TIMEOUT))
        .isEqualTo(Duration.ofSeconds(2));
    assertThat(deserialized.get("slow", TypedDriverOption.REQUEST_TIMEOUT)).isEqualTo(slowTimeout);
    // Listeners are transient
    assertThat(deserialized.removeChangeListener(mockListener)).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_serialize_and_deserialize_deprecated_graph_options() {
    OptionsMap initial = OptionsMap.driverDefaults();
    assertThat(initial.get(TypedDriverOption.GRAPH_TRAVERSAL_SOURCE)).isEqualTo("g");
    assertThat(initial.get(TypedDriverOption.GRAPH_PAGING_ENABLED)).isEqualTo("AUTO");
    initial.put(TypedDriverOption.GRAPH_NAME, "legacy-graph");

    OptionsMap deserialized = SerializationHelper.serializeAndDeserialize(initial);

    assertThat(deserialized.get(TypedDriverOption.GRAPH_NAME)).isEqualTo("legacy-graph");
    assertThat(deserialized.get(TypedDriverOption.METRICS_SESSION_GRAPH_REQUESTS_HIGHEST))
        .isEqualTo(Duration.ofSeconds(12));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_deserialize_graph_options_written_by_4_19_2_1() throws Exception {
    byte[] encoded;
    try (InputStream input =
        getClass().getResourceAsStream("/config/options-map-4.19.2.1.base64")) {
      assertThat(input).isNotNull();
      encoded = input.readAllBytes();
    }

    OptionsMap deserialized =
        SerializationHelper.deserialize(
            Base64.getDecoder().decode(new String(encoded, StandardCharsets.US_ASCII).trim()));

    assertThat(deserialized.get(TypedDriverOption.GRAPH_NAME)).isEqualTo("legacy-graph");
    assertThat(deserialized.get(TypedDriverOption.GRAPH_READ_CONSISTENCY_LEVEL))
        .isEqualTo("LOCAL_QUORUM");
    assertThat(deserialized.get(TypedDriverOption.GRAPH_CONTINUOUS_PAGING_PAGE_SIZE))
        .isEqualTo(100);
    assertThat(deserialized.get(TypedDriverOption.METRICS_NODE_GRAPH_MESSAGES_SLO))
        .containsExactly(Duration.ofMillis(100));
    int graphOptionCount = 0;
    for (TypedDriverOption<?> option : TypedDriverOption.builtInValues()) {
      if (option.getRawOption() instanceof DseDriverOption
          && option.getRawOption().toString().contains("GRAPH")) {
        assertThat(deserialized.get(option)).as(option.getRawOption().toString()).isNotNull();
        graphOptionCount++;
      }
    }
    assertThat(graphOptionCount).isEqualTo(24);
  }
}
