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

import com.datastax.oss.driver.internal.SerializationHelper;
import java.io.ObjectInputStream;
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
        .isEqualTo(Duration.ofSeconds(11));
    assertThat(deserialized.get("slow", TypedDriverOption.REQUEST_TIMEOUT)).isEqualTo(slowTimeout);
    // Listeners are transient
    assertThat(deserialized.removeChangeListener(mockListener)).isFalse();
  }

  @Test
  public void should_deserialize_proxy_with_removed_provenance_fields() throws Exception {
    // Captured from the previous SerializationProxy, which had explicitOptions and legacyUnknown.
    try (ObjectInputStream stream =
        new ObjectInputStream(
            Base64.getMimeDecoder()
                .wrap(
                    OptionsMapTest.class.getResourceAsStream(
                        "/config/options-map-with-provenance.base64")))) {
      OptionsMap deserialized = (OptionsMap) stream.readObject();
      deserialized.put(TypedDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(17));
      assertThat(deserialized.get(TypedDriverOption.REQUEST_TIMEOUT))
          .isEqualTo(Duration.ofSeconds(17));
    }
  }
}
