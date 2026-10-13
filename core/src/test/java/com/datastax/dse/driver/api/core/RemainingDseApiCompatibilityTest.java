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
package com.datastax.dse.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveSession;
import com.datastax.dse.driver.api.core.data.time.DateRangeBound;
import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.SerializationHelper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletionException;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class RemainingDseApiCompatibilityTest {

  @Test
  public void should_keep_legacy_signatures_and_local_value_helpers() throws Exception {
    assertThat(ContinuousSession.class).isAssignableFrom(CqlSession.class);
    assertThat(ContinuousReactiveSession.class).isAssignableFrom(CqlSession.class);
    assertThat(ProtocolVersion.DSE_V1).isSameAs(DseProtocolVersion.DSE_V1);
    assertThat(TypedDriverOption.GRAPH_NAME.getRawOption()).isSameAs(DseDriverOption.GRAPH_NAME);
    assertThat(Version.parse("4.19.2.3").getDSEPatch()).isEqualTo(3);
    assertThat(DateRangeBound.parseLowerBound("2020-01-01")).isNotNull();
  }

  @Test
  public void should_fail_fast_for_unsupported_dse_execution() {
    ContinuousSession continuousSession = mock(ContinuousSession.class, CALLS_REAL_METHODS);
    SimpleStatement statement = SimpleStatement.newInstance("SELECT 1");

    assertThatThrownBy(() -> continuousSession.executeContinuously(statement))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSession.builder().buildAsync().toCompletableFuture().join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseTypeCodecs.DATE_RANGE.encode(null, ProtocolVersion.V4))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_read_older_serialized_dse_options() throws Exception {
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
    assertThat(deserialized.get(TypedDriverOption.APPLICATION_NAME))
        .isEqualTo("legacy-application");
    assertThat(deserialized.get(TypedDriverOption.APPLICATION_VERSION)).isEqualTo("4.19.2.1");
    assertThat(deserialized.get("legacy-profile", TypedDriverOption.APPLICATION_NAME))
        .isEqualTo("profile-application");
    assertThat(deserialized.get("legacy-profile", TypedDriverOption.APPLICATION_VERSION))
        .isEqualTo("profile-version");
    try (DriverConfigLoader loader = DriverConfigLoader.fromMap(deserialized)) {
      DriverExecutionProfile profile = loader.getInitialConfig().getDefaultProfile();
      assertThat(profile.isDefined(DseDriverOption.APPLICATION_NAME)).isTrue();
      assertThat(profile.isDefined(DseDriverOption.APPLICATION_VERSION)).isTrue();
      assertThat(profile.getString(DseDriverOption.APPLICATION_NAME))
          .isEqualTo("legacy-application");
      assertThat(profile.getString(DseDriverOption.APPLICATION_VERSION)).isEqualTo("4.19.2.1");
      assertThat(
              loader
                  .getInitialConfig()
                  .getDefaultProfile()
                  .getString(DefaultDriverOption.APPLICATION_NAME))
          .isEqualTo("legacy-application");
      assertThat(
              loader
                  .getInitialConfig()
                  .getDefaultProfile()
                  .getString(DefaultDriverOption.APPLICATION_VERSION))
          .isEqualTo("4.19.2.1");
    }
  }

  @Test
  public void should_share_application_aliases_before_and_after_serialization() {
    OptionsMap options = new OptionsMap();
    TypedDriverOption<String> legacyName =
        new TypedDriverOption<>(DseDriverOption.APPLICATION_NAME, GenericType.STRING);
    TypedDriverOption<String> legacyVersion =
        new TypedDriverOption<>(DseDriverOption.APPLICATION_VERSION, GenericType.STRING);
    options.put(legacyName, "legacy-name");
    options.put(legacyVersion, "legacy-version");
    options.put(TypedDriverOption.APPLICATION_NAME, "current-name");
    options.put(TypedDriverOption.APPLICATION_VERSION, "current-version");

    OptionsMap deserialized = SerializationHelper.serializeAndDeserialize(options);
    assertThat(deserialized.get(TypedDriverOption.APPLICATION_NAME)).isEqualTo("current-name");
    assertThat(deserialized.get(TypedDriverOption.APPLICATION_VERSION))
        .isEqualTo("current-version");
    assertThat(deserialized.get(legacyName)).isEqualTo("current-name");
    assertThat(deserialized.get(legacyVersion)).isEqualTo("current-version");
  }

  @Test
  public void should_share_application_alias_updates_removals_and_derived_overrides() {
    OptionsMap options = new OptionsMap();
    TypedDriverOption<String> legacyName =
        new TypedDriverOption<>(DseDriverOption.APPLICATION_NAME, GenericType.STRING);
    TypedDriverOption<String> legacyVersion =
        new TypedDriverOption<>(DseDriverOption.APPLICATION_VERSION, GenericType.STRING);
    options.put(legacyName, "legacy-name");
    options.put(legacyVersion, "legacy-version");
    assertThat(options.get(TypedDriverOption.APPLICATION_NAME)).isEqualTo("legacy-name");
    assertThat(options.get(TypedDriverOption.APPLICATION_VERSION)).isEqualTo("legacy-version");
    try (DriverConfigLoader loader = DriverConfigLoader.fromMap(options)) {
      DriverExecutionProfile profile = loader.getInitialConfig().getDefaultProfile();
      DriverExecutionProfile derived =
          profile
              .withString(DseDriverOption.APPLICATION_NAME, "derived-name")
              .withString(DefaultDriverOption.APPLICATION_VERSION, "derived-version");
      assertThat(derived.getString(DefaultDriverOption.APPLICATION_NAME)).isEqualTo("derived-name");
      assertThat(derived.getString(DseDriverOption.APPLICATION_VERSION))
          .isEqualTo("derived-version");
      assertThat(
              derived
                  .without(DseDriverOption.APPLICATION_NAME)
                  .isDefined(DefaultDriverOption.APPLICATION_NAME))
          .isFalse();
      assertThat(
              derived
                  .withString(DefaultDriverOption.APPLICATION_NAME, "new-name")
                  .getString(DseDriverOption.APPLICATION_NAME))
          .isEqualTo("new-name");
      assertThat(options.remove(legacyName)).isEqualTo("legacy-name");
      assertThat(profile.isDefined(DefaultDriverOption.APPLICATION_NAME)).isFalse();
      assertThat(profile.isDefined(DseDriverOption.APPLICATION_NAME)).isFalse();
      assertThat(options.remove(TypedDriverOption.APPLICATION_VERSION)).isEqualTo("legacy-version");
      assertThat(options.get(legacyVersion)).isNull();
    }
  }

  @Test
  public void should_preserve_legacy_option_when_delegating_to_custom_base_profile() {
    DriverExecutionProfile customProfile = mock(DriverExecutionProfile.class, CALLS_REAL_METHODS);
    when(customProfile.isDefined(DseDriverOption.APPLICATION_NAME)).thenReturn(true);
    when(customProfile.getString(DseDriverOption.APPLICATION_NAME)).thenReturn("custom-name");
    DriverExecutionProfile derived =
        customProfile.withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE");
    assertThat(derived.isDefined(DseDriverOption.APPLICATION_NAME)).isTrue();
    assertThat(derived.getString(DseDriverOption.APPLICATION_NAME)).isEqualTo("custom-name");
  }
}
