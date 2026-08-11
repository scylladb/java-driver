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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.protocol.internal.request.Startup;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class StartupOptionsBuilderTest {

  private DefaultDriverContext buildMockedContext(String compression) {

    DriverExecutionProfile defaultProfile = mock(DriverExecutionProfile.class);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn(compression);
    when(defaultProfile.getName()).thenReturn(DriverExecutionProfile.DEFAULT_NAME);
    return MockedDriverContextFactory.defaultDriverContext(Optional.of(defaultProfile));
  }

  private void assertDefaultStartupOptions(Startup startup) {

    assertThat(startup.options).containsEntry(Startup.CQL_VERSION_KEY, "3.0.0");
    assertThat(startup.options)
        .containsEntry(
            StartupOptionsBuilder.DRIVER_NAME_KEY, Session.OSS_DRIVER_COORDINATES.getName());
    assertThat(startup.options).containsKey(StartupOptionsBuilder.DRIVER_VERSION_KEY);
    Version version = Version.parse(startup.options.get(StartupOptionsBuilder.DRIVER_VERSION_KEY));
    assertThat(version).isEqualByComparingTo(Session.OSS_DRIVER_COORDINATES.getVersion());
    // SESSION_ID is innate: sent on every connection, whatever the configuration says.
    assertThat(startup.options).containsKey(StartupOptionsBuilder.SESSION_ID_KEY);
    assertThat(UUID.fromString(startup.options.get(StartupOptionsBuilder.SESSION_ID_KEY)))
        .isNotNull();
  }

  @Test
  public void should_build_startup_options_with_no_compression_if_undefined() {

    DefaultDriverContext ctx = MockedDriverContextFactory.defaultDriverContext();
    Startup startup = new Startup(ctx.getStartupOptions());
    assertThat(startup.options).doesNotContainKey(Startup.COMPRESSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_build_startup_options_with_no_compression_if_defined_as_none() {

    DefaultDriverContext ctx = buildMockedContext("none");
    Startup startup = new Startup(ctx.getStartupOptions());
    assertThat(startup.options).doesNotContainKey(Startup.COMPRESSION_KEY);
    assertDefaultStartupOptions(startup);
  }

  @Test
  @DataProvider({"lz4", "snappy"})
  public void should_build_startup_options(String compression) {

    DefaultDriverContext ctx = buildMockedContext(compression);
    Startup startup = new Startup(ctx.getStartupOptions());
    // assert the compression option is present
    assertThat(startup.options).containsEntry(Startup.COMPRESSION_KEY, compression);
    assertDefaultStartupOptions(startup);
  }

  @Test
  public void should_use_a_stable_session_id_for_the_whole_session() {

    // The startup options are built once per session and copied into every connection's STARTUP, so
    // all of a session's connections report the same SESSION_ID.
    DefaultDriverContext ctx = MockedDriverContextFactory.defaultDriverContext();
    assertThat(ctx.getStartupOptions().get(StartupOptionsBuilder.SESSION_ID_KEY))
        .isEqualTo(ctx.getStartupOptions().get(StartupOptionsBuilder.SESSION_ID_KEY));
  }

  @Test
  public void should_use_a_distinct_session_id_per_session() {

    DefaultDriverContext ctx1 = MockedDriverContextFactory.defaultDriverContext();
    DefaultDriverContext ctx2 = MockedDriverContextFactory.defaultDriverContext();
    assertThat(ctx1.getStartupOptions().get(StartupOptionsBuilder.SESSION_ID_KEY))
        .isNotEqualTo(ctx2.getStartupOptions().get(StartupOptionsBuilder.SESSION_ID_KEY));
  }

  @Test
  public void should_not_derive_session_id_from_client_id() {

    // SESSION_ID must be driver-generated, not the (user-settable) CLIENT_ID, so that it is
    // guaranteed unique per session as the grouping key requires.
    DefaultDriverContext ctx = MockedDriverContextFactory.defaultDriverContext();
    assertThat(ctx.getStartupOptions().get(StartupOptionsBuilder.SESSION_ID_KEY))
        .isNotEqualTo(ctx.getStartupOptions().get(StartupOptionsBuilder.CLIENT_ID_KEY));
  }

  @Test
  public void should_fail_to_build_startup_options_with_invalid_compression() {

    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              DefaultDriverContext ctx = buildMockedContext("foobar");
              new Startup(ctx.getStartupOptions());
            });
  }
}
