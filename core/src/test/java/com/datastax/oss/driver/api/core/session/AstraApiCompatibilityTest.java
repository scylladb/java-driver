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
package com.datastax.oss.driver.api.core.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Paths;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class AstraApiCompatibilityTest {

  @Test
  public void should_retain_deprecated_builder_signatures_and_reject_astra() throws Exception {
    assertThatThrownBy(() -> CqlSession.builder().withCloudSecureConnectBundle(Paths.get("bundle")))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Astra");
    assertThatThrownBy(
            () -> CqlSession.builder().withCloudSecureConnectBundle(new URL("file:/bundle")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () ->
                CqlSession.builder()
                    .withCloudSecureConnectBundle(new ByteArrayInputStream(new byte[0])))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () -> CqlSession.builder().withCloudProxyAddress(new InetSocketAddress(9042)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_retain_deprecated_argument_and_option_signatures() {
    assertThat(TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE.getRawOption())
        .isSameAs(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE);
    assertThatThrownBy(
            () ->
                ProgrammaticArguments.builder().withCloudProxyAddress(new InetSocketAddress(9042)))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(ProgrammaticArguments.builder().build().getCloudProxyAddress()).isNull();
  }

  @Test
  public void should_reject_configured_astra_bundle_before_connecting() {
    OptionsMap options = OptionsMap.driverDefaults();
    options.put(TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, "bundle.zip");

    assertThatThrownBy(
            () ->
                CqlSession.builder()
                    .withConfigLoader(DriverConfigLoader.fromMap(options))
                    .buildAsync()
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Astra");
  }

  @Test
  public void should_reject_legacy_astra_bundle_stream_before_connecting() {
    assertThatThrownBy(
            () ->
                new LegacyAstraSessionBuilder()
                    .withConfigLoader(DriverConfigLoader.fromMap(OptionsMap.driverDefaults()))
                    .buildAsync()
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Astra");
  }

  private static class LegacyAstraSessionBuilder extends CqlSessionBuilder {
    private LegacyAstraSessionBuilder() {
      cloudConfigInputStream =
          () -> {
            throw new AssertionError("The unsupported bundle stream must not be opened");
          };
    }

    @Override
    protected DriverContext buildContext(
        DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
      throw new AssertionError("The driver context must not be created for an unsupported bundle");
    }
  }
}
