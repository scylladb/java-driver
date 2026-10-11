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

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionException;
import org.junit.Test;

public class LegacyCloudCompatibilityTest {

  @Test
  @SuppressWarnings("deprecation")
  public void should_reject_secure_connect_bundle_in_hocon() {
    DriverConfigLoader loader =
        DriverConfigLoader.fromString(
            "datastax-java-driver.basic.cloud.secure-connect-bundle = bundle.zip");

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () ->
                new TestSessionBuilder()
                    .withConfigLoader(loader)
                    .buildAsync()
                    .toCompletableFuture()
                    .join())
        .withCauseInstanceOf(UnsupportedOperationException.class)
        .withMessageContaining("DataStax Astra");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_reject_secure_connect_bundle_in_options_map() {
    OptionsMap options = OptionsMap.driverDefaults();
    options.put(TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, "bundle.zip");

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () ->
                new TestSessionBuilder()
                    .withConfigLoader(DriverConfigLoader.fromMap(options))
                    .buildAsync()
                    .toCompletableFuture()
                    .join())
        .withCauseInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_reject_programmatic_cloud_proxy_address() {
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(
            () ->
                ProgrammaticArguments.builder()
                    .withCloudProxyAddress(new InetSocketAddress("localhost", 9042)))
        .withMessageContaining("DataStax Astra");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_reject_null_programmatic_cloud_proxy_address() {
    assertThatExceptionOfType(UnsupportedOperationException.class)
        .isThrownBy(() -> ProgrammaticArguments.builder().withCloudProxyAddress(null))
        .withMessageContaining("DataStax Astra");
  }

  private static class TestSessionBuilder extends SessionBuilder<TestSessionBuilder, CqlSession> {
    @Override
    protected CqlSession wrap(@NonNull CqlSession defaultSession) {
      return defaultSession;
    }
  }
}
