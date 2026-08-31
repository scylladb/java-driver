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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.protocol.Lz4Compressor;
import com.datastax.oss.driver.internal.core.protocol.SnappyCompressor;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.NoopCompressor;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DefaultDriverContextTest {

  private DefaultDriverContext buildMockedContext(Optional<String> compressionOption) {

    DriverExecutionProfile defaultProfile = mock(DriverExecutionProfile.class);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none"))
        .thenReturn(compressionOption.orElse("none"));
    return MockedDriverContextFactory.defaultDriverContext(Optional.of(defaultProfile));
  }

  private void doCreateCompressorTest(Optional<String> configVal, Class<?> expectedClz) {

    DefaultDriverContext ctx = buildMockedContext(configVal);
    Compressor<ByteBuf> compressor = ctx.getCompressor();
    assertThat(compressor).isNotNull();
    assertThat(compressor).isInstanceOf(expectedClz);
  }

  @Test
  @DataProvider({"lz4", "lZ4", "Lz4", "LZ4"})
  public void should_create_lz4_compressor(String name) {

    doCreateCompressorTest(Optional.of(name), Lz4Compressor.class);
  }

  @Test
  @DataProvider({"snappy", "SNAPPY", "sNaPpY", "SNapPy"})
  public void should_create_snappy_compressor(String name) {

    doCreateCompressorTest(Optional.of(name), SnappyCompressor.class);
  }

  @Test
  public void should_create_noop_compressor_if_undefined() {

    doCreateCompressorTest(Optional.empty(), NoopCompressor.class);
  }

  @Test
  @DataProvider({"none", "NONE", "NoNe", "nONe"})
  public void should_create_noop_compressor_if_defined_as_none(String name) {

    doCreateCompressorTest(Optional.of(name), NoopCompressor.class);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_warn_and_ignore_deprecated_graph_configuration() {
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(InternalDriverContext.class, Level.WARN);
    try {
      DriverConfigLoader loader =
          DriverConfigLoader.fromString(
              "datastax-java-driver {\n"
                  + " basic.graph.name = \"legacy-graph\"\n"
                  + " advanced.graph.paging-enabled = ENABLED\n"
                  + " advanced.metrics.session.enabled = [graph-requests, graph-client-timeouts]\n"
                  + " advanced.metrics.node.enabled = [graph-messages]\n"
                  + "}\n");

      DefaultDriverContext context =
          new DefaultDriverContext(loader, ProgrammaticArguments.builder().build());

      assertThat(context.getConfig().getDefaultProfile().getString(DseDriverOption.GRAPH_NAME))
          .isEqualTo("legacy-graph");
      verify(logger.appender, times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains("DSE Graph configuration is deprecated and ignored")
          .contains("basic.graph.name")
          .contains("graph-requests")
          .contains("graph-client-timeouts")
          .contains("graph-messages");
    } finally {
      logger.close();
    }
  }

  @Test
  public void should_not_warn_for_deprecated_graph_defaults() {
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(InternalDriverContext.class, Level.WARN);
    try {
      DriverConfigLoader loader = DriverConfigLoader.fromString("datastax-java-driver {}\n");

      new DefaultDriverContext(loader, ProgrammaticArguments.builder().build());

      verify(logger.appender, never()).doAppend(logger.loggingEventCaptor.capture());
    } finally {
      logger.close();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_warn_when_deprecated_graph_configuration_is_added_at_runtime() {
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(InternalDriverContext.class, Level.WARN);
    OptionsMap options = OptionsMap.driverDefaults();
    DriverConfigLoader loader = DriverConfigLoader.fromMap(options);
    try {
      DefaultDriverContext context =
          new DefaultDriverContext(loader, ProgrammaticArguments.builder().build());
      loader.onDriverInit(context);

      options.put(TypedDriverOption.GRAPH_NAME, "legacy-graph");

      verify(logger.appender, times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains("DSE Graph configuration is deprecated and ignored")
          .contains("basic.graph.name");
    } finally {
      loader.close();
      logger.close();
    }
  }
}
