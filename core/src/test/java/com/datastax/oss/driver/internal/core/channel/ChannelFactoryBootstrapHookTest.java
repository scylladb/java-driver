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
package com.datastax.oss.driver.internal.core.channel;

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

/**
 * Verifies the {@link NettyOptions#afterBootstrapInitialized(Bootstrap)} contract: the hook runs on
 * a handler-less bootstrap, and a handler it installs is replaced by the driver's own.
 */
public class ChannelFactoryBootstrapHookTest extends ChannelFactoryTestBase {

  @Test
  public void should_replace_handler_installed_by_bootstrap_hook() {
    // Given – a hook that (incorrectly) installs its own channel handler. The driver sets its own
    // handler on each per-attempt copy afterwards, logging a one-time warning; if the dummy
    // handler below survived instead, the protocol handshake would never happen and this connect
    // would fail on the init timeout.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    doAnswer(
            invocation -> {
              Bootstrap bootstrap = invocation.getArgument(0);
              bootstrap.handler(new ChannelInboundHandlerAdapter());
              return null;
            })
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    // Then
    assertThatStage(channelFuture).isSuccess();
  }
}
