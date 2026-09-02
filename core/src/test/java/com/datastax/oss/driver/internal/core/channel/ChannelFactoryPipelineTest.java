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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopSessionMetricUpdater;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class ChannelFactoryPipelineTest {

  @Test
  @SuppressWarnings("unchecked")
  public void should_install_ssl_before_protocol_init_and_channel_customization() {
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    MetricsFactory metricsFactory = mock(MetricsFactory.class);
    SslHandlerFactory sslHandlerFactory = mock(SslHandlerFactory.class);
    SslHandler sslHandler = mock(SslHandler.class);
    FrameCodec<ByteBuf> frameCodec = mock(FrameCodec.class);

    when(context.getSessionName()).thenReturn("test");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(profile.getDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT))
        .thenReturn(Duration.ofSeconds(1));
    when(profile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .thenReturn(Duration.ofSeconds(1));
    when(profile.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL))
        .thenReturn(Duration.ofSeconds(30));
    when(profile.getBytes(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH)).thenReturn(1024L);
    when(profile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS)).thenReturn(128);
    when(profile.getInt(DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS)).thenReturn(32);
    when(context.getSslHandlerFactory()).thenReturn(Optional.of(sslHandlerFactory));
    when(sslHandlerFactory.newSslHandler(any(), eq(ChannelFactoryTestBase.SERVER_ADDRESS)))
        .thenReturn(sslHandler);
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    when(metricsFactory.getSessionUpdater()).thenReturn(NoopSessionMetricUpdater.INSTANCE);
    when(context.getFrameCodec()).thenReturn(frameCodec);
    when(context.getNettyOptions()).thenReturn(nettyOptions);

    AtomicBoolean observedCompletePipeline = new AtomicBoolean();
    doAnswer(
            invocation -> {
              Channel channel = invocation.getArgument(0);
              List<String> names = channel.pipeline().names();
              assertThat(channel.pipeline().get(ChannelFactory.SSL_HANDLER_NAME))
                  .isSameAs(sslHandler);
              assertThat(names.indexOf(ChannelFactory.SSL_HANDLER_NAME))
                  .isLessThan(names.indexOf(ChannelFactory.INIT_HANDLER_NAME));
              observedCompletePipeline.set(true);
              return null;
            })
        .when(nettyOptions)
        .afterChannelInitialized(any());

    ChannelFactory factory = new ChannelFactory(context);
    CompletableFuture<DriverChannel> resultFuture = new CompletableFuture<>();
    ChannelInitializer<Channel> initializer =
        factory.initializer(
            ChannelFactoryTestBase.SERVER_ADDRESS,
            DefaultProtocolVersion.V4,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            resultFuture);

    EmbeddedChannel channel = new EmbeddedChannel(initializer);
    try {
      assertThat(observedCompletePipeline).isTrue();
      assertThat(resultFuture).isNotCompletedExceptionally();
      verify(nettyOptions).afterChannelInitialized(channel);
    } finally {
      channel.finishAndReleaseAll();
    }
  }
}
