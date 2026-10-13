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
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopSessionMetricUpdater;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.ReferenceCountUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChannelFactorySslTest {
  private final InternalDriverContext context = mock(InternalDriverContext.class);
  private final DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
  private final NettyOptions nettyOptions = mock(NettyOptions.class);
  private final List<Channel> channels = new ArrayList<>();
  private final CompletableFuture<SslHandler> initialized = new CompletableFuture<>();
  private final AtomicInteger bootstrapCalls = new AtomicInteger();

  private NioEventLoopGroup ioGroup;
  private NioEventLoopGroup serverGroup;
  private HashedWheelTimer timer;
  private EndPoint endPoint;
  private SslHandlerFactory sslHandlerFactory;
  private BlockingSslEngineFactory blockingEngineFactory;
  private String cipher;

  @Before
  public void setup() throws Exception {
    ioGroup = new NioEventLoopGroup(1);
    serverGroup = new NioEventLoopGroup(1);
    timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);
    endPoint =
        new DefaultEndPoint(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 9042));
    cipher = SSLContext.getDefault().getDefaultSSLParameters().getCipherSuites()[0];

    DriverConfig config = mock(DriverConfig.class);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(profile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(profile.getDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(profile.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL))
        .thenReturn(Duration.ofSeconds(30));
    when(profile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS)).thenReturn(1024);
    when(profile.getInt(DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS)).thenReturn(256);
    when(profile.getBytes(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH)).thenReturn(256L << 20);
    when(profile.getBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true)).thenReturn(true);
    when(profile.getBoolean(DefaultDriverOption.SSL_ALLOW_DNS_REVERSE_LOOKUP_SAN, true))
        .thenReturn(true);
    when(profile.isDefined(DefaultDriverOption.SSL_CIPHER_SUITES)).thenReturn(true);
    when(profile.getStringList(DefaultDriverOption.SSL_CIPHER_SUITES))
        .thenReturn(Collections.singletonList(cipher));

    ProtocolVersionRegistry versions = mock(ProtocolVersionRegistry.class);
    when(context.getProtocolVersionRegistry()).thenReturn(versions);
    when(versions.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    MetricsFactory metrics = mock(MetricsFactory.class);
    when(context.getMetricsFactory()).thenReturn(metrics);
    when(metrics.getSessionUpdater()).thenReturn(NoopSessionMetricUpdater.INSTANCE);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.ioEventLoopGroup()).thenReturn(ioGroup);
    when(nettyOptions.channelClass()).thenAnswer(invocation -> NioSocketChannel.class);
    when(nettyOptions.allocator()).thenReturn(ByteBufAllocator.DEFAULT);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(context.getFrameCodec())
        .thenReturn(
            FrameCodec.defaultClient(
                new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), Compressor.none()));
    doAnswer(
            invocation -> {
              bootstrapCalls.incrementAndGet();
              return null;
            })
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    doAnswer(
            invocation -> {
              Channel channel = invocation.getArgument(0);
              initialized.complete(channel.pipeline().get(SslHandler.class));
              // This test exercises the production pipeline up to SSL handler installation. No
              // TLS server or CQL handshake is needed to inspect the prepared engine.
              channel.close();
              return null;
            })
        .when(nettyOptions)
        .afterChannelInitialized(any(Channel.class));
  }

  @After
  public void teardown() throws Exception {
    if (blockingEngineFactory != null) {
      blockingEngineFactory.releaseLookup.countDown();
    }
    if (sslHandlerFactory != null) {
      sslHandlerFactory.close();
    }
    for (Channel channel : channels) {
      channel.close().syncUninterruptibly();
    }
    ioGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    serverGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
    timer.stop();
  }

  @Test
  public void should_keep_other_connections_readable_during_reverse_dns_and_preserve_ssl_settings()
      throws Exception {
    CompletableFuture<Integer> echoed = new CompletableFuture<>();
    Channel healthyConnection = openEchoConnection(echoed);
    blockingEngineFactory = new BlockingSslEngineFactory();
    install(new JdkSslHandlerFactory(blockingEngineFactory));

    CompletableFuture<DriverChannel> connection = connect();
    assertThat(blockingEngineFactory.lookupStarted.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(connection.isDone()).isFalse();
    assertThat(initialized.isDone()).isFalse();
    assertThat(ioGroup.next().inEventLoop(blockingEngineFactory.lookupThread)).isFalse();
    assertThat(blockingEngineFactory.lookupThread).isNotSameAs(Thread.currentThread());

    // Both scheduled work and inbound TCP bytes use the same single I/O thread that would
    // otherwise have been blocked by engine creation in the channel initializer.
    assertThat(ioGroup.submit(() -> true).get(5, TimeUnit.SECONDS)).isTrue();
    healthyConnection.writeAndFlush(Unpooled.buffer(1).writeByte(42)).sync();
    assertThat(echoed.get(5, TimeUnit.SECONDS)).isEqualTo(42);

    blockingEngineFactory.releaseLookup.countDown();
    SSLEngine engine = initialized.get(5, TimeUnit.SECONDS).engine();
    assertThat(engine.getPeerHost()).isEqualTo("node.example.com");
    assertThat(engine.getPeerPort()).isEqualTo(9042);
    assertThat(engine.getEnabledCipherSuites()).containsExactly(cipher);
    assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
    assertThat(engine.getUseClientMode()).isTrue();
  }

  @Test
  public void should_propagate_engine_preparation_failure_without_opening_a_channel() {
    RuntimeException failure = new IllegalArgumentException("engine preparation failed");
    SslEngineFactory engineFactory = mock(SslEngineFactory.class);
    when(engineFactory.newSslEngine(endPoint)).thenThrow(failure);
    install(new JdkSslHandlerFactory(engineFactory));

    assertThatStage(connect()).isFailed(error -> assertThat(error).isSameAs(failure));
    assertThat(bootstrapCalls).hasValue(0);
    assertThat(initialized.isDone()).isFalse();
  }

  @Test
  public void should_return_a_failed_stage_if_prepare_throws() {
    RuntimeException failure = new IllegalArgumentException("prepare failed");
    SslHandlerFactory factory = mock(SslHandlerFactory.class);
    when(factory.prepare(endPoint)).thenThrow(failure);
    install(factory);

    assertThatStage(connect()).isFailed(error -> assertThat(error).isSameAs(failure));
    assertThat(bootstrapCalls).hasValue(0);
  }

  @Test
  public void should_discard_late_preparation_after_connection_timeout() throws Exception {
    when(profile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT))
        .thenReturn(Duration.ofMillis(100));
    NonCancellingPreparation preparation = installDeferredPreparation();

    assertThatStage(connect())
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(DriverTimeoutException.class)
                    .hasMessageContaining("SSL engine preparation timed out"));
    assertThat(preparation.cancellationRequested).isTrue();
    verifyLatePreparationIsDiscarded(preparation);
  }

  @Test
  public void should_discard_late_preparation_after_connection_cancellation() throws Exception {
    NonCancellingPreparation preparation = installDeferredPreparation();

    CompletableFuture<DriverChannel> connection = connect();
    assertThat(connection.cancel(false)).isTrue();
    assertThat(preparation.cancellationRequested).isTrue();
    verifyLatePreparationIsDiscarded(preparation);
  }

  @Test
  public void should_fail_pending_connection_when_ssl_factory_closes() throws Exception {
    blockingEngineFactory = new BlockingSslEngineFactory();
    install(new JdkSslHandlerFactory(blockingEngineFactory));
    CompletableFuture<DriverChannel> connection = connect();
    assertThat(blockingEngineFactory.lookupStarted.await(5, TimeUnit.SECONDS)).isTrue();

    sslHandlerFactory.close();
    assertThatStage(connection)
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("closed"));
    blockingEngineFactory.releaseLookup.countDown();
    SSLEngine lateEngine = blockingEngineFactory.engineCreated.get(5, TimeUnit.SECONDS);
    await().atMost(5, TimeUnit.SECONDS).until(lateEngine::isOutboundDone);
    assertThat(bootstrapCalls).hasValue(0);
    assertThat(initialized.isDone()).isFalse();
  }

  @Test
  public void should_preserve_io_thread_creation_for_custom_low_level_ssl_factories()
      throws Exception {
    AtomicReference<Thread> handlerThread = new AtomicReference<>();
    SSLEngine engine = SSLContext.getDefault().createSSLEngine();
    engine.setUseClientMode(true);
    SslHandler handler = new SslHandler(engine);
    install(
        new SslHandlerFactory() {
          @Override
          public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
            assertThat(channel.eventLoop().inEventLoop()).isTrue();
            assertThat(remoteEndpoint).isSameAs(endPoint);
            handlerThread.set(Thread.currentThread());
            return handler;
          }

          @Override
          public void close() {}
        });

    connect();
    assertThat(initialized.get(5, TimeUnit.SECONDS)).isSameAs(handler);
    assertThat(ioGroup.next().inEventLoop(handlerThread.get())).isTrue();
  }

  private CompletableFuture<DriverChannel> connect() {
    return new ChannelFactory(context)
        .connect(endPoint, null, null, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE)
        .toCompletableFuture();
  }

  private void install(SslHandlerFactory factory) {
    sslHandlerFactory = factory;
    when(context.getSslHandlerFactory()).thenReturn(Optional.of(factory));
  }

  private NonCancellingPreparation installDeferredPreparation() {
    NonCancellingPreparation preparation = new NonCancellingPreparation();
    SslHandlerFactory factory = mock(SslHandlerFactory.class);
    when(factory.prepare(endPoint)).thenReturn(preparation);
    install(factory);
    return preparation;
  }

  private void verifyLatePreparationIsDiscarded(NonCancellingPreparation preparation)
      throws Exception {
    SslHandlerFactory preparedFactory = mock(SslHandlerFactory.class);
    preparation.complete(preparedFactory);
    verify(preparedFactory).close();
    verify(preparedFactory, never()).newSslHandler(any(Channel.class), any(EndPoint.class));
    verify(sslHandlerFactory, never()).close();
    assertThat(bootstrapCalls).hasValue(0);
    assertThat(initialized.isDone()).isFalse();
  }

  private Channel openEchoConnection(CompletableFuture<Integer> echoed) throws Exception {
    Channel server =
        new ServerBootstrap()
            .group(serverGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<Channel>() {
                  @Override
                  protected void initChannel(Channel channel) {
                    channel
                        .pipeline()
                        .addLast(
                            new ChannelInboundHandlerAdapter() {
                              @Override
                              public void channelRead(ChannelHandlerContext ctx, Object message) {
                                ctx.writeAndFlush(message);
                              }
                            });
                  }
                })
            .bind(InetAddress.getLoopbackAddress(), 0)
            .sync()
            .channel();
    channels.add(server);
    Channel client =
        new Bootstrap()
            .group(ioGroup)
            .channel(NioSocketChannel.class)
            .handler(
                new ChannelInboundHandlerAdapter() {
                  @Override
                  public void channelRead(ChannelHandlerContext ctx, Object message) {
                    try {
                      echoed.complete((int) ((ByteBuf) message).readUnsignedByte());
                    } finally {
                      ReferenceCountUtil.release(message);
                    }
                  }
                })
            .connect(server.localAddress())
            .sync()
            .channel();
    channels.add(client);
    return client;
  }

  private class BlockingSslEngineFactory extends DefaultSslEngineFactory {
    final CountDownLatch lookupStarted = new CountDownLatch(1);
    final CountDownLatch releaseLookup = new CountDownLatch(1);
    final CompletableFuture<SSLEngine> engineCreated = new CompletableFuture<>();
    volatile Thread lookupThread;

    BlockingSslEngineFactory() {
      super(context);
    }

    @Override
    protected String hostMaybeFromDnsReverseLookup(InetSocketAddress address) {
      lookupThread = Thread.currentThread();
      lookupStarted.countDown();
      // Real resolver calls need not respond to interruption either.
      Uninterruptibles.awaitUninterruptibly(releaseLookup);
      return "node.example.com";
    }

    @Override
    public SSLEngine newSslEngine(EndPoint endpoint) {
      SSLEngine engine = super.newSslEngine(endpoint);
      engineCreated.complete(engine);
      return engine;
    }
  }

  private static class NonCancellingPreparation extends CompletableFuture<SslHandlerFactory> {
    volatile boolean cancellationRequested;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancellationRequested = true;
      return false;
    }
  }
}
