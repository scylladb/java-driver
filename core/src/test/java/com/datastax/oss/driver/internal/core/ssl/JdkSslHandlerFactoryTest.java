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
package com.datastax.oss.driver.internal.core.ssl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCounted;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.junit.After;
import org.junit.Test;

public class JdkSslHandlerFactoryTest {
  private final EndPoint endpoint =
      new DefaultEndPoint(InetSocketAddress.createUnresolved("server.example", 9042));
  private final SslEngineFactory engineFactory = mock(SslEngineFactory.class);
  private JdkSslHandlerFactory factory;

  @After
  public void cleanup() throws Exception {
    if (factory != null) {
      factory.close();
    }
  }

  @Test
  public void should_prepare_on_dedicated_daemon_thread_and_transfer_engine_to_handler()
      throws Exception {
    SSLEngine engine = SSLContext.getDefault().createSSLEngine("server.example", 9042);
    engine.setUseClientMode(true);
    AtomicReference<Thread> preparationThread = new AtomicReference<>();
    when(engineFactory.newSslEngine(endpoint))
        .thenAnswer(
            invocation -> {
              preparationThread.set(Thread.currentThread());
              return engine;
            });
    factory = new JdkSslHandlerFactory(engineFactory);

    SslHandlerFactory prepared =
        factory.prepare(endpoint).toCompletableFuture().get(5, TimeUnit.SECONDS);
    SslHandler handler = prepared.newSslHandler(null, endpoint);
    prepared.close();

    assertThat(preparationThread.get()).isNotSameAs(Thread.currentThread());
    assertThat(preparationThread.get().isDaemon()).isTrue();
    assertThat(preparationThread.get().getName()).startsWith("driver-ssl-preparation-");
    assertThat(handler.engine()).isSameAs(engine);
    assertThat(engine.isOutboundDone()).isFalse();
    assertThat(factory.getSslEngineFactory()).isSameAs(engineFactory);
    assertThatThrownBy(() -> prepared.newSslHandler(null, endpoint))
        .isInstanceOf(IllegalStateException.class);
    engine.closeOutbound();
  }

  @Test
  public void should_preserve_reverse_lookup_and_hostname_validation_when_enabled()
      throws Exception {
    assertDefaultEngineConfiguration(true, true);
  }

  @Test
  public void should_preserve_ip_peer_and_disabled_hostname_validation() throws Exception {
    assertDefaultEngineConfiguration(false, false);
  }

  private void assertDefaultEngineConfiguration(boolean reverseLookup, boolean hostnameValidation)
      throws Exception {
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverContext context = mock(DriverContext.class);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getBoolean(DefaultDriverOption.SSL_ALLOW_DNS_REVERSE_LOOKUP_SAN, true))
        .thenReturn(reverseLookup);
    when(profile.getBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true))
        .thenReturn(hostnameValidation);
    String cipher = SSLContext.getDefault().getSupportedSSLParameters().getCipherSuites()[0];
    when(profile.isDefined(DefaultDriverOption.SSL_CIPHER_SUITES)).thenReturn(true);
    when(profile.getStringList(DefaultDriverOption.SSL_CIPHER_SUITES))
        .thenReturn(Collections.singletonList(cipher));
    AtomicReference<Thread> lookupThread = new AtomicReference<>();
    DefaultSslEngineFactory defaultFactory =
        new DefaultSslEngineFactory(context) {
          @Override
          protected String hostMaybeFromDnsReverseLookup(InetSocketAddress address) {
            lookupThread.set(Thread.currentThread());
            return "reverse.example";
          }
        };
    EndPoint ipEndpoint =
        new DefaultEndPoint(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 2}), 9142));
    factory = new JdkSslHandlerFactory(defaultFactory);

    SslHandlerFactory prepared =
        factory.prepare(ipEndpoint).toCompletableFuture().get(5, TimeUnit.SECONDS);
    SSLEngine engine = prepared.newSslHandler(null, ipEndpoint).engine();
    prepared.close();
    try {
      assertThat(engine.getPeerHost()).isEqualTo(reverseLookup ? "reverse.example" : "127.0.0.2");
      assertThat(engine.getPeerPort()).isEqualTo(9142);
      assertThat(engine.getUseClientMode()).isTrue();
      assertThat(engine.getEnabledCipherSuites()).containsExactly(cipher);
      assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm())
          .isEqualTo(hostnameValidation ? "HTTPS" : null);
      if (reverseLookup) {
        assertThat(lookupThread.get()).isNotSameAs(Thread.currentThread());
        assertThat(lookupThread.get().getName()).startsWith("driver-ssl-preparation-");
      } else {
        assertThat(lookupThread.get()).isNull();
      }
    } finally {
      engine.closeOutbound();
    }
  }

  @Test
  public void should_propagate_preparation_failure() {
    IllegalArgumentException failure = new IllegalArgumentException("engine creation failed");
    when(engineFactory.newSslEngine(endpoint)).thenThrow(failure);
    factory = new JdkSslHandlerFactory(engineFactory);

    assertThatThrownBy(
            () -> factory.prepare(endpoint).toCompletableFuture().get(5, TimeUnit.SECONDS))
        .hasCause(failure);
  }

  @Test
  public void should_close_unused_prepared_engine_only_once() throws Exception {
    SSLEngine engine =
        mock(SSLEngine.class, withSettings().extraInterfaces(ReferenceCounted.class));
    when(engineFactory.newSslEngine(endpoint)).thenReturn(engine);
    factory = new JdkSslHandlerFactory(engineFactory);

    SslHandlerFactory prepared =
        factory.prepare(endpoint).toCompletableFuture().get(5, TimeUnit.SECONDS);
    prepared.close();
    prepared.close();

    verify(engine).closeOutbound();
    verify((ReferenceCounted) engine).release();
    verify(engineFactory, never()).close();
    assertThatThrownBy(() -> prepared.newSslHandler(null, endpoint))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void should_remove_cancelled_queued_preparation_and_release_late_engine()
      throws Exception {
    ThreadPoolExecutor executor = newSingleWorkerExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    SSLEngine engine = mock(SSLEngine.class);
    when(engineFactory.newSslEngine(endpoint))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              awaitUninterruptibly(release);
              return engine;
            });
    factory = new JdkSslHandlerFactory(engineFactory, executor);
    try {
      CompletableFuture<SslHandlerFactory> running =
          factory.prepare(endpoint).toCompletableFuture();
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      CompletableFuture<SslHandlerFactory> queued = factory.prepare(endpoint).toCompletableFuture();
      assertThat(executor.getQueue()).hasSize(1);

      assertThat(queued.cancel(true)).isTrue();
      assertThat(executor.getQueue()).isEmpty();
      assertThat(running.cancel(true)).isTrue();
      release.countDown();

      await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(engine).closeOutbound());
      assertThat(running.isCancelled()).isTrue();
      verify(engineFactory).newSslEngine(endpoint);
    } finally {
      release.countDown();
    }
  }

  @Test(timeout = 10000)
  public void should_fail_running_and_queued_preparations_without_waiting_for_blocked_lookup()
      throws Exception {
    ThreadPoolExecutor executor = newSingleWorkerExecutor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    SSLEngine engine = mock(SSLEngine.class);
    when(engineFactory.newSslEngine(endpoint))
        .thenAnswer(
            invocation -> {
              entered.countDown();
              awaitUninterruptibly(release);
              return engine;
            });
    factory = new JdkSslHandlerFactory(engineFactory, executor);
    try {
      CompletableFuture<SslHandlerFactory> running =
          factory.prepare(endpoint).toCompletableFuture();
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      CompletableFuture<SslHandlerFactory> queued = factory.prepare(endpoint).toCompletableFuture();

      factory.close();

      assertThatThrownBy(running::get).hasCauseInstanceOf(IllegalStateException.class);
      assertThatThrownBy(queued::get).hasCauseInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> factory.prepare(endpoint).toCompletableFuture().get())
          .hasCauseInstanceOf(IllegalStateException.class);
      assertThat(executor.isShutdown()).isTrue();
      assertThat(executor.getQueue()).isEmpty();
      verify(engineFactory).close();
      release.countDown();
      await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> verify(engine).closeOutbound());
      verify(engineFactory).newSslEngine(endpoint);
    } finally {
      release.countDown();
    }
  }

  @Test(timeout = 10000)
  public void should_not_hold_factory_lock_while_running_completion_callbacks() throws Exception {
    CountDownLatch engineCreationStarted = new CountDownLatch(1);
    CountDownLatch releaseEngineCreation = new CountDownLatch(1);
    CountDownLatch callbackStarted = new CountDownLatch(1);
    CountDownLatch releaseCallback = new CountDownLatch(1);
    CountDownLatch observerFinished = new CountDownLatch(1);
    AtomicReference<Boolean> observerHeldFactoryLock = new AtomicReference<>();
    SSLEngine engine = mock(SSLEngine.class);
    when(engineFactory.newSslEngine(endpoint))
        .thenAnswer(
            invocation -> {
              engineCreationStarted.countDown();
              awaitUninterruptibly(releaseEngineCreation);
              return engine;
            });
    factory = new JdkSslHandlerFactory(engineFactory);
    try {
      CompletableFuture<SslHandlerFactory> preparation =
          factory.prepare(endpoint).toCompletableFuture();
      preparation.whenComplete(
          (prepared, error) -> {
            observerHeldFactoryLock.set(Thread.holdsLock(factory));
            observerFinished.countDown();
          });
      preparation.whenComplete(
          (prepared, error) -> {
            callbackStarted.countDown();
            awaitUninterruptibly(releaseCallback);
          });
      assertThat(engineCreationStarted.await(5, TimeUnit.SECONDS)).isTrue();
      releaseEngineCreation.countDown();
      assertThat(callbackStarted.await(5, TimeUnit.SECONDS)).isTrue();

      CompletableFuture<Void> closing =
          CompletableFuture.runAsync(
              () -> {
                try {
                  factory.close();
                } catch (Exception error) {
                  throw new RuntimeException(error);
                }
              });
      closing.get(5, TimeUnit.SECONDS);
      releaseCallback.countDown();
      assertThat(observerFinished.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(observerHeldFactoryLock.get()).isFalse();
      preparation.get(5, TimeUnit.SECONDS).close();
    } finally {
      releaseEngineCreation.countDown();
      releaseCallback.countDown();
    }
  }

  @Test
  public void should_fail_preparation_if_executor_rejects_task() {
    ThreadPoolExecutor executor = newSingleWorkerExecutor();
    executor.shutdownNow();
    factory = new JdkSslHandlerFactory(engineFactory, executor);

    assertThatThrownBy(
            () -> factory.prepare(endpoint).toCompletableFuture().get(5, TimeUnit.SECONDS))
        .hasCauseInstanceOf(RejectedExecutionException.class);
  }

  @Test
  public void should_preserve_subclass_handler_override() throws Exception {
    SslHandler customHandler = mock(SslHandler.class);
    factory =
        new JdkSslHandlerFactory(engineFactory) {
          @Override
          public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
            return customHandler;
          }
        };

    SslHandlerFactory prepared = factory.prepare(endpoint).toCompletableFuture().get();

    assertThat(prepared).isSameAs(factory);
    assertThat(prepared.newSslHandler(null, endpoint)).isSameAs(customHandler);
    verify(engineFactory, never()).newSslEngine(endpoint);
  }

  @Test
  public void should_preserve_default_low_level_factory_preparation() throws Exception {
    SslHandler customHandler = mock(SslHandler.class);
    SslHandlerFactory customFactory =
        new SslHandlerFactory() {
          @Override
          public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
            return customHandler;
          }

          @Override
          public void close() {}
        };

    assertThat(customFactory.prepare(endpoint).toCompletableFuture().get()).isSameAs(customFactory);
  }

  private ThreadPoolExecutor newSingleWorkerExecutor() {
    return new ThreadPoolExecutor(
        1,
        1,
        0,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(),
        task -> {
          Thread thread = new Thread(task, "ssl-preparation-test");
          thread.setDaemon(true);
          return thread;
        });
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException error) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
