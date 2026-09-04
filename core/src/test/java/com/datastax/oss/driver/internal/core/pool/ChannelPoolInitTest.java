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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.pool;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ClusterNameMismatchException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.MockChannelFactoryHelper;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.slf4j.LoggerFactory;

public class ChannelPoolInitTest extends ChannelPoolTestBase {

  @Mock private Appender<ILoggingEvent> appender;

  @Test
  public void should_initialize_when_all_channels_succeed() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .success(node, channel1)
            .success(node, channel2)
            .success(node, channel3)
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 3);

    assertThatStage(poolFuture)
        .isSuccess(pool -> assertThat(pool.channels[0]).containsOnly(channel1, channel2, channel3));
    verify(eventBus, VERIFY_TIMEOUT.times(3)).fire(ChannelEvent.channelOpened(node));

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_initialize_when_all_channels_fail() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node, "mock channel init failure")
            .failure(node, "mock channel init failure")
            .failure(node, "mock channel init failure")
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess(pool -> assertThat(pool.channels).isNull());
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node));
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);

    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_count_both_metrics_when_a_node_fails_on_auth_and_transport() {
    // One endpoint, several addresses: ChannelFactory reports a single failure with the others
    // attached as suppressed, and it promotes the authentication failure over the transport ones.
    // isAuthOnly() is false for that mix, which is right for errors.connection.init -- most of the
    // node really is unreachable -- but routing it there *alone* would make
    // errors.connection.auth unreachable for any node whose endpoint is a name (SNI/cloud proxy,
    // a client route, a translator with resolve-addresses = false), because this method is the
    // driver's only writer of that counter. An operator watching it would see zero while every
    // connect failed on credentials. Both happened, so both are counted.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    AuthenticationException authError =
        new AuthenticationException(node.getEndPoint(), "mock auth failure");
    authError.addSuppressed(new Exception("mock connection refused"));

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).failure(node, authError).build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess();
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null);
  }

  @Test
  public void should_count_only_the_auth_metric_when_every_address_fails_on_auth() {
    // The other side of the same decision: nothing but authentication failed, so
    // errors.connection.init must stay untouched.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    AuthenticationException authError =
        new AuthenticationException(node.getEndPoint(), "mock auth failure");
    authError.addSuppressed(new AuthenticationException(node.getEndPoint(), "mock auth failure"));

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).failure(node, authError).build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess();
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null);
    verify(nodeMetricUpdater, never())
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
  }

  @Test
  public void should_count_the_auth_metric_when_a_keyspace_failure_is_what_surfaced() {
    // The same mix, but with a third kind of failure in it -- and ChannelFactory#surfacedFailure
    // ranks an invalid keyspace above an authentication one, so *that* is what arrives here with
    // the auth failure attached as suppressed. A node whose endpoint is a name expanding to two
    // addresses: one rejects the credentials, the other answers but has no such keyspace.
    //
    // Testing the type of what arrived would leave errors.connection.auth at zero here, which is
    // the very blind spot the branch above exists to close: this method is the driver's only
    // writer of that counter.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    InvalidKeyspaceException surfaced = new InvalidKeyspaceException("invalid keyspace");
    surfaced.addSuppressed(new AuthenticationException(node.getEndPoint(), "mock auth failure"));

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).failure(node, surfaced).build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess();
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null);
  }

  @Test
  public void should_warn_about_credentials_when_a_fatal_failure_is_what_surfaced() {
    // The failure the pool has to *act* on and the failure the operator has to *hear* about are
    // not always the same one, and the routing used to decide both. A node whose endpoint is a name
    // expanding to two addresses: one rejects the credentials, the other rejects the protocol
    // version -- which, for an identified node, ChannelFactory#surfacedFailure treats as node-wide
    // and promotes. Acting on it is right: forceDown, and nothing in the driver reverses that. But
    // it used to be the whole story, so the operator got a protocol-version message, a climbing
    // errors.connection.auth counter, and no line at any level about the login that was refused --
    // reachable only by reading getSuppressed() off the logged throwable.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    UnsupportedProtocolVersionException surfaced =
        UnsupportedProtocolVersionException.forSingleAttempt(
            node.getEndPoint(), DefaultProtocolVersion.V4);
    surfaced.addSuppressed(new AuthenticationException(node.getEndPoint(), "mock auth failure"));

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory).failure(node, surfaced).build();

    Logger logger = (Logger) LoggerFactory.getLogger(ChannelPool.class);
    Level levelBefore = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);
    try {
      ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");
      factoryHelper.waitForCalls(node, 1);

      // Still acted on as before.
      verify(eventBus, VERIFY_TIMEOUT)
          .fire(TopologyEvent.forceDown(node.getBroadcastRpcAddress().get()));
      verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
          .incrementCounter(DefaultNodeMetric.AUTHENTICATION_ERRORS, null);

      // And now also reported.
      ArgumentCaptor<ILoggingEvent> logs = ArgumentCaptor.forClass(ILoggingEvent.class);
      verify(appender, VERIFY_TIMEOUT.atLeastOnce()).doAppend(logs.capture());
      assertThat(logs.getAllValues())
          .anySatisfy(
              event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getFormattedMessage())
                    .contains("authentication failed on some of the node's addresses");
              });
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(levelBefore);
    }
  }

  @Test
  public void should_indicate_when_keyspace_failed_on_all_channels() {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node, new InvalidKeyspaceException("invalid keyspace"))
            .failure(node, new InvalidKeyspaceException("invalid keyspace"))
            .failure(node, new InvalidKeyspaceException("invalid keyspace"))
            .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);
    assertThatStage(poolFuture)
        .isSuccess(
            pool -> {
              assertThat(pool.isInvalidKeyspace()).isTrue();
              verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
                  .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
            });
  }

  @Test
  public void should_fire_force_down_event_when_cluster_name_does_not_match() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(3);

    ClusterNameMismatchException error =
        new ClusterNameMismatchException(node.getEndPoint(), "actual", "expected");
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            .failure(node, error)
            .failure(node, error)
            .failure(node, error)
            .build();

    ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    verify(eventBus, VERIFY_TIMEOUT)
        .fire(TopologyEvent.forceDown(node.getBroadcastRpcAddress().get()));
    verify(eventBus, never()).fire(ChannelEvent.channelOpened(node));

    verify(nodeMetricUpdater, VERIFY_TIMEOUT.times(1))
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_return_zero_for_metric_accessors_when_pool_uninitialized() throws Exception {
    // Reproduce CUSTOMER-413: when the initial connection attempt fails, connectFuture completes
    // with channels == null (initialize() is only called on the success path). Before the fix,
    // any call to size/getAvailableIds/getInFlight/getOrphanedIds threw NullPointerException via
    // Arrays.stream(null), which propagated through the Dropwizard Metrics gauge lambdas
    // registered in DropwizardNodeMetricUpdater.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    MockChannelFactoryHelper.builder(channelFactory)
        .failure(node, "mock channel init failure")
        .build();

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Confirm the precondition: pool entered the map but channels was never initialized
    assertThat(pool.channels).isNull();

    // All four accessor methods must return 0 without throwing
    assertThat(pool.size()).isEqualTo(0);
    assertThat(pool.getAvailableIds()).isEqualTo(0);
    assertThat(pool.getInFlight()).isEqualTo(0);
    assertThat(pool.getOrphanedIds()).isEqualTo(0);
  }

  @Test
  public void should_reconnect_when_init_incomplete() throws Exception {
    // Short delay so we don't have to wait in the test
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofNanos(1));

    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    CompletableFuture<DriverChannel> channel2Future = new CompletableFuture<>();
    MockChannelFactoryHelper factoryHelper =
        MockChannelFactoryHelper.builder(channelFactory)
            // Init: 1 channel fails, the other succeeds
            .failure(node, "mock channel init failure")
            .success(node, channel1)
            // 1st reconnection
            .pending(node, channel2Future)
            .build();
    InOrder inOrder = inOrder(eventBus);

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    factoryHelper.waitForCalls(node, 1);

    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // A reconnection should have been scheduled
    verify(reconnectionSchedule, VERIFY_TIMEOUT).nextDelay();
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStarted(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));
    assertThat(pool.channels[0]).containsOnly(channel1);

    channel2Future.complete(channel2);
    factoryHelper.waitForCalls(node, 1);
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.channelOpened(node));
    inOrder.verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));

    await().untilAsserted(() -> assertThat(pool.channels[0]).containsOnly(channel1, channel2));

    verify(nodeMetricUpdater, VERIFY_TIMEOUT)
        .incrementCounter(DefaultNodeMetric.CONNECTION_INIT_ERRORS, null);
    factoryHelper.waitForCalls(node, 1);
  }
}
