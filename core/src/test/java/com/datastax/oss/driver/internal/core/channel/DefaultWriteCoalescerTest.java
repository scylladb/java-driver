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

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultWriteCoalescerTest {

  @Mock private DriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile executionProfile;
  @Mock private Channel channel;
  @Mock private EventLoop eventLoop;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(executionProfile);
    when(executionProfile.getDuration(DefaultDriverOption.COALESCER_INTERVAL))
        .thenReturn(Duration.ZERO);
    when(channel.eventLoop()).thenReturn(eventLoop);
    when(eventLoop.inEventLoop()).thenReturn(true);
  }

  @Test
  public void should_fail_concurrent_writes_and_allow_later_enqueue_if_initial_scheduling_fails() {
    DefaultWriteCoalescer coalescer = new DefaultWriteCoalescer(context);
    Object rejectedMessage = new Object();
    StreamIdGenerator streamIds = new StreamIdGenerator(1);
    assertThat(streamIds.preAcquire()).isTrue();
    DriverChannel.RequestMessage queuedMessage = newRequestMessage(streamIds);
    Object laterMessage = new Object();
    AtomicReference<ChannelFuture> queuedFuture = new AtomicReference<>();
    RejectedExecutionException failure = new RejectedExecutionException("mock failure");

    doAnswer(
            invocation -> {
              // Simulate another caller enqueueing while the first scheduling attempt owns the
              // running flag.
              queuedFuture.set(coalescer.writeAndFlush(channel, queuedMessage));
              throw failure;
            })
        .doAnswer(
            invocation -> {
              invocation.getArgument(0, Runnable.class).run();
              return null;
            })
        .when(eventLoop)
        .execute(any(Runnable.class));

    assertThatThrownBy(() -> coalescer.writeAndFlush(channel, rejectedMessage))
        .isInstanceOf(ClosedConnectionException.class)
        .hasCause(failure);

    verify(channel, never()).write(any(), any(ChannelPromise.class));
    assertThat(queuedFuture.get())
        .isFailed(
            error ->
                assertThat(error).isInstanceOf(ClosedConnectionException.class).hasCause(failure));
    assertThat(streamIds.getAvailableIds()).isEqualTo(1);

    // Listener notification must not depend on the same event loop that rejected the write task.
    when(eventLoop.inEventLoop()).thenReturn(false);
    AtomicReference<Throwable> listenerFailure = new AtomicReference<>();
    queuedFuture.get().addListener(future -> listenerFailure.set(future.cause()));
    assertThat(listenerFailure.get())
        .isInstanceOf(ClosedConnectionException.class)
        .hasCause(failure);

    // The failed drain releases the running flag, so a later write can schedule normally.
    when(eventLoop.inEventLoop()).thenReturn(true);
    coalescer.writeAndFlush(channel, laterMessage);
    verify(channel).write(eq(laterMessage), any(ChannelPromise.class));
    verify(channel).flush();
  }

  @Test
  public void should_fail_concurrently_enqueued_writes_when_event_loop_is_shutting_down() {
    DefaultWriteCoalescer coalescer = new DefaultWriteCoalescer(context);
    StreamIdGenerator streamIds = new StreamIdGenerator(1);
    assertThat(streamIds.preAcquire()).isTrue();
    DriverChannel.RequestMessage queuedMessage = newRequestMessage(streamIds);
    AtomicReference<ChannelFuture> queuedFuture = new AtomicReference<>();

    doAnswer(
            invocation -> {
              invocation.getArgument(0, Runnable.class).run();
              return null;
            })
        .when(eventLoop)
        .execute(any(Runnable.class));
    doAnswer(
            invocation -> {
              queuedFuture.set(coalescer.writeAndFlush(channel, queuedMessage));
              return null;
            })
        .when(channel)
        .flush();
    when(eventLoop.isShuttingDown()).thenReturn(true);

    coalescer.writeAndFlush(channel, new Object());

    assertThat(queuedFuture.get())
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(ClosedConnectionException.class)
                    .hasCauseInstanceOf(RejectedExecutionException.class));
    assertThat(streamIds.getAvailableIds()).isEqualTo(1);
  }

  @Test
  public void should_fail_concurrently_enqueued_writes_when_rescheduling_is_rejected() {
    DefaultWriteCoalescer coalescer = new DefaultWriteCoalescer(context);
    StreamIdGenerator streamIds = new StreamIdGenerator(1);
    assertThat(streamIds.preAcquire()).isTrue();
    DriverChannel.RequestMessage queuedMessage = newRequestMessage(streamIds);
    AtomicReference<ChannelFuture> queuedFuture = new AtomicReference<>();
    RejectedExecutionException failure = new RejectedExecutionException("mock failure");

    doAnswer(
            invocation -> {
              invocation.getArgument(0, Runnable.class).run();
              return null;
            })
        .when(eventLoop)
        .execute(any(Runnable.class));
    doAnswer(
            invocation -> {
              queuedFuture.set(coalescer.writeAndFlush(channel, queuedMessage));
              return null;
            })
        .when(channel)
        .flush();
    when(eventLoop.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.NANOSECONDS)))
        .thenThrow(failure);

    coalescer.writeAndFlush(channel, new Object());

    assertThat(queuedFuture.get())
        .isFailed(
            error ->
                assertThat(error).isInstanceOf(ClosedConnectionException.class).hasCause(failure));
    assertThat(streamIds.getAvailableIds()).isEqualTo(1);
  }

  @Test
  public void should_release_pre_acquired_id_when_channel_write_throws() {
    DefaultWriteCoalescer coalescer = new DefaultWriteCoalescer(context);
    StreamIdGenerator streamIds = new StreamIdGenerator(1);
    assertThat(streamIds.preAcquire()).isTrue();
    DriverChannel.RequestMessage message = newRequestMessage(streamIds);
    RuntimeException failure = new RuntimeException("mock failure");

    doAnswer(
            invocation -> {
              invocation.getArgument(0, Runnable.class).run();
              return null;
            })
        .when(eventLoop)
        .execute(any(Runnable.class));
    doThrow(failure).when(channel).write(eq(message), any(ChannelPromise.class));

    ChannelFuture writeFuture = coalescer.writeAndFlush(channel, message);

    assertThat(writeFuture).isFailed(error -> assertThat(error).isSameAs(failure));
    assertThat(streamIds.getAvailableIds()).isEqualTo(1);
  }

  private DriverChannel.RequestMessage newRequestMessage(StreamIdGenerator streamIds) {
    InFlightHandler inFlightHandler =
        new InFlightHandler(
            DefaultProtocolVersion.V3,
            streamIds,
            Integer.MAX_VALUE,
            0,
            mock(ChannelPromise.class),
            null,
            "test");
    return new DriverChannel.RequestMessage(
        new Query("mock query"),
        false,
        Frame.NO_PAYLOAD,
        mock(ResponseCallback.class),
        inFlightHandler);
  }
}
