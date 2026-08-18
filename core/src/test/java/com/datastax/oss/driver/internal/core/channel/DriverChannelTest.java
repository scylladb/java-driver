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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.Void;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DriverChannelTest extends ChannelHandlerTestBase {
  public static final int SET_KEYSPACE_TIMEOUT_MILLIS = 100;

  private DriverChannel driverChannel;
  private MockWriteCoalescer writeCoalescer;

  @Mock private StreamIdGenerator streamIds;

  @Before
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
    channel
        .pipeline()
        .addLast(
            new InFlightHandler(
                DefaultProtocolVersion.V3,
                streamIds,
                Integer.MAX_VALUE,
                SET_KEYSPACE_TIMEOUT_MILLIS,
                channel.newPromise(),
                null,
                "test"));
    writeCoalescer = new MockWriteCoalescer();
    driverChannel =
        new DriverChannel(
            new EmbeddedEndPoint(), channel, writeCoalescer, DefaultProtocolVersion.V3);
  }

  /**
   * Ensures that the potential delay introduced by the write coalescer does not mess with the
   * graceful shutdown sequence: any write submitted before {@link DriverChannel#close()} is
   * guaranteed to complete.
   */
  @Test
  public void should_wait_for_coalesced_writes_when_closing_gracefully() {
    // Given
    MockResponseCallback responseCallback = new MockResponseCallback();
    driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
    // nothing written yet because the coalescer hasn't flushed
    assertNoOutboundFrame();

    // When
    Future<java.lang.Void> closeFuture = driverChannel.close();

    // Then
    // not closed yet because there is still a pending write
    assertThat(closeFuture).isNotDone();
    assertNoOutboundFrame();

    // When
    // the coalescer finally runs
    writeCoalescer.triggerFlush();

    // Then
    // the pending write goes through
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame).isNotNull();
    // not closed yet because there is now a pending response
    assertThat(closeFuture).isNotDone();

    // When
    // the pending response arrives
    writeInboundFrame(requestFrame, Void.INSTANCE);
    assertThat(responseCallback.getLastResponse().message).isEqualTo(Void.INSTANCE);

    // Then
    assertThat(closeFuture).isSuccess();
  }

  /**
   * Ensures that the potential delay introduced by the write coalescer does not mess with the
   * forceful shutdown sequence: any write submitted before {@link DriverChannel#forceClose()}
   * should get the "Channel was force-closed" error, whether it had been flushed or not.
   */
  @Test
  public void should_wait_for_coalesced_writes_when_closing_forcefully() {
    // Given
    MockResponseCallback responseCallback = new MockResponseCallback();
    driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
    // nothing written yet because the coalescer hasn't flushed
    assertNoOutboundFrame();

    // When
    Future<java.lang.Void> closeFuture = driverChannel.forceClose();

    // Then
    // not closed yet because there is still a pending write
    assertThat(closeFuture).isNotDone();
    assertNoOutboundFrame();

    // When
    // the coalescer finally runs
    writeCoalescer.triggerFlush();
    // and the pending write goes through
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame).isNotNull();

    // Then
    assertThat(closeFuture).isSuccess();
    assertThat(responseCallback.getFailure())
        .isInstanceOf(ClosedConnectionException.class)
        .hasMessageContaining("Channel was force-closed");
  }

  @Test
  public void should_cancel_pre_acquired_id_when_write_is_rejected_before_submission() {
    // Given
    when(streamIds.preAcquire()).thenReturn(true);
    assertThat(driverChannel.preAcquireId()).isTrue();
    driverChannel.close();

    // When
    Future<java.lang.Void> writeFuture =
        driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, new MockResponseCallback());

    // Then
    assertThat(writeFuture).isFailed();
    verify(streamIds).cancelPreAcquire();
  }

  @Test
  public void should_cancel_pre_acquired_id_when_coalesced_write_throws() {
    // Given
    RuntimeException failure = new RuntimeException("mock failure");
    driverChannel =
        new DriverChannel(
            new EmbeddedEndPoint(),
            channel,
            (channel, message) -> {
              throw failure;
            },
            DefaultProtocolVersion.V3);
    when(streamIds.preAcquire()).thenReturn(true);
    assertThat(driverChannel.preAcquireId()).isTrue();

    // When
    Future<java.lang.Void> writeFuture =
        driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, new MockResponseCallback());

    // Then
    assertThat(writeFuture).isFailed(error -> assertThat(error).isSameAs(failure));
    verify(streamIds).cancelPreAcquire();
  }

  @Test
  public void should_cancel_pre_acquired_id_when_coalesced_write_throws_error() {
    // Given
    AssertionError failure = new AssertionError("mock failure");
    driverChannel =
        new DriverChannel(
            new EmbeddedEndPoint(),
            channel,
            (channel, message) -> {
              throw failure;
            },
            DefaultProtocolVersion.V3);
    when(streamIds.preAcquire()).thenReturn(true);
    assertThat(driverChannel.preAcquireId()).isTrue();

    // When
    Future<java.lang.Void> writeFuture =
        driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, new MockResponseCallback());

    // Then
    assertThat(writeFuture).isFailed(error -> assertThat(error).isSameAs(failure));
    verify(streamIds).cancelPreAcquire();
  }

  @Test
  public void should_cancel_pre_acquired_id_when_coalesced_write_fails_asynchronously() {
    // Given
    RuntimeException failure = new RuntimeException("mock failure");
    when(streamIds.preAcquire()).thenReturn(true);
    assertThat(driverChannel.preAcquireId()).isTrue();

    // When
    Future<java.lang.Void> writeFuture =
        driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, new MockResponseCallback());
    writeCoalescer.failWrites(failure);

    // Then
    assertThat(writeFuture).isFailed(error -> assertThat(error).isSameAs(failure));
    verify(streamIds).cancelPreAcquire();
  }

  @Test
  public void should_cancel_pre_acquired_id_when_custom_coalescer_listener_is_rejected() {
    // Given
    RuntimeException failure = new RuntimeException("mock failure");
    EventExecutor rejectingExecutor = mock(EventExecutor.class);
    when(rejectingExecutor.inEventLoop()).thenReturn(false);
    doThrow(new RejectedExecutionException()).when(rejectingExecutor).execute(any(Runnable.class));
    driverChannel =
        new DriverChannel(
            new EmbeddedEndPoint(),
            channel,
            (channel, message) ->
                new DefaultChannelPromise(channel, rejectingExecutor).setFailure(failure),
            DefaultProtocolVersion.V3);
    when(streamIds.preAcquire()).thenReturn(true);
    assertThat(driverChannel.preAcquireId()).isTrue();

    // When
    Future<java.lang.Void> writeFuture =
        driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, new MockResponseCallback());

    // Then
    assertThat(writeFuture).isFailed(error -> assertThat(error).isSameAs(failure));
    verify(streamIds).cancelPreAcquire();
  }

  // Simple implementation that holds all the writes, and flushes them when it's explicitly
  // triggered.
  private class MockWriteCoalescer implements WriteCoalescer {
    private Queue<Map.Entry<Object, ChannelPromise>> messages = new ArrayDeque<>();

    @Override
    public ChannelFuture writeAndFlush(Channel channel, Object message) {
      assertThat(channel).isEqualTo(DriverChannelTest.this.channel);
      ChannelPromise writePromise = channel.newPromise();
      messages.offer(new AbstractMap.SimpleEntry<>(message, writePromise));
      return writePromise;
    }

    void triggerFlush() {
      for (Map.Entry<Object, ChannelPromise> entry : messages) {
        channel.writeAndFlush(entry.getKey(), entry.getValue());
      }
    }

    void failWrites(Throwable failure) {
      for (Map.Entry<Object, ChannelPromise> entry : messages) {
        entry.getValue().tryFailure(failure);
      }
    }
  }
}
