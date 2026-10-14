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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolFeatures;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.timeout.IdleStateEvent;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class InFlightHandlerTest extends ChannelHandlerTestBase {
  private static final Query QUERY = new Query("select * from foo");
  private static final int SET_KEYSPACE_TIMEOUT_MILLIS = 100;
  private static final int MAX_ORPHAN_IDS = 10;

  @Mock private StreamIdGenerator streamIds;

  @Before
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
    when(streamIds.preAcquire()).thenReturn(true);
  }

  @Test
  public void should_fail_if_connection_busy() throws Throwable {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(-1);

    // When
    ChannelFuture writeFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(
                QUERY, false, Frame.NO_PAYLOAD, new MockResponseCallback()));

    // Then
    assertThat(writeFuture)
        .isFailed(e -> assertThat(e).isInstanceOf(BusyConnectionException.class));
  }

  @Test
  public void should_fail_write_if_pre_acquired_id_was_already_cancelled() {
    // Given
    addToPipeline();
    InFlightHandler handler = channel.pipeline().get(InFlightHandler.class);
    assertThat(handler.preAcquireId()).isTrue();
    DriverChannel.RequestMessage message =
        new DriverChannel.RequestMessage(
            QUERY, false, Frame.NO_PAYLOAD, new MockResponseCallback(), handler);
    message.cancelPreAcquireId();

    // When
    ChannelFuture writeFuture = channel.writeAndFlush(message);

    // Then
    assertThat(writeFuture)
        .isFailed(error -> assertThat(error).isInstanceOf(IllegalStateException.class));
    verify(streamIds).cancelPreAcquire();
    verify(streamIds, never()).acquire();
  }

  @Test
  public void should_assign_streamid_and_send_frame() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();

    // When
    ChannelFuture writeFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));

    // Then
    assertThat(writeFuture).isSuccess();
    verify(streamIds).acquire();

    Frame frame = readOutboundFrame();
    assertThat(frame.streamId).isEqualTo(42);
    assertThat(frame.message).isEqualTo(QUERY);
  }

  @Test
  public void should_notify_callback_of_response() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    Frame requestFrame = readOutboundFrame();

    // When
    Frame responseFrame = buildInboundFrame(requestFrame, Void.INSTANCE);
    writeInboundFrame(responseFrame);

    // Then
    assertThat(responseCallback.getLastResponse()).isSameAs(responseFrame);
    verify(streamIds).release(42);
  }

  @Test
  public void should_notify_response_promise_when_decoding_fails() throws Throwable {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    RuntimeException mockCause = new RuntimeException("test");
    channel.pipeline().fireExceptionCaught(new FrameDecodingException(42, mockCause));

    // Then
    assertThat(responseCallback.getFailure()).isSameAs(mockCause);
    verify(streamIds).release(42);
  }

  @Test
  public void should_complete_callbacks_once_for_alternating_valid_and_malformed_frames() {
    addDecoder();
    addToPipeline();
    when(streamIds.acquire()).thenReturn(0, 1, 2, 3);
    ResponseCallback[] callbacks = new ResponseCallback[4];
    ByteBuf input = channel.alloc().buffer();
    for (int streamId = 0; streamId < callbacks.length; streamId++) {
      callbacks[streamId] = mock(ResponseCallback.class);
      when(callbacks[streamId].isLastResponse(any(Frame.class))).thenReturn(true);
      sendRequest(callbacks[streamId]);
      appendResponse(input, streamId, streamId % 2 == 0 ? 0x7f : 0x08);
    }

    channel.writeInbound(input);

    for (int streamId = 0; streamId < callbacks.length; streamId++) {
      if (streamId % 2 == 0) {
        verify(callbacks[streamId]).onFailure(any(IllegalArgumentException.class));
        verify(callbacks[streamId], never()).onResponse(any(Frame.class));
      } else {
        verify(callbacks[streamId]).onResponse(any(Frame.class));
        verify(callbacks[streamId], never()).onFailure(any(Throwable.class));
      }
      verify(streamIds).release(streamId);
    }
    assertThat(channel.isActive()).isTrue();
    assertThat(input.refCnt()).isZero();
    channel.finishAndReleaseAll();
  }

  @Test
  public void should_stop_decoding_when_failure_callback_closes_channel() {
    addDecoder();
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback failed =
        new MockResponseCallback() {
          @Override
          public void onFailure(Throwable error) {
            super.onFailure(error);
            channel.close();
          }
        };
    MockResponseCallback pending = new MockResponseCallback();
    sendRequest(failed);
    sendRequest(pending);
    ByteBuf input = channel.alloc().buffer();
    appendResponse(input, 42, 0x7f);
    appendResponse(input, 43, 0x08);

    channel.writeInbound(input);

    assertThat(failed.getFailure()).isInstanceOf(IllegalArgumentException.class);
    assertThat(failed.getFailure()).isNull();
    assertThat(pending.getFailure()).isInstanceOf(ClosedConnectionException.class);
    assertThat(pending.getLastResponse()).isNull();
    assertThat(channel.isOpen()).isFalse();
    assertThat(input.refCnt()).isZero();
    channel.finishAndReleaseAll();
  }

  @Test
  public void should_stop_decoding_when_failure_callback_removes_decoder() {
    addDecoder();
    List<ByteBuf> remainingBytes = new ArrayList<>();
    channel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext context, Object message) {
                if (message instanceof ByteBuf) {
                  remainingBytes.add((ByteBuf) message);
                } else {
                  context.fireChannelRead(message);
                }
              }
            });
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback failed =
        new MockResponseCallback() {
          @Override
          public void onFailure(Throwable error) {
            super.onFailure(error);
            channel.pipeline().remove(FrameDecoder.class);
          }
        };
    sendRequest(failed);
    ByteBuf input = channel.alloc().buffer();
    appendResponse(input, 42, 0x7f);
    appendResponse(input, 43, 0x08);

    channel.writeInbound(input);

    assertThat(failed.getFailure()).isInstanceOf(IllegalArgumentException.class);
    assertThat(failed.getFailure()).isNull();
    assertThat(remainingBytes).hasSize(1);
    ByteBuf remaining = remainingBytes.get(0);
    assertThat(remaining.readableBytes()).isEqualTo(13);
    assertThat(remaining.getShort(remaining.readerIndex() + 2)).isEqualTo((short) 43);
    remaining.release();
    assertThat(channel.isActive()).isTrue();
    assertThat(input.refCnt()).isZero();
    channel.finishAndReleaseAll();
  }

  @Test
  public void should_close_channel_when_frame_extraction_fails() {
    assertUnsafeDecodingClosesChannel(new IllegalArgumentException("extraction failed"));
  }

  @Test
  public void should_close_channel_when_oversized_error_does_not_consume_frame() {
    assertUnsafeDecodingClosesChannel(new TooLongFrameException("no discard progress"));
  }

  @Test
  public void should_close_channel_when_heartbeat_response_is_malformed() throws Exception {
    addDecoder();
    DriverExecutionProfile config = mock(DriverExecutionProfile.class);
    when(config.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL))
        .thenReturn(Duration.ofSeconds(30));
    when(config.getDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(config.getDuration(DefaultDriverOption.REPEATED_LOG_INTERVAL))
        .thenReturn(Duration.ofSeconds(60));
    HeartbeatHandler heartbeatHandler = new HeartbeatHandler(config);
    channel.pipeline().addLast(heartbeatHandler);
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback pending = new MockResponseCallback();
    sendRequest(pending);
    heartbeatHandler.channelIdle(
        channel.pipeline().context(heartbeatHandler), IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);
    Frame heartbeat = readOutboundFrame();
    assertThat(heartbeat.streamId).isEqualTo(43);
    ByteBuf input = channel.alloc().buffer();
    appendResponse(input, 43, 0x7f);
    appendResponse(input, 42, 0x08);

    channel.writeInbound(input);

    assertThat(pending.getFailure())
        .isInstanceOf(HeartbeatException.class)
        .hasCauseInstanceOf(IllegalArgumentException.class);
    assertThat(pending.getLastResponse()).isNull();
    assertThat(channel.isOpen()).isFalse();
    assertThat(input.refCnt()).isZero();
    channel.finishAndReleaseAll();
  }

  private void assertUnsafeDecodingClosesChannel(RuntimeException cause) {
    FrameCodec<ByteBuf> codec =
        FrameCodec.defaultClient(new ByteBufPrimitiveCodec(channel.alloc()), Compressor.none());
    channel
        .pipeline()
        .addLast(
            new FrameDecoder(
                codec, ProtocolFeatures.EMPTY, cause instanceof TooLongFrameException ? 12 : 1024) {
              @Override
              protected long getUnadjustedFrameLength(
                  ByteBuf buffer, int offset, int length, ByteOrder order) {
                long frameLength = super.getUnadjustedFrameLength(buffer, offset, length, order);
                if (cause instanceof TooLongFrameException) {
                  throw cause;
                }
                return frameLength;
              }

              @Override
              protected ByteBuf extractFrame(
                  ChannelHandlerContext context, ByteBuf buffer, int index, int length) {
                throw cause;
              }
            });
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback first = new MockResponseCallback();
    MockResponseCallback second = new MockResponseCallback();
    sendRequest(first);
    sendRequest(second);
    ByteBuf input = channel.alloc().buffer();
    appendResponse(input, 42, 0x08);
    appendResponse(input, 43, 0x08);

    channel.writeInbound(input);

    for (MockResponseCallback callback : ImmutableList.of(first, second)) {
      Throwable failure = callback.getFailure();
      assertThat(failure).isInstanceOf(ClosedConnectionException.class);
      assertThat(failure.getCause()).isInstanceOf(DecoderException.class);
      if (cause instanceof DecoderException) {
        assertThat(failure.getCause()).isSameAs(cause);
      } else {
        assertThat(failure.getCause().getCause()).isSameAs(cause);
      }
      assertThat(callback.getFailure()).isNull();
    }
    assertThat(channel.isOpen()).isFalse();
    assertThat(input.refCnt()).isZero();
    channel.finishAndReleaseAll();
  }

  private void addDecoder() {
    FrameCodec<ByteBuf> codec =
        FrameCodec.defaultClient(new ByteBufPrimitiveCodec(channel.alloc()), Compressor.none());
    channel.pipeline().addLast(new FrameDecoder(codec, ProtocolFeatures.EMPTY, 1024));
  }

  private void sendRequest(ResponseCallback callback) {
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, callback));
    readOutboundFrame();
  }

  private void appendResponse(ByteBuf input, int streamId, int opcode) {
    input
        .writeByte(0x84)
        .writeByte(0)
        .writeShort(streamId)
        .writeByte(opcode)
        .writeInt(4)
        .writeInt(1);
  }

  @Test
  public void should_release_stream_id_when_orphaned_callback_receives_response() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    Frame requestFrame = readOutboundFrame();

    // When
    channel.writeAndFlush(responseCallback); // means cancellation (see DriverChannel#cancel)
    Frame responseFrame = buildInboundFrame(requestFrame, Void.INSTANCE);
    writeInboundFrame(responseFrame);

    // Then
    verify(streamIds).release(42);
    // The response is not propagated, because we assume a callback that cancelled managed its own
    // termination
    assertThat(responseCallback.getLastResponse()).isNull();
  }

  @Test
  public void should_delay_graceful_close_and_complete_when_last_pending_completes() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    // not closed yet because there is one pending request
    assertThat(channel.closeFuture()).isNotDone();

    // When
    // completing pending request
    Frame requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, Void.INSTANCE);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_delay_graceful_close_and_complete_when_last_pending_cancelled() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    // not closed yet because there is one pending request
    assertThat(channel.closeFuture()).isNotDone();

    // When
    // cancelling pending request
    channel.write(responseCallback);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_graceful_close_immediately_if_no_pending() {
    // Given
    addToPipeline();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_refuse_new_writes_during_graceful_close() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    // not closed yet because there is one pending request
    assertThat(channel.closeFuture()).isNotDone();
    // should not allow other write
    ChannelFuture otherWriteFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    assertThat(otherWriteFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Channel is closing"));
  }

  @Test
  public void should_close_gracefully_if_orphan_ids_above_max_and_pending_request() {
    // Given
    addToPipeline();
    // Generate n orphan ids by writing and cancelling the requests:
    for (int i = 0; i < MAX_ORPHAN_IDS; i++) {
      when(streamIds.acquire()).thenReturn(i);
      MockResponseCallback responseCallback = new MockResponseCallback();
      channel
          .writeAndFlush(
              new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
          .awaitUninterruptibly();
      channel.writeAndFlush(responseCallback).awaitUninterruptibly();
    }
    // Generate another request that is pending and not cancelled:
    when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS);
    MockResponseCallback pendingResponseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(
                QUERY, false, Frame.NO_PAYLOAD, pendingResponseCallback))
        .awaitUninterruptibly();

    // When
    // Generate the n+1th orphan id that makes us go above the threshold
    when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS + 1);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();
    channel.writeAndFlush(responseCallback).awaitUninterruptibly();

    // Then
    // Channel should be closing gracefully. There's no way to observe that from the outside, so
    // write another request and check that it's rejected:
    assertThat(channel.closeFuture()).isNotDone();
    ChannelFuture otherWriteFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    assertThat(otherWriteFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Channel is closing"));

    // When
    // Cancel the last pending request
    channel.writeAndFlush(pendingResponseCallback).awaitUninterruptibly();

    // Then
    // The graceful shutdown completes
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_close_gracefully_if_orphan_ids_above_max_and_multiple_pending_requests() {
    // Given
    addToPipeline();
    // Generate n orphan ids by writing and cancelling the requests.
    for (int i = 0; i < MAX_ORPHAN_IDS; i++) {
      when(streamIds.acquire()).thenReturn(i);
      MockResponseCallback responseCallback = new MockResponseCallback();
      channel
          .writeAndFlush(
              new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
          .awaitUninterruptibly();
      channel.writeAndFlush(responseCallback).awaitUninterruptibly();
    }
    // Generate 3 additional requests that are pending and not cancelled.
    List<MockResponseCallback> pendingResponseCallbacks = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS + i);
      MockResponseCallback responseCallback = new MockResponseCallback();
      channel
          .writeAndFlush(
              new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
          .awaitUninterruptibly();
      pendingResponseCallbacks.add(responseCallback);
    }

    // When
    // Generate the n+1th orphan id that makes us go above the threshold by canceling one if the
    // pending requests.
    channel.writeAndFlush(pendingResponseCallbacks.remove(0)).awaitUninterruptibly();

    // Then
    // Channel should be closing gracefully but there's no way to observe that from the outside
    // besides writing another request and check that it's rejected.
    assertThat(channel.closeFuture()).isNotDone();
    ChannelFuture otherWriteFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(
                QUERY, false, Frame.NO_PAYLOAD, new MockResponseCallback()));
    assertThat(otherWriteFuture).isFailed();
    assertThat(otherWriteFuture.cause())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Channel is closing");

    // When
    // Cancel the remaining pending requests causing the n+ith orphan ids above the threshold.
    for (MockResponseCallback pendingResponseCallback : pendingResponseCallbacks) {
      ChannelFuture future = channel.writeAndFlush(pendingResponseCallback).awaitUninterruptibly();

      // Then
      // The future should succeed even though the channel has started closing gracefully.
      assertThat(future).isSuccess();
    }

    // Then
    // The graceful shutdown completes.
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_close_immediately_if_orphan_ids_above_max_and_no_pending_requests() {
    // Given
    addToPipeline();
    // Generate n orphan ids by writing and cancelling the requests:
    for (int i = 0; i < MAX_ORPHAN_IDS; i++) {
      when(streamIds.acquire()).thenReturn(i);
      MockResponseCallback responseCallback = new MockResponseCallback();
      channel
          .writeAndFlush(
              new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
          .awaitUninterruptibly();
      channel.writeAndFlush(responseCallback).awaitUninterruptibly();
    }

    // When
    // Generate the n+1th orphan id that makes us go above the threshold
    when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();
    channel.writeAndFlush(responseCallback).awaitUninterruptibly();

    // Then
    // Channel should close immediately since no active pending requests.
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_fail_all_pending_when_force_closed() throws Throwable {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1))
        .awaitUninterruptibly();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.FORCEFUL_CLOSE_MESSAGE);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      assertThat(callback.getFailure())
          .isInstanceOf(ClosedConnectionException.class)
          .hasMessageContaining("Channel was force-closed");
    }
  }

  @Test
  public void should_fail_all_pending_and_close_on_unexpected_inbound_exception() throws Throwable {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1))
        .awaitUninterruptibly();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2))
        .awaitUninterruptibly();

    // When
    RuntimeException mockException = new RuntimeException("test");
    channel.pipeline().fireExceptionCaught(mockException);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      Throwable failure = callback.getFailure();
      assertThat(failure).isInstanceOf(ClosedConnectionException.class);
      assertThat(failure.getCause()).isSameAs(mockException);
    }
  }

  @Test
  public void should_fail_all_pending_if_connection_lost() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1))
        .awaitUninterruptibly();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2))
        .awaitUninterruptibly();

    // When
    channel.pipeline().fireChannelInactive();

    // Then
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      assertThat(callback.getFailure())
          .isInstanceOf(ClosedConnectionException.class)
          .hasMessageContaining("Lost connection to remote peer");
    }
  }

  @Test
  public void should_hold_stream_id_for_multi_response_callback() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback =
        new MockResponseCallback(frame -> frame.message instanceof Error);

    // When
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // Then
    // notify callback of stream id
    assertThat(responseCallback.streamId).isEqualTo(42);

    Frame requestFrame = readOutboundFrame();
    for (int i = 0; i < 5; i++) {
      // When
      // completing pending request
      Frame responseFrame = buildInboundFrame(requestFrame, Void.INSTANCE);
      writeInboundFrame(responseFrame);

      // Then
      assertThat(responseCallback.getLastResponse()).isSameAs(responseFrame);
      // Stream id not released, callback can receive more responses
      verify(streamIds, never()).release(42);
    }

    // When
    // a terminal response comes in
    Frame responseFrame = buildInboundFrame(requestFrame, new Error(0, "test"));
    writeInboundFrame(responseFrame);

    // Then
    verify(streamIds).release(42);
    assertThat(responseCallback.getLastResponse()).isSameAs(responseFrame);

    // When
    // more responses come in
    writeInboundFrame(requestFrame, Void.INSTANCE);

    // Then
    // the callback does not get them anymore (this could only be responses to a new request that
    // reused the id)
    assertThat(responseCallback.getLastResponse()).isNull();
  }

  @Test
  public void
      should_release_stream_id_when_orphaned_multi_response_callback_receives_last_response() {
    // Given
    addToPipeline();
    when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback =
        new MockResponseCallback(frame -> frame.message instanceof Error);

    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    Frame requestFrame = readOutboundFrame();
    for (int i = 0; i < 5; i++) {
      Frame responseFrame = buildInboundFrame(requestFrame, Void.INSTANCE);
      writeInboundFrame(responseFrame);
      assertThat(responseCallback.getLastResponse()).isSameAs(responseFrame);
      verify(streamIds, never()).release(42);
    }

    // When
    // cancelled mid-flight
    channel.writeAndFlush(responseCallback);

    // Then
    // subsequent non-final responses are not propagated (we assume the callback completed itself
    // already), but do not release the stream id
    writeInboundFrame(requestFrame, Void.INSTANCE);
    assertThat(responseCallback.getLastResponse()).isNull();
    verify(streamIds, never()).release(42);

    // When
    // the terminal response arrives
    writeInboundFrame(requestFrame, new Error(0, "test"));

    // Then
    // still not propagated but the id is released
    assertThat(responseCallback.getLastResponse()).isNull();
    verify(streamIds).release(42);
  }

  @Test
  public void should_set_keyspace() {
    // Given
    addToPipeline();
    ChannelPromise setKeyspacePromise = channel.newPromise();
    DriverChannel.SetKeyspaceEvent setKeyspaceEvent =
        new DriverChannel.SetKeyspaceEvent(CqlIdentifier.fromCql("ks"), setKeyspacePromise);

    // When
    channel.pipeline().fireUserEventTriggered(setKeyspaceEvent);
    Frame requestFrame = readOutboundFrame();

    // Then
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    writeInboundFrame(requestFrame, new SetKeyspace("ks"));
    assertThat(setKeyspacePromise).isSuccess();
  }

  @Test
  public void should_fail_to_set_keyspace_if_query_times_out() throws InterruptedException {
    // Given
    addToPipeline();
    ChannelPromise setKeyspacePromise = channel.newPromise();
    DriverChannel.SetKeyspaceEvent setKeyspaceEvent =
        new DriverChannel.SetKeyspaceEvent(CqlIdentifier.fromCql("ks"), setKeyspacePromise);

    // When
    channel.pipeline().fireUserEventTriggered(setKeyspaceEvent);
    TimeUnit.MILLISECONDS.sleep(SET_KEYSPACE_TIMEOUT_MILLIS * 2);
    channel.runPendingTasks();

    // Then
    assertThat(setKeyspacePromise).isFailed();
  }

  @Test
  public void should_notify_callback_of_events() {
    // Given
    EventCallback eventCallback = mock(EventCallback.class);
    addToPipelineWithEventCallback(eventCallback);

    // When
    StatusChangeEvent event =
        new StatusChangeEvent(
            ProtocolConstants.StatusChangeType.UP, new InetSocketAddress("127.0.0.1", 9042));
    Frame eventFrame =
        Frame.forResponse(
            DefaultProtocolVersion.V3.getCode(),
            -1,
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            event);
    writeInboundFrame(eventFrame);

    // Then
    ArgumentCaptor<StatusChangeEvent> captor = ArgumentCaptor.forClass(StatusChangeEvent.class);
    verify(eventCallback).onEvent(captor.capture());
    assertThat(captor.getValue()).isSameAs(event);
  }

  private void addToPipeline() {
    addToPipelineWithEventCallback(null);
  }

  private void addToPipelineWithEventCallback(EventCallback eventCallback) {
    channel
        .pipeline()
        .addLast(
            new InFlightHandler(
                DefaultProtocolVersion.V3,
                streamIds,
                MAX_ORPHAN_IDS,
                SET_KEYSPACE_TIMEOUT_MILLIS,
                channel.newPromise(),
                eventCallback,
                "test"));
  }
}
