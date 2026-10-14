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
package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.internal.core.channel.ChannelHandlerTestBase;
import com.datastax.oss.driver.internal.core.util.ByteBufs;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolFeatures;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FrameDecoderTest extends ChannelHandlerTestBase {
  // A valid binary payload for a response frame.
  private static final ByteBuf VALID_PAYLOAD =
      ByteBufs.fromHexString(
          "0x84" // response frame, protocol version 4
              + "00" // flags (none)
              + "002a" // stream id (42)
              + "10" // opcode for AUTH_SUCCESS message
              + "00000008" // body length
              + "00000004cafebabe" // body
          );

  // A binary payload that is invalid because the protocol version is not supported by the codec
  private static final ByteBuf INVALID_PAYLOAD =
      ByteBufs.fromHexString(
          "0xFF" // response frame, protocol version 127
              + "00002a100000000800000004cafebabe");

  private FrameCodec<ByteBuf> frameCodec;
  private final List<Object> decoded = new ArrayList<>();

  @Before
  @Override
  public void setup() {
    super.setup();
    frameCodec =
        FrameCodec.defaultClient(new ByteBufPrimitiveCodec(channel.alloc()), Compressor.none());
  }

  @After
  public void teardown() {
    channel.finishAndReleaseAll();
  }

  @Test
  public void should_decode_valid_payload() {
    // Given
    FrameDecoder decoder = new FrameDecoder(frameCodec, ProtocolFeatures.EMPTY, 1024);
    channel.pipeline().addLast(decoder);

    // When
    // The decoder releases the buffer, so make sure we retain it for the other tests
    VALID_PAYLOAD.retain();
    channel.writeInbound(VALID_PAYLOAD.duplicate());
    Frame frame = readInboundFrame();

    // Then
    assertThat(frame.message).isInstanceOf(AuthSuccess.class);
  }

  /**
   * Checks that an exception carrying the stream id is thrown when decoding fails in the {@link
   * LengthFieldBasedFrameDecoder} code.
   */
  @Test
  public void should_fail_to_decode_if_payload_is_valid_but_too_long() {
    // Given
    FrameDecoder decoder =
        new FrameDecoder(frameCodec, ProtocolFeatures.EMPTY, VALID_PAYLOAD.readableBytes() - 1);
    channel.pipeline().addLast(decoder);

    // When
    VALID_PAYLOAD.retain();
    try {
      channel.writeInbound(VALID_PAYLOAD.duplicate());
      fail("expected an exception");
    } catch (FrameDecodingException e) {
      // Then
      assertThat(e.streamId).isEqualTo(42);
      assertThat(e.getCause()).isInstanceOf(FrameTooLongException.class);
    }
  }

  /** Checks that an exception carrying the stream id is thrown when decoding fails in our code. */
  @Test
  public void should_fail_to_decode_if_payload_cannot_be_decoded() {
    // Given
    FrameDecoder decoder = new FrameDecoder(frameCodec, ProtocolFeatures.EMPTY, 1024);
    channel.pipeline().addLast(decoder);

    // When
    INVALID_PAYLOAD.retain();
    try {
      channel.writeInbound(INVALID_PAYLOAD.duplicate());
      fail("expected an exception");
    } catch (FrameDecodingException e) {
      // Then
      assertThat(e.streamId).isEqualTo(42);
      assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void should_decode_valid_frame_after_malformed_frame_in_same_input() {
    addDecoder(1024);
    ByteBuf input = channel.alloc().buffer();
    appendPayload(input, INVALID_PAYLOAD, 42);
    appendPayload(input, VALID_PAYLOAD, 43);

    channel.writeInbound(input);

    assertThat(decoded).hasSize(2);
    assertFailure(0, 42, IllegalArgumentException.class);
    assertResponse(1, 43);
    assertThat(channel.isActive()).isTrue();
    assertThat(input.refCnt()).isZero();
  }

  @Test
  public void should_decode_alternating_valid_and_malformed_frames_in_order() {
    addDecoder(1024);
    ByteBuf input = channel.alloc().buffer();
    for (int streamId = 0; streamId < 6; streamId++) {
      appendPayload(input, streamId % 2 == 0 ? VALID_PAYLOAD : INVALID_PAYLOAD, streamId);
    }

    channel.writeInbound(input);

    assertThat(decoded).hasSize(6);
    for (int streamId = 0; streamId < 6; streamId++) {
      if (streamId % 2 == 0) {
        assertResponse(streamId, streamId);
      } else {
        assertFailure(streamId, streamId, IllegalArgumentException.class);
      }
    }
    assertThat(input.refCnt()).isZero();
  }

  @Test
  public void should_decode_valid_frame_after_complete_oversized_frame() {
    addDecoder(20);
    ByteBuf input = channel.alloc().buffer();
    ByteBuf oversized = oversizedPayload(42);
    input.writeBytes(oversized);
    oversized.release();
    appendPayload(input, VALID_PAYLOAD, 43);

    channel.writeInbound(input);

    assertThat(decoded).hasSize(2);
    assertFailure(0, 42, FrameTooLongException.class);
    assertResponse(1, 43);
    assertThat(input.refCnt()).isZero();
  }

  @Test
  public void should_decode_valid_frame_after_fragmented_oversized_frame() {
    addDecoder(20);
    ByteBuf oversized = oversizedPayload(42);
    channel.writeInbound(oversized.readRetainedSlice(11));

    assertThat(decoded).hasSize(1);
    assertFailure(0, 42, FrameTooLongException.class);

    channel.writeInbound(oversized.readRetainedSlice(7));
    assertThat(decoded).hasSize(1);
    ByteBuf input = channel.alloc().buffer();
    input.writeBytes(oversized);
    oversized.release();
    appendPayload(input, VALID_PAYLOAD, 43);
    channel.writeInbound(input);

    assertThat(decoded).hasSize(2);
    assertResponse(1, 43);
    assertThat(input.refCnt()).isZero();
    assertThat(oversized.refCnt()).isZero();
  }

  @Test
  public void should_attribute_malformed_frame_after_oversized_discard_to_its_header() {
    addDecoder(20);
    ByteBuf oversized = oversizedPayload(42);
    channel.writeInbound(oversized.readRetainedSlice(11));
    ByteBuf input = channel.alloc().buffer();
    input.writeBytes(oversized);
    oversized.release();
    appendPayload(input, INVALID_PAYLOAD, 43);
    appendPayload(input, VALID_PAYLOAD, 44);

    channel.writeInbound(input);

    assertThat(decoded).hasSize(3);
    assertFailure(0, 42, FrameTooLongException.class);
    assertFailure(1, 43, IllegalArgumentException.class);
    assertResponse(2, 44);
    assertThat(input.refCnt()).isZero();
  }

  @Test
  public void should_attribute_second_oversized_frame_after_discard_to_its_header() {
    addDecoder(20);
    ByteBuf oversized = oversizedPayload(42);
    channel.writeInbound(oversized.readRetainedSlice(11));
    ByteBuf input = channel.alloc().buffer();
    input.writeBytes(oversized);
    oversized.release();
    ByteBuf secondOversized = oversizedPayload(43);
    input.writeBytes(secondOversized);
    secondOversized.release();
    appendPayload(input, VALID_PAYLOAD, 44);

    channel.writeInbound(input);

    assertThat(decoded).hasSize(3);
    assertFailure(0, 42, FrameTooLongException.class);
    assertFailure(1, 43, FrameTooLongException.class);
    assertResponse(2, 44);
    assertThat(input.refCnt()).isZero();
  }

  @Test
  public void should_wait_for_partial_frame_after_malformed_frame() {
    addDecoder(1024);
    ByteBuf valid = payload(VALID_PAYLOAD, 43);
    ByteBuf input = channel.alloc().buffer();
    appendPayload(input, INVALID_PAYLOAD, 42);
    input.writeBytes(valid.readSlice(10));

    channel.writeInbound(input);

    assertThat(decoded).hasSize(1);
    assertFailure(0, 42, IllegalArgumentException.class);
    assertThat(channel.isActive()).isTrue();

    channel.writeInbound(valid);

    assertThat(decoded).hasSize(2);
    assertResponse(1, 43);
    assertThat(input.refCnt()).isZero();
    assertThat(valid.refCnt()).isZero();
  }

  @Test
  public void should_wait_for_partial_header_after_oversized_discard() {
    addDecoder(20);
    ByteBuf oversized = oversizedPayload(42);
    channel.writeInbound(oversized.readRetainedSlice(11));
    ByteBuf valid = payload(VALID_PAYLOAD, 43);
    ByteBuf input = channel.alloc().buffer();
    input.writeBytes(oversized);
    oversized.release();
    input.writeBytes(valid.readSlice(3));

    channel.writeInbound(input);

    assertThat(decoded).hasSize(1);
    assertFailure(0, 42, FrameTooLongException.class);
    channel.writeInbound(valid);

    assertThat(decoded).hasSize(2);
    assertResponse(1, 43);
    assertThat(input.refCnt()).isZero();
    assertThat(valid.refCnt()).isZero();
  }

  private void addDecoder(int maxFrameLength) {
    channel
        .pipeline()
        .addLast(new FrameDecoder(frameCodec, ProtocolFeatures.EMPTY, maxFrameLength));
    channel
        .pipeline()
        .addLast(
            new ChannelInboundHandlerAdapter() {
              @Override
              public void channelRead(ChannelHandlerContext context, Object message) {
                decoded.add(message);
                context.fireChannelRead(message);
              }

              @Override
              public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                decoded.add(cause);
              }
            });
  }

  private ByteBuf payload(ByteBuf template, int streamId) {
    ByteBuf payload = template.copy();
    payload.setShort(2, streamId);
    return payload;
  }

  private void appendPayload(ByteBuf input, ByteBuf template, int streamId) {
    int startIndex = input.writerIndex();
    input.writeBytes(template, template.readerIndex(), template.readableBytes());
    input.setShort(startIndex + 2, streamId);
  }

  private ByteBuf oversizedPayload(int streamId) {
    ByteBuf payload = channel.alloc().buffer();
    payload.writeByte(0x84).writeByte(0).writeShort(streamId).writeByte(0x10).writeInt(32);
    for (int index = 0; index < 32; index++) {
      payload.writeByte(0x77);
    }
    return payload;
  }

  private void assertFailure(int index, int streamId, Class<? extends Throwable> causeType) {
    assertThat(decoded.get(index)).isInstanceOf(FrameDecodingException.class);
    FrameDecodingException failure = (FrameDecodingException) decoded.get(index);
    assertThat(failure.streamId).isEqualTo(streamId);
    assertThat(failure.getCause()).isInstanceOf(causeType);
  }

  private void assertResponse(int index, int streamId) {
    assertThat(decoded.get(index)).isInstanceOf(Frame.class);
    Frame response = (Frame) decoded.get(index);
    assertThat(response.streamId).isEqualTo(streamId);
    assertThat(response.message).isInstanceOf(AuthSuccess.class);
  }
}
