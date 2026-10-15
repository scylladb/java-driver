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

import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolFeatures;
import com.datastax.oss.protocol.internal.response.Error;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import java.nio.ByteOrder;
import java.util.Collections;
import net.jcip.annotations.NotThreadSafe;

/**
 * Decodes legacy frames, reporting recoverable failures downstream without interrupting decoding of
 * subsequent buffered frames. Extraction failures with unknown frame boundaries are propagated as
 * decoder exceptions so that the connection can be closed.
 */
@NotThreadSafe
public class FrameDecoder extends LengthFieldBasedFrameDecoder {
  // Where the length of the frame is located in the payload
  private static final int LENGTH_FIELD_OFFSET = 5;
  private static final int LENGTH_FIELD_LENGTH = 4;

  private final FrameCodec<ByteBuf> frameCodec;
  private final ProtocolFeatures protocolFeatures;
  private final int maxFrameLengthInBytes;
  private boolean isFirstResponse;
  private int frameStreamId;
  private boolean oversizedFrameHeaderRead;

  public FrameDecoder(
      FrameCodec<ByteBuf> frameCodec,
      ProtocolFeatures protocolFeatures,
      int maxFrameLengthInBytes) {
    super(maxFrameLengthInBytes, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, 0, 0, true);
    this.frameCodec = frameCodec;
    this.protocolFeatures = protocolFeatures;
    this.maxFrameLengthInBytes = maxFrameLengthInBytes;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    if (!ctx.channel().isOpen()) {
      in.skipBytes(in.readableBytes());
      return null;
    }
    int startIndex = in.readerIndex();
    if (isFirstResponse) {
      isFirstResponse = false;

      // Must read at least the protocol v1/v2 header (see below)
      if (in.readableBytes() < 8) {
        return null;
      }
      // Special case for obsolete protocol versions (< v3): the length field is at a different
      // position, so we can't delegate to super.decode() which would read the wrong length.
      int protocolVersion = (int) in.getByte(startIndex) & 0b0111_1111;
      if (protocolVersion < 3) {
        int streamId = in.getByte(startIndex + 2);
        int length = in.getInt(startIndex + 4);
        // We don't need a full-blown decoder, just to signal the protocol error. So discard the
        // incoming data and spoof a server-side protocol error.
        if (in.readableBytes() < 8 + length) {
          return null; // keep reading until we can discard the whole message at once
        } else {
          in.readerIndex(startIndex + 8 + length);
        }
        return Frame.forResponse(
            protocolVersion,
            streamId,
            null,
            Frame.NO_PAYLOAD,
            Collections.emptyList(),
            new Error(
                ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
                "Invalid or unsupported protocol version"));
      }
    }

    ByteBuf buffer;
    oversizedFrameHeaderRead = false;
    try {
      buffer = (ByteBuf) super.decode(ctx, in);
    } catch (TooLongFrameException exception) {
      if (!oversizedFrameHeaderRead || in.readerIndex() <= startIndex) {
        throw exception;
      }
      ctx.fireExceptionCaught(
          new FrameDecodingException(
              frameStreamId,
              new FrameTooLongException(ctx.channel().remoteAddress(), exception.getMessage())));
      return null;
    }
    if (buffer == null) {
      return null;
    }
    int streamId = buffer.getShort(buffer.readerIndex() + 2);
    try {
      return frameCodec.decode(buffer, protocolFeatures);
    } catch (Exception exception) {
      ctx.fireExceptionCaught(new FrameDecodingException(streamId, exception));
      return null;
    }
  }

  @Override
  protected long getUnadjustedFrameLength(ByteBuf buffer, int offset, int length, ByteOrder order) {
    long frameLength = super.getUnadjustedFrameLength(buffer, offset, length, order);
    if (frameLength + LENGTH_FIELD_OFFSET + LENGTH_FIELD_LENGTH > maxFrameLengthInBytes) {
      frameStreamId = buffer.getShort(offset - LENGTH_FIELD_OFFSET + 2);
      oversizedFrameHeaderRead = true;
    }
    return frameLength;
  }

  @Override
  protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
    return buffer.slice(index, length);
  }
}
