/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.exceptions.DriverInternalError;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import net.jpountz.lz4.LZ4Exception;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.xerial.snappy.SnappyError;
import org.xerial.snappy.SnappyErrorCode;

public class FrameCompressorTest {

  private static final byte[] PAYLOAD = compressiblePayload();

  @DataProvider(name = "compressors")
  public static Object[][] compressors() {
    return new Object[][] {
      {Compression.SNAPPY, false},
      {Compression.SNAPPY, true},
      {Compression.LZ4, false},
      {Compression.LZ4, true}
    };
  }

  @DataProvider(name = "bufferKinds")
  public static Object[][] bufferKinds() {
    return new Object[][] {{false}, {true}};
  }

  @Test(groups = "unit", dataProvider = "compressors")
  public void should_round_trip_frame(Compression compression, boolean direct) throws Exception {
    FrameCompressor compressor = compressor(compression);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf body = buffer(allocator, direct, PAYLOAD);
    Frame frame =
        Frame.create(
            ProtocolVersion.V4,
            Message.Request.Type.QUERY.opcode,
            42,
            EnumSet.noneOf(Frame.Header.Flag.class),
            body);

    Frame compressed = compressor.compress(frame);

    assertThat(compressed.body.isDirect()).isEqualTo(direct);
    assertThat(compressed.body.readableBytes()).isLessThan(PAYLOAD.length);
    assertThat(compressed.header.bodyLength).isEqualTo(compressed.body.readableBytes());
    assertThat(compressed.header.streamId).isEqualTo(42);
    assertThat(compressed.header.opcode).isEqualTo(Message.Request.Type.QUERY.opcode);
    assertThat(compressed.header.version).isEqualTo(ProtocolVersion.V4);
    assertThat(body.readableBytes()).isZero();
    assertThat(body.refCnt()).isEqualTo(1);

    Frame decompressed = compressor.decompress(compressed);

    assertThat(decompressed.body.isDirect()).isEqualTo(direct);
    assertThat(ByteBufUtil.getBytes(decompressed.body)).isEqualTo(PAYLOAD);
    assertThat(decompressed.header.bodyLength).isEqualTo(PAYLOAD.length);
    assertThat(decompressed.header.streamId).isEqualTo(42);
    assertThat(compressed.body.readableBytes()).isZero();
    assertThat(compressed.body.refCnt()).isEqualTo(1);

    release(body, compressed.body, decompressed.body);
  }

  @Test(groups = "unit", dataProvider = "compressors")
  public void should_round_trip_byte_buf(Compression compression, boolean direct) throws Exception {
    FrameCompressor compressor = compressor(compression);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf input = buffer(allocator, direct, PAYLOAD);

    ByteBuf compressed = compressor.compress(input);

    assertThat(compressed.isDirect()).isEqualTo(direct);
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    assertThat(input.readableBytes()).isZero();
    assertThat(input.refCnt()).isEqualTo(1);

    ByteBuf decompressed = compressor.decompress(compressed, PAYLOAD.length);

    assertThat(decompressed.isDirect()).isEqualTo(direct);
    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
    assertThat(compressed.readableBytes()).isZero();
    assertThat(compressed.refCnt()).isEqualTo(1);

    release(input, compressed, decompressed);
  }

  @Test(groups = "unit", dataProvider = "bufferKinds")
  public void should_prefix_lz4_frame_body_with_uncompressed_length(boolean direct)
      throws Exception {
    FrameCompressor compressor = compressor(Compression.LZ4);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf frameBody = buffer(allocator, direct, PAYLOAD);
    ByteBuf rawInput = buffer(allocator, direct, PAYLOAD);

    Frame compressedFrame =
        compressor.compress(
            Frame.create(
                ProtocolVersion.V4,
                Message.Request.Type.QUERY.opcode,
                1,
                EnumSet.noneOf(Frame.Header.Flag.class),
                frameBody));
    ByteBuf compressedRaw = compressor.compress(rawInput);

    ByteBuf body = compressedFrame.body;
    assertThat(body.readableBytes()).isEqualTo(4 + compressedRaw.readableBytes());
    assertThat(body.getInt(body.readerIndex())).isEqualTo(PAYLOAD.length);
    assertThat(ByteBufUtil.getBytes(body, body.readerIndex() + 4, body.readableBytes() - 4))
        .isEqualTo(ByteBufUtil.getBytes(compressedRaw));

    release(frameBody, rawInput, body, compressedRaw);
  }

  @Test(groups = "unit", dataProvider = "bufferKinds")
  public void should_release_output_and_wrap_when_lz4_compression_fails(boolean direct)
      throws Exception {
    FrameCompressor compressor = compressor(Compression.LZ4);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf input = buffer(allocator, direct, PAYLOAD);
    allocator.startTracking(16);

    try {
      compressor.compress(input);
      fail("Expected an IOException");
    } catch (IOException e) {
      assertThat(e.getCause()).isInstanceOf(LZ4Exception.class);
    }

    assertThat(allocator.allocated).hasSize(1);
    assertThat(allocator.allocated.get(0).isDirect()).isEqualTo(direct);
    assertThat(allocator.allocated.get(0).refCnt()).isZero();
    assertThat(input.refCnt()).isEqualTo(1);
    release(input);
  }

  @Test(groups = "unit", dataProvider = "bufferKinds")
  public void should_release_output_and_wrap_when_lz4_compressed_lengths_mismatch(boolean direct)
      throws Exception {
    FrameCompressor compressor = compressor(Compression.LZ4);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf input = buffer(allocator, direct, PAYLOAD);
    ByteBuf compressed = compressor.compress(input);
    compressed.writeBytes(new byte[] {1, 2, 3});
    allocator.startTracking(Integer.MAX_VALUE);

    try {
      compressor.decompress(compressed, PAYLOAD.length);
      fail("Expected an IOException");
    } catch (IOException e) {
      // The mismatch IOException thrown inside the try is re-wrapped by the catch-all.
      assertThat(e.getCause()).isInstanceOf(IOException.class);
      assertThat(e.getCause().getMessage()).isEqualTo("Compressed lengths mismatch");
    }

    assertThat(allocator.allocated).hasSize(1);
    assertThat(allocator.allocated.get(0).refCnt()).isZero();
    assertThat(compressed.readableBytes()).isZero();
    assertThat(compressed.refCnt()).isEqualTo(1);
    release(input, compressed);
  }

  @Test(groups = "unit", dataProvider = "bufferKinds")
  public void should_reject_input_that_is_not_snappy_compressed(boolean direct) throws Exception {
    FrameCompressor compressor = compressor(Compression.SNAPPY);
    TrackingAllocator allocator = new TrackingAllocator();
    // A length varint that never terminates.
    byte[] garbage = new byte[8];
    Arrays.fill(garbage, (byte) 0xFF);
    ByteBuf input = buffer(allocator, direct, garbage);
    allocator.startTracking(Integer.MAX_VALUE);

    try {
      compressor.decompress(input, 8);
      fail("Expected a DriverInternalError");
    } catch (DriverInternalError e) {
      assertThat(e.getMessage())
          .isEqualTo("Provided frame does not appear to be Snappy compressed");
    }

    assertThat(allocator.allocated).isEmpty();
    assertThat(input.readableBytes()).isZero();
    assertThat(input.refCnt()).isEqualTo(1);
    release(input);
  }

  @Test(groups = "unit", dataProvider = "bufferKinds")
  public void should_ignore_provided_uncompressed_length_with_snappy(boolean direct)
      throws Exception {
    FrameCompressor compressor = compressor(Compression.SNAPPY);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf input = buffer(allocator, direct, PAYLOAD);
    ByteBuf compressed = compressor.compress(input);

    ByteBuf decompressed = compressor.decompress(compressed, 1);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
    release(input, compressed, decompressed);
  }

  @Test(groups = "unit")
  public void should_leak_snappy_output_when_direct_composite_input_is_compressed()
      throws Exception {
    FrameCompressor compressor = compressor(Compression.SNAPPY);
    TrackingAllocator allocator = new TrackingAllocator();
    CompositeByteBuf input = directComposite(allocator, PAYLOAD);
    assertThat(input.isDirect()).isTrue();
    allocator.startTracking(Integer.MAX_VALUE);

    try {
      compressor.compress(input);
      fail("Expected a SnappyError");
    } catch (SnappyError e) {
      // Pins current behaviour (#1187): FrameCompressor.inputNioBuffer merges the two components
      // into a heap ByteBuffer, which Snappy rejects.
      assertThat(e.errorCode).isEqualTo(SnappyErrorCode.NOT_A_DIRECT_BUFFER);
    }

    // Pins current behaviour (#1173): SnappyCompressor only catches IOException, so a SnappyError
    // leaks the output buffer it allocated.
    assertThat(allocator.allocated).hasSize(1);
    assertThat(allocator.allocated.get(0).refCnt()).isEqualTo(1);
    assertThat(input.refCnt()).isEqualTo(1);
    release(input, allocator.allocated.get(0));
  }

  @Test(groups = "unit")
  public void should_fail_before_allocating_when_snappy_decompresses_direct_composite_input()
      throws Exception {
    FrameCompressor compressor = compressor(Compression.SNAPPY);
    TrackingAllocator allocator = new TrackingAllocator();
    ByteBuf heapInput = buffer(allocator, false, PAYLOAD);
    ByteBuf compressed = compressor.compress(heapInput);
    CompositeByteBuf input = directComposite(allocator, ByteBufUtil.getBytes(compressed));
    allocator.startTracking(Integer.MAX_VALUE);

    try {
      compressor.decompress(input, PAYLOAD.length);
      fail("Expected an IOException");
    } catch (IOException e) {
      // Pins current behaviour (#1187): the native validity check rejects the merged heap
      // ByteBuffer.
      assertThat(e.getMessage()).isEqualTo("NOT_A_DIRECT_BUFFER(3)");
    }

    assertThat(allocator.allocated).isEmpty();
    assertThat(input.refCnt()).isEqualTo(1);
    release(heapInput, compressed, input);
  }

  @Test(groups = "unit")
  public void should_round_trip_direct_composite_input_with_lz4() throws Exception {
    FrameCompressor compressor = compressor(Compression.LZ4);
    TrackingAllocator allocator = new TrackingAllocator();
    CompositeByteBuf input = directComposite(allocator, PAYLOAD);
    assertThat(input.nioBufferCount()).isGreaterThan(1);

    ByteBuf compressed = compressor.compress(input);
    assertThat(compressed.isDirect()).isTrue();
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);

    CompositeByteBuf compressedComposite =
        directComposite(allocator, ByteBufUtil.getBytes(compressed));
    assertThat(compressedComposite.nioBufferCount()).isGreaterThan(1);
    ByteBuf decompressed = compressor.decompress(compressedComposite, PAYLOAD.length);

    assertThat(decompressed.isDirect()).isTrue();
    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
    release(input, compressed, compressedComposite, decompressed);
  }

  private static FrameCompressor compressor(Compression compression) {
    FrameCompressor compressor = compression.compressor();
    // Both libraries are on the test classpath, so null means the compressor failed to load.
    assertThat(compressor).as(compression + " compressor").isNotNull();
    return compressor;
  }

  /** Writes {@code bytes} after a skipped prefix, so that non-zero reader indexes are exercised. */
  private static ByteBuf buffer(TrackingAllocator allocator, boolean direct, byte[] bytes) {
    ByteBuf buf =
        direct ? allocator.directBuffer(bytes.length + 3) : allocator.heapBuffer(bytes.length + 3);
    buf.writeBytes(new byte[] {9, 9, 9}).skipBytes(3);
    return buf.writeBytes(bytes);
  }

  private static CompositeByteBuf directComposite(TrackingAllocator allocator, byte[] bytes) {
    int half = bytes.length / 2;
    ByteBuf first = allocator.directBuffer(half).writeBytes(bytes, 0, half);
    ByteBuf second =
        allocator.directBuffer(bytes.length - half).writeBytes(bytes, half, bytes.length - half);
    CompositeByteBuf composite = allocator.compositeDirectBuffer();
    composite.addComponents(true, first, second);
    return composite;
  }

  private static void release(ByteBuf... buffers) {
    for (ByteBuf buffer : buffers) {
      if (buffer.refCnt() > 0) {
        buffer.release();
      }
    }
  }

  private static byte[] compressiblePayload() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      sb.append("The quick brown fox jumps over the lazy dog ").append(i % 4).append('\n');
    }
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Records the buffers allocated after {@link #startTracking(int)}, and caps their capacity to
   * make a compressor run out of room. Heap buffers have a non-zero {@code arrayOffset()}, as
   * pooled buffers and decoded frame slices do.
   */
  private static class TrackingAllocator extends AbstractByteBufAllocator {

    // The slice must end at the array end: heap compressors bound their output by the array.
    private static final int HEAP_ARRAY_OFFSET = 7;

    final List<ByteBuf> allocated = new ArrayList<ByteBuf>();
    private boolean tracking;
    private int capacityCap = Integer.MAX_VALUE;

    TrackingAllocator() {
      super(false);
    }

    void startTracking(int capacityCap) {
      this.tracking = true;
      this.capacityCap = capacityCap;
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
      int capacity = Math.min(initialCapacity, capacityCap);
      int arrayLength = HEAP_ARRAY_OFFSET + capacity;
      return track(
          new UnpooledHeapByteBuf(this, arrayLength, arrayLength)
              .slice(HEAP_ARRAY_OFFSET, capacity)
              .clear());
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
      return track(
          new UnpooledDirectByteBuf(
              this, Math.min(initialCapacity, capacityCap), Math.min(maxCapacity, capacityCap)));
    }

    @Override
    public boolean isDirectBufferPooled() {
      return false;
    }

    private ByteBuf track(ByteBuf buf) {
      if (tracking) {
        allocated.add(buf);
      }
      return buf;
    }
  }
}
