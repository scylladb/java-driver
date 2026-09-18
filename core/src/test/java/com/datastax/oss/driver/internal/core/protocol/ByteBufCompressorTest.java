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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.jpountz.lz4.LZ4Exception;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.xerial.snappy.SnappyError;

/**
 * Covers {@link ByteBufCompressor} and both built-in implementations. {@link
 * BuiltInCompressorsTest} only covers the factory lookup, so nothing exercised the compression
 * itself.
 */
@RunWith(DataProviderRunner.class)
public class ByteBufCompressorTest {

  /** Repetitive on purpose, so both algorithms actually shrink it. */
  private static final byte[] PAYLOAD = payload();

  /** Big enough for LZ4's 4-byte length prefix, far too small for the frame that follows. */
  private static final int UNDERSIZED_OUTPUT = 8;

  /** Released from {@link #releaseBuffers()} so a failing assertion cannot leak a direct buffer. */
  private TrackingAllocator allocator;

  @Before
  public void setup() {
    allocator = new TrackingAllocator();
  }

  @After
  public void releaseBuffers() {
    allocator.releaseAll();
  }

  private static byte[] payload() {
    byte[] bytes = new byte[512];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (i % 8);
    }
    return bytes;
  }

  @DataProvider
  public static Object[][] compressors() {
    return new Object[][] {
      {"snappy", new SnappyCompressor(Mockito.mock(DriverContext.class))},
      {"lz4", new Lz4Compressor("test")},
    };
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_heap_buffer(String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compress(input);
    assertThat(compressed.isDirect()).isFalse();
    // PAYLOAD is repetitive, so a compressor that returned an uncompressed copy would fail here
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    ByteBuf decompressed = compressor.decompress(compressed);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_direct_buffer(String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.directBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compress(input);
    assertThat(compressed.isDirect()).isTrue();
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    ByteBuf decompressed = compressor.decompress(compressed);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_heap_buffer_without_length(
      String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compressWithoutLength(input);
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    ByteBuf decompressed = compressor.decompressWithoutLength(compressed, PAYLOAD.length);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_round_trip_a_direct_buffer_without_length(
      String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.directBuffer().writeBytes(PAYLOAD);

    ByteBuf compressed = compressor.compressWithoutLength(input);
    assertThat(compressed.readableBytes()).isLessThan(PAYLOAD.length);
    ByteBuf decompressed = compressor.decompressWithoutLength(compressed, PAYLOAD.length);

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  @Test
  @UseDataProvider("compressors")
  public void should_consume_the_whole_input(String name, ByteBufCompressor compressor) {
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);

    compressor.compress(input);

    // Every implementation advances the reader index to the writer index
    assertThat(input.readableBytes()).isZero();
  }

  @Test
  @UseDataProvider("compressors")
  public void should_report_its_algorithm(String name, ByteBufCompressor compressor) {
    assertThat(compressor.algorithm()).isEqualTo(name);
  }

  /**
   * LZ4 writes the uncompressed length ahead of the frame and reads it back; Snappy does not, and
   * returns a bogus length that its decompress path ignores.
   */
  @Test
  public void should_prepend_the_uncompressed_length_for_lz4_only() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));

    ByteBuf lz4WithLength = lz4.compress(allocator.heapBuffer().writeBytes(PAYLOAD));
    ByteBuf lz4WithoutLength =
        lz4.compressWithoutLength(allocator.heapBuffer().writeBytes(PAYLOAD));
    ByteBuf snappyWithLength = snappy.compress(allocator.heapBuffer().writeBytes(PAYLOAD));
    ByteBuf snappyWithoutLength =
        snappy.compressWithoutLength(allocator.heapBuffer().writeBytes(PAYLOAD));

    assertThat(lz4WithLength.readableBytes()).isEqualTo(lz4WithoutLength.readableBytes() + 4);
    assertThat(lz4WithLength.getInt(lz4WithLength.readerIndex())).isEqualTo(PAYLOAD.length);
    assertThat(lz4.readUncompressedLength(lz4WithLength)).isEqualTo(PAYLOAD.length);

    assertThat(snappyWithLength.readableBytes()).isEqualTo(snappyWithoutLength.readableBytes());
    assertThat(snappy.readUncompressedLength(snappyWithLength)).isEqualTo(-1);
  }

  @Test
  public void should_reject_a_heap_frame_that_is_not_snappy() {
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));
    ByteBuf garbage = allocator.heapBuffer().writeBytes(notSnappy());

    assertThatThrownBy(() -> snappy.decompress(garbage))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not appear to be Snappy compressed");
  }

  @Test
  public void should_reject_a_direct_frame_that_is_not_snappy() {
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));
    ByteBuf garbage = allocator.directBuffer().writeBytes(notSnappy());

    assertThatThrownBy(() -> snappy.decompress(garbage))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not appear to be Snappy compressed");
  }

  /**
   * Trailing bytes make the frame longer than what LZ4 actually reads, which is the mismatch the
   * decompress path guards against. Also proves the output buffer is released on that path rather
   * than leaked.
   */
  @Test
  public void should_reject_a_heap_frame_whose_length_does_not_match() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBuf compressed = lz4.compressWithoutLength(allocator.heapBuffer().writeBytes(PAYLOAD));
    compressed.writeBytes(new byte[] {1, 2, 3, 4});

    assertThatThrownBy(() -> lz4.decompressWithoutLength(compressed, PAYLOAD.length))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Compressed lengths mismatch");

    assertThat(allocator.lastAllocated().refCnt()).isZero();
  }

  @Test
  public void should_reject_a_direct_frame_whose_length_does_not_match() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBuf compressed = lz4.compressWithoutLength(allocator.directBuffer().writeBytes(PAYLOAD));
    compressed.writeBytes(new byte[] {1, 2, 3, 4});

    assertThatThrownBy(() -> lz4.decompressWithoutLength(compressed, PAYLOAD.length))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Compressed lengths mismatch");

    assertThat(allocator.lastAllocated().refCnt()).isZero();
  }

  /**
   * An output buffer too small for the frame is the only thing that makes LZ4 throw from inside the
   * compress-side {@code try}, so it is the only way to reach that {@code catch}. This and its heap
   * twin match on lz4-java's own wording, which a bump can reword; it is what proves the failure
   * came from the compressor rather than the undersized allocation itself.
   */
  @Test
  public void should_release_the_output_buffer_when_direct_compression_fails() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBuf input = allocator.directBuffer().writeBytes(PAYLOAD);
    allocator.undersizeNextAllocation(UNDERSIZED_OUTPUT);

    assertThatThrownBy(() -> lz4.compress(input))
        .isInstanceOf(LZ4Exception.class)
        .hasMessageContaining("maxDestLen is too small");

    assertThat(allocator.undersizedBuffer().refCnt()).isZero();
  }

  @Test
  public void should_release_the_output_buffer_when_heap_compression_fails() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBuf input = allocator.heapBuffer().writeBytes(PAYLOAD);
    allocator.undersizeNextAllocation(UNDERSIZED_OUTPUT);

    assertThatThrownBy(() -> lz4.compress(input))
        .isInstanceOf(LZ4Exception.class)
        .hasMessageContaining("maxDestLen is too small");

    assertThat(allocator.undersizedBuffer().refCnt()).isZero();
  }

  /**
   * A composite is the only input that reports more than one NIO buffer, which is the arm of {@link
   * ByteBufCompressor#inputNioBuffer} the round trips above never take. LZ4 only: a composite hands
   * back a heap copy, which Snappy's native path rejects (#1090).
   */
  @Test
  public void should_compress_a_composite_direct_buffer_with_lz4() {
    Lz4Compressor lz4 = new Lz4Compressor("test");
    ByteBufPrimitiveCodec codec = new ByteBufPrimitiveCodec(allocator);
    int half = PAYLOAD.length / 2;
    ByteBuf composite =
        codec.concat(
            allocator.directBuffer().writeBytes(PAYLOAD, 0, half),
            allocator.directBuffer().writeBytes(PAYLOAD, half, half));

    // concat degrades to a plain duplicate when either side is unreadable, which would quietly
    // exercise the single-buffer arm instead. All-direct components route to compressDirect.
    assertThat(composite.nioBufferCount()).isGreaterThan(1);
    assertThat(composite.isDirect()).isTrue();

    ByteBuf decompressed;
    try {
      ByteBuf compressed = lz4.compress(composite);
      decompressed = lz4.decompress(compressed);
    } finally {
      // The allocator tracks what it allocates, and a composite is not one of those, so releaseAll
      // would free the two components while the composite still owned them. Release it here.
      composite.release();
    }

    assertThat(ByteBufUtil.getBytes(decompressed)).isEqualTo(PAYLOAD);
  }

  /**
   * The same input on the other algorithm. inputNioBuffer hands a heap copy to Snappy's native
   * path, which rejects it; filed as scylladb/java-driver#1090 and pinned here so a change to the
   * routing predicate, or a snappy-java bump, cannot land silently. The direct output buffer
   * allocated before the try leaks with it, because catch (IOException) does not catch an Error --
   * scylladb/java-driver#1112. Only releaseAll() keeps that invisible here.
   */
  @Test
  public void should_reject_a_composite_direct_buffer_with_snappy() {
    SnappyCompressor snappy = new SnappyCompressor(Mockito.mock(DriverContext.class));
    ByteBufPrimitiveCodec codec = new ByteBufPrimitiveCodec(allocator);
    int half = PAYLOAD.length / 2;
    ByteBuf composite =
        codec.concat(
            allocator.directBuffer().writeBytes(PAYLOAD, 0, half),
            allocator.directBuffer().writeBytes(PAYLOAD, half, half));

    try {
      assertThat(composite.nioBufferCount()).isGreaterThan(1);
      assertThatThrownBy(() -> snappy.compress(composite)).isInstanceOf(SnappyError.class);
    } finally {
      composite.release();
    }
  }

  private static byte[] notSnappy() {
    byte[] bytes = new byte[64];
    Arrays.fill(bytes, (byte) 0xFF);
    return bytes;
  }

  /**
   * Hands out the buffers the compressors allocate internally, so a test can assert one was
   * released, and cleans up everything afterwards.
   */
  private static class TrackingAllocator extends AbstractByteBufAllocator {

    private static final int NO_CAP = -1;

    private final List<ByteBuf> allocated = new ArrayList<>();
    private int nextCapacityCap = NO_CAP;
    private ByteBuf undersized;

    TrackingAllocator() {
      super(false);
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
      return newBuffer(false, initialCapacity, maxCapacity);
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
      return newBuffer(true, initialCapacity, maxCapacity);
    }

    @Override
    public boolean isDirectBufferPooled() {
      return false;
    }

    // Pass `this` as the allocator so buf.alloc() leads back here: the compressors allocate their
    // output from the input buffer's own allocator, which is what makes them observable.
    private ByteBuf newBuffer(boolean direct, int initialCapacity, int maxCapacity) {
      boolean capped = nextCapacityCap != NO_CAP;
      int capacity = capped ? Math.min(nextCapacityCap, initialCapacity) : initialCapacity;
      // Pin maxCapacity too, otherwise netty would silently grow the buffer back to what was asked.
      int max = capped ? capacity : maxCapacity;
      nextCapacityCap = NO_CAP;
      ByteBuf buf =
          direct
              ? new UnpooledDirectByteBuf(this, capacity, max)
              : new UnpooledHeapByteBuf(this, capacity, max);
      if (capped) {
        undersized = buf;
      }
      allocated.add(buf);
      return buf;
    }

    ByteBuf lastAllocated() {
      return allocated.get(allocated.size() - 1);
    }

    /**
     * Makes the next allocation only {@code capacity} bytes wide, so the compressor's output buffer
     * is too small for the frame it is about to write. Opt-in per test on purpose: Snappy's native
     * compress does no bounds checking, so it must never be handed an undersized buffer.
     */
    void undersizeNextAllocation(int capacity) {
      nextCapacityCap = capacity;
    }

    /** The buffer handed out for the request capped by {@link #undersizeNextAllocation(int)}. */
    ByteBuf undersizedBuffer() {
      return undersized;
    }

    void releaseAll() {
      for (ByteBuf buf : allocated) {
        if (buf.refCnt() > 0) {
          buf.release();
        }
      }
    }
  }
}
