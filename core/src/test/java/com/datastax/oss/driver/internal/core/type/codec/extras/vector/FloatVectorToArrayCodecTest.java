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
package com.datastax.oss.driver.internal.core.type.codec.extras.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.ExtraTypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.internal.core.type.codec.CodecTestBase;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.junit.Before;
import org.junit.Test;

public class FloatVectorToArrayCodecTest extends CodecTestBase<float[]> {

  private static final int DIMENSIONS = 2;
  private static final float[] VECTOR = {1.0f, 2.5f};

  /**
   * Two big-endian floats, no length prefix — the same wire form {@code VectorCodecTest} pins for a
   * float vector.
   */
  private static final String VECTOR_HEX = "0x3f80000040200000";

  private static final VectorType FLOAT_VECTOR = new DefaultVectorType(DataTypes.FLOAT, DIMENSIONS);

  @Before
  public void setup() {
    codec = ExtraTypeCodecs.floatVectorToArray(DIMENSIONS);
  }

  @Test
  public void should_encode() {
    assertThat(encode(VECTOR)).isEqualTo(VECTOR_HEX);
  }

  @Test
  public void should_encode_null() {
    assertThat(encode(null)).isNull();
  }

  @Test
  public void should_decode() {
    assertThat(decode(VECTOR_HEX)).isEqualTo(VECTOR);
  }

  /**
   * A contract violation, not a design choice: {@code TypeCodec#decode} requires null and empty
   * buffers to be handled gracefully with no exception, {@code VectorCodec} returns null and the
   * ancestor {@code AbstractPrimitiveListToArrayCodec} returns an empty array. Every CQL NULL
   * vector cell therefore throws out of {@code row.get(...)}. Filed as scylladb/java-driver#1114,
   * pinned here until it is fixed.
   */
  @Test
  public void should_throw_when_decoding_null_buffer() {
    assertThatThrownBy(() -> codec.decode(null, ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null and must have non-zero remaining bytes");
  }

  @Test
  public void should_throw_when_decoding_empty_buffer() {
    assertThatThrownBy(() -> codec.decode(ByteBuffer.allocate(0), ProtocolVersion.DEFAULT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null and must have non-zero remaining bytes");
  }

  @Test
  public void should_not_consume_the_callers_buffer() {
    ByteBuffer bytes = Bytes.fromHexString(VECTOR_HEX);

    codec.decode(bytes, ProtocolVersion.DEFAULT);

    assertThat(bytes.position()).isZero();
    assertThat(bytes.remaining()).isEqualTo(DIMENSIONS * 4);
  }

  /**
   * The decode loop trusts the declared dimensions; a short buffer underflows rather than fails.
   */
  @Test
  public void should_underflow_when_buffer_holds_fewer_elements_than_dimensions() {
    assertThatThrownBy(
            () -> codec.decode(Bytes.fromHexString("0x3f800000"), ProtocolVersion.DEFAULT))
        .isInstanceOf(BufferUnderflowException.class);
  }

  /**
   * encode() sizes its output from the array it is handed, while decode() always reads exactly
   * {@code cqlType.getDimensions()} elements, so neither direction validates the length. {@code
   * VectorCodec} rejects both, which is why this is filed as scylladb/java-driver#1115 rather than
   * recorded as intended behaviour.
   */
  @Test
  public void should_truncate_an_array_longer_than_the_declared_dimensions() {
    ByteBuffer encoded = codec.encode(new float[] {1.0f, 2.5f, 9.0f}, ProtocolVersion.DEFAULT);

    // Three floats went out, even though the codec declares two dimensions
    assertThat(encoded.remaining()).isEqualTo(3 * Float.BYTES);
    // ... and the third silently disappears on the way back
    assertThat(codec.decode(encoded, ProtocolVersion.DEFAULT)).isEqualTo(VECTOR);
  }

  /** The other half of #1115: a short array is written as-is and only fails on the way back. */
  @Test
  public void should_under_write_an_array_shorter_than_the_declared_dimensions() {
    ByteBuffer encoded = codec.encode(new float[] {1.0f}, ProtocolVersion.DEFAULT);

    // One float went out for a vector<float, 2>, with no complaint ...
    assertThat(encoded.remaining()).isEqualTo(Float.BYTES);
    // ... and it is unreadable by the codec that wrote it
    assertThatThrownBy(() -> codec.decode(encoded, ProtocolVersion.DEFAULT))
        .isInstanceOf(BufferUnderflowException.class);
  }

  @Test
  public void should_report_java_and_cql_types() {
    assertThat(codec.getJavaType()).isEqualTo(GenericType.of(float[].class));
    assertThat(codec.getCqlType()).isInstanceOf(VectorType.class);
    assertThat(((VectorType) codec.getCqlType()).getDimensions()).isEqualTo(DIMENSIONS);
    assertThat(((VectorType) codec.getCqlType()).getElementType()).isEqualTo(DataTypes.FLOAT);
  }

  @Test
  public void should_accept_float_array_class() {
    assertThat(codec.accepts(float[].class)).isTrue();
    assertThat(codec.accepts(double[].class)).isFalse();
    assertThat(codec.accepts(Float[].class)).isFalse();
  }

  @Test
  public void should_accept_float_array_value() {
    assertThat(codec.accepts(VECTOR)).isTrue();
    assertThat(codec.accepts(new double[] {1.0})).isFalse();
    assertThat(codec.accepts("not a vector")).isFalse();
  }

  @Test
  public void should_accept_matching_vector_type_only() {
    // Dimension-exact, which is what keeps the registry from handing this codec a column whose
    // width does not match -- see should_truncate_an_array_longer_than_the_declared_dimensions
    assertThat(codec.accepts(FLOAT_VECTOR)).isTrue();
    assertThat(codec.accepts(new DefaultVectorType(DataTypes.FLOAT, DIMENSIONS - 1))).isFalse();
    assertThat(codec.accepts(new DefaultVectorType(DataTypes.FLOAT, DIMENSIONS + 1))).isFalse();
    assertThat(codec.accepts(new DefaultVectorType(DataTypes.DOUBLE, DIMENSIONS))).isFalse();
    assertThat(codec.accepts(DataTypes.FLOAT)).isFalse();
  }

  @Test
  public void should_accept_float_array_generic_type() {
    assertThat(codec.accepts(GenericType.of(float[].class))).isTrue();
    assertThat(codec.accepts(GenericType.of(double[].class))).isFalse();
    assertThat(codec.accepts(GenericType.listOf(Float.class))).isFalse();
  }

  @Test
  public void should_throw_when_accepts_is_given_null() {
    assertThatThrownBy(() -> codec.accepts((Class<?>) null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> codec.accepts((Object) null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void should_format() {
    assertThat(format(VECTOR)).isEqualTo("[1.0, 2.5]");
  }

  @Test
  public void should_format_null() {
    assertThat(format(null)).isEqualTo("NULL");
  }

  @Test
  public void should_parse() {
    assertThat(parse("[1.0, 2.5]")).isEqualTo(VECTOR);
  }

  @Test
  public void should_throw_when_parsing_null_string() {
    assertThatThrownBy(() -> parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot create float array from null string");
  }

  @Test
  public void should_throw_when_parsing_empty_string() {
    assertThatThrownBy(() -> parse(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot create float array from empty string");
  }

  /**
   * {@code TypeCodec#parse} requires null, the empty string and "NULL" to be accepted and says not
   * to throw at all; this codec throws on all three, and format(null) emits exactly the "NULL" it
   * cannot read back. Filed as scylladb/java-driver#1113.
   */
  @Test
  public void should_throw_when_parsing_the_null_literal() {
    assertThatThrownBy(() -> parse("NULL")).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * Worse than the throws above: parse() strips the first and last character without checking that
   * the literal is bracketed, so an unbracketed one is silently mangled into valid-looking floats.
   * Also #1113.
   */
  @Test
  public void should_mangle_an_unbracketed_literal() {
    assertThat(parse("1.0, 2.5")).isEqualTo(new float[] {0.0f, 2.0f});
  }

  @Test
  public void should_reject_null_cql_type() {
    assertThatThrownBy(() -> new StubCodec(null, GenericType.of(String.class)))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("cqlType cannot be null");
  }

  @Test
  public void should_reject_null_array_type() {
    assertThatThrownBy(() -> new StubCodec(FLOAT_VECTOR, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("arrayType cannot be null");
  }

  @Test
  public void should_reject_a_java_type_that_is_not_an_array() {
    assertThatThrownBy(() -> new StubCodec(FLOAT_VECTOR, GenericType.of(String.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expecting Java array class");
  }

  /**
   * Reaches {@link AbstractVectorToArrayCodec}'s constructor guards with a non-array element type,
   * which {@link FloatVectorToArrayCodec} cannot do because it hard-codes {@code float[]}.
   */
  private static class StubCodec extends AbstractVectorToArrayCodec<String> {

    StubCodec(VectorType cqlType, GenericType<String> javaType) {
      super(cqlType, javaType);
    }

    @NonNull
    @Override
    protected String newInstance() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected int sizeOfComponentType() {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void serializeElement(
        @NonNull ByteBuffer output,
        @NonNull String array,
        int index,
        @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void deserializeElement(
        @NonNull ByteBuffer input,
        @NonNull String array,
        int index,
        @NonNull ProtocolVersion protocolVersion) {
      throw new UnsupportedOperationException();
    }

    @NonNull
    @Override
    public String format(@Nullable String value) {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public String parse(@Nullable String value) {
      throw new UnsupportedOperationException();
    }
  }
}
