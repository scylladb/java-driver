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
package com.datastax.dse.driver.api.core.type.codec;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.api.core.type.DseDataTypes;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.Immutable;

/** @deprecated DSE geometry and DateRange codecs are no longer supported. */
@Deprecated
public class DseTypeCodecs extends TypeCodecs {

  public static final TypeCodec<LineString> LINE_STRING =
      new UnsupportedCodec<>(LineString.class, DseDataTypes.LINE_STRING);
  public static final TypeCodec<Point> POINT =
      new UnsupportedCodec<>(Point.class, DseDataTypes.POINT);
  public static final TypeCodec<Polygon> POLYGON =
      new UnsupportedCodec<>(Polygon.class, DseDataTypes.POLYGON);
  public static final TypeCodec<DateRange> DATE_RANGE =
      new UnsupportedCodec<>(DateRange.class, DseDataTypes.DATE_RANGE);

  @Immutable
  private static final class UnsupportedCodec<T> implements TypeCodec<T> {

    private final GenericType<T> javaType;
    private final DataType cqlType;

    private UnsupportedCodec(Class<T> javaType, DataType cqlType) {
      this.javaType = GenericType.of(javaType);
      this.cqlType = cqlType;
    }

    @NonNull
    @Override
    public GenericType<T> getJavaType() {
      return javaType;
    }

    @NonNull
    @Override
    public DataType getCqlType() {
      return cqlType;
    }

    @Nullable
    @Override
    public ByteBuffer encode(@Nullable T value, @NonNull ProtocolVersion protocolVersion) {
      throw unsupported();
    }

    @Nullable
    @Override
    public T decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      throw unsupported();
    }

    @NonNull
    @Override
    public String format(@Nullable T value) {
      throw unsupported();
    }

    @Nullable
    @Override
    public T parse(@Nullable String value) {
      throw unsupported();
    }

    private UnsupportedOperationException unsupported() {
      return new UnsupportedOperationException(
          "DSE geometry and DateRange codecs are no longer supported");
    }
  }
}
