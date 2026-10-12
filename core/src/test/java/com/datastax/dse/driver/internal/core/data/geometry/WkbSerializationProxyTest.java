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
package com.datastax.dse.driver.internal.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.oss.driver.internal.SerializationHelper;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@SuppressWarnings("deprecation")
public class WkbSerializationProxyTest {

  @Parameterized.Parameters(name = "{0}")
  public static Object[][] byteOrders() {
    return new Object[][] {{ByteOrder.LITTLE_ENDIAN}, {ByteOrder.BIG_ENDIAN}};
  }

  private final ByteOrder byteOrder;

  public WkbSerializationProxyTest(ByteOrder byteOrder) {
    this.byteOrder = byteOrder;
  }

  @Test
  public void should_deserialize_point_using_encoded_byte_order() {
    ByteBuffer wkb = newBuffer(21, 1);
    wkb.putDouble(1).putDouble(2);

    assertDeserializes(wkb, Point.fromCoordinates(1, 2));
  }

  @Test
  public void should_deserialize_line_string_using_encoded_byte_order() {
    ByteBuffer wkb = newBuffer(41, 2);
    wkb.putInt(2).putDouble(1).putDouble(2).putDouble(3).putDouble(4);

    assertDeserializes(wkb, LineString.fromWellKnownText("LINESTRING (1 2, 3 4)"));
  }

  @Test
  public void should_deserialize_polygon_using_encoded_byte_order() {
    ByteBuffer wkb = newBuffer(77, 3);
    wkb.putInt(1).putInt(4);
    wkb.putDouble(0).putDouble(0);
    wkb.putDouble(2).putDouble(0);
    wkb.putDouble(0).putDouble(2);
    wkb.putDouble(0).putDouble(0);

    assertDeserializes(wkb, Polygon.fromWellKnownText("POLYGON ((0 0, 2 0, 0 2, 0 0))"));
  }

  private ByteBuffer newBuffer(int size, int type) {
    ByteBuffer buffer = ByteBuffer.allocate(size).order(byteOrder);
    buffer.put((byte) (byteOrder == ByteOrder.LITTLE_ENDIAN ? 1 : 0)).putInt(type);
    return buffer;
  }

  private void assertDeserializes(ByteBuffer wkb, Geometry expected) {
    wkb.flip();
    Object restored =
        SerializationHelper.deserialize(
            SerializationHelper.serialize(new WkbSerializationProxy(wkb)));
    assertThat(restored).isEqualTo(expected);
    assertThat(SerializationHelper.serializeAndDeserialize(expected)).isEqualTo(expected);
  }
}
