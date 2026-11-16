/*
 * Copyright (C) 2020 ScyllaDB
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapExtensionReader {
  private static final Logger logger = LoggerFactory.getLogger(MapExtensionReader.class);

  private final ByteBuffer rawData;

  public MapExtensionReader(ByteBuffer rawData) {
    // Make a shallow copy, so that changing
    // position of rawData in MapExtensionReader
    // will not modify original rawData
    // (but without the cost of copying the
    // underlying data).
    //
    // Also read in LITTLE_ENDIAN.
    this.rawData = Preconditions.checkNotNull(rawData).slice().order(ByteOrder.LITTLE_ENDIAN);
  }

  public Map<String, String> parse() {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();

    int numElements = parseInt();
    Preconditions.checkArgument(numElements >= 0);

    for (int i = 0; i < numElements; i++) {
      try {
        String key = parseString();
        String value = parseString();
        builder.put(key, value);
      } catch (UnsupportedEncodingException ex) {
        logger.warn("Encoding exception while parsing extension map metadata", ex);
      }
    }

    return builder.build();
  }

  private String parseString() throws UnsupportedEncodingException {
    int length = parseInt();
    Preconditions.checkArgument(length >= 0, "Negative string length: %s", length);
    Preconditions.checkArgument(
        length <= rawData.remaining(),
        "String length %s exceeds %s remaining bytes",
        length,
        rawData.remaining());
    byte[] rawString = new byte[length];
    rawData.get(rawString);
    return new String(rawString, "UTF-8");
  }

  private int parseInt() {
    Preconditions.checkArgument(
        rawData.remaining() >= Integer.BYTES,
        "Expected 4 bytes for an integer, but only %s remain",
        rawData.remaining());
    // Parse little-endian 32-bit integer.
    return rawData.getInt();
  }
}
