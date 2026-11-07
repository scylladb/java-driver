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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Tablet;
import org.junit.Test;

public class TabletRoutingTest {
  @Test
  public void should_sample_version_including_zero_and_unsigned_high_bits() {
    Tablet tablet = mock(Tablet.class);
    for (long version : new long[] {0, -1, Long.MIN_VALUE, 0x0123456789abcdefL}) {
      when(tablet.getTabletVersion()).thenReturn(version);
      for (int i = 0; i < 256; i++) {
        int block = Byte.toUnsignedInt(TabletRouting.versionBlock(tablet));
        int index = block >>> 4;
        assertThat(block & 15).isEqualTo((int) ((version >>> (index * 4)) & 15));
      }
    }
  }
}
