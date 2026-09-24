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
package com.datastax.oss.driver.internal.core.adminrequest;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Queue;
import org.junit.Test;

public class AdminResultTest {

  @Test
  public void getColumnNames_should_return_all_column_names() {
    // Given
    ColumnSpec col1 =
        new ColumnSpec(
            "system",
            "local",
            "rpc_address",
            0,
            RawType.PRIMITIVES.get(ProtocolConstants.DataType.INET));
    ColumnSpec col2 =
        new ColumnSpec(
            "system",
            "local",
            "data_center",
            1,
            RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR));
    ColumnSpec col3 =
        new ColumnSpec(
            "system",
            "local",
            "host_id",
            2,
            RawType.PRIMITIVES.get(ProtocolConstants.DataType.UUID));
    RowsMetadata metadata = new RowsMetadata(ImmutableList.of(col1, col2, col3), null, null, null);
    Queue<List<ByteBuffer>> data = Lists.newLinkedList();
    Rows rows = new DefaultRows(metadata, data);

    AdminResult result = new AdminResult(rows, null, ProtocolVersion.DEFAULT);

    // When
    List<String> columnNames = result.getColumnNames();

    // Then
    // Order must match the server response (rpc_address, data_center, host_id)
    assertThat(columnNames).containsExactly("rpc_address", "data_center", "host_id");
  }

  @Test
  public void getColumnNames_should_return_empty_list_for_empty_metadata() {
    // Given
    RowsMetadata metadata = new RowsMetadata(ImmutableList.of(), null, null, null);
    Queue<List<ByteBuffer>> data = Lists.newLinkedList();
    Rows rows = new DefaultRows(metadata, data);

    AdminResult result = new AdminResult(rows, null, ProtocolVersion.DEFAULT);

    // When
    List<String> columnNames = result.getColumnNames();

    // Then
    assertThat(columnNames).isEmpty();
  }

  @Test
  public void getColumnNames_should_return_unmodifiable_list() {
    // Given
    ColumnSpec col =
        new ColumnSpec(
            "system",
            "local",
            "host_id",
            0,
            RawType.PRIMITIVES.get(ProtocolConstants.DataType.UUID));
    RowsMetadata metadata = new RowsMetadata(ImmutableList.of(col), null, null, null);
    Queue<List<ByteBuffer>> data = Lists.newLinkedList();
    Rows rows = new DefaultRows(metadata, data);

    AdminResult result = new AdminResult(rows, null, ProtocolVersion.DEFAULT);
    List<String> columnNames = result.getColumnNames();

    // When / Then
    assertThat(columnNames).containsExactly("host_id");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> columnNames.add("extra_column"))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
