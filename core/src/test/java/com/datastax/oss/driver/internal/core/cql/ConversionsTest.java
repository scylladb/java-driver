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
package com.datastax.oss.driver.internal.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Test;

public class ConversionsTest {
  @Test
  public void should_find_pk_indices_if_all_bound() {
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk"))).containsExactly(0);
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk", "c")))
        .containsExactly(0);
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("c", "pk")))
        .containsExactly(1);
    assertThat(
            Conversions.findIndices(
                partitionKey("pk1", "pk2", "pk3"),
                variables("c1", "pk2", "pk3", "c2", "pk1", "c3")))
        .containsExactly(4, 1, 2);
  }

  @Test
  public void should_use_first_pk_index_if_bound_multiple_times() {
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk", "pk")))
        .containsExactly(0);
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("pk", "c1", "pk", "c2")))
        .containsExactly(0);
    assertThat(
            Conversions.findIndices(
                partitionKey("pk1", "pk2", "pk3"),
                variables("c1", "pk2", "pk3", "c2", "pk1", "c3", "pk1", "pk2")))
        .containsExactly(4, 1, 2);
  }

  @Test
  public void should_return_empty_pk_indices_if_at_least_one_component_not_bound() {
    assertThat(Conversions.findIndices(partitionKey("pk"), variables("c1", "c2"))).isEmpty();
    assertThat(
            Conversions.findIndices(
                partitionKey("pk1", "pk2", "pk3"), variables("c1", "pk2", "c2", "pk1", "c3")))
        .isEmpty();
  }

  private List<ColumnMetadata> partitionKey(String... columnNames) {
    ImmutableList.Builder<ColumnMetadata> columns =
        ImmutableList.builderWithExpectedSize(columnNames.length);
    for (String columnName : columnNames) {
      ColumnMetadata column = mock(ColumnMetadata.class);
      when(column.getName()).thenReturn(CqlIdentifier.fromInternal(columnName));
      columns.add(column);
    }
    return columns.build();
  }

  private ColumnDefinitions variables(String... columnNames) {
    ImmutableList.Builder<ColumnDefinition> columns =
        ImmutableList.builderWithExpectedSize(columnNames.length);
    for (String columnName : columnNames) {
      ColumnDefinition column = mock(ColumnDefinition.class);
      when(column.getName()).thenReturn(CqlIdentifier.fromInternal(columnName));
      columns.add(column);
    }
    return DefaultColumnDefinitions.valueOf(columns.build());
  }

  /**
   * The invariant that makes the driver immune to CUSTOMER-583: an EXECUTE carries its values
   * positionally, so the name the server synthesized for an anonymous marker never travels back to
   * the coordinator and cannot be re-resolved there. Everything else in this area only guards the
   * local name-to-index lookup; if this branch ever started sending named values, that lookup would
   * be bypassed and a server that respelled a marker between PREPARE and EXECUTE would break
   * applications again.
   */
  @Test
  public void should_send_bound_statement_values_positionally() {
    List<ByteBuffer> values =
        ImmutableList.of(ByteBuffer.allocate(1), ByteBuffer.allocate(2), ByteBuffer.allocate(3));

    Message message = Conversions.toMessage(boundStatement(values), profile(), context());

    assertThat(message).isInstanceOf(Execute.class);
    Execute execute = (Execute) message;
    assertThat(execute.options.namedValues).isEmpty();
    assertThat(execute.options.positionalValues).isEqualTo(values);
  }

  private BoundStatement boundStatement(List<ByteBuffer> values) {
    // Built before the stubbing below: variables() mocks in turn, and nesting that inside a when()
    // argument leaves Mockito with an unfinished stubbing.
    ColumnDefinitions resultSetDefinitions = variables("v");
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.getId()).thenReturn(ByteBuffer.allocate(4));
    when(preparedStatement.getResultSetDefinitions()).thenReturn(resultSetDefinitions);
    BoundStatement boundStatement = mock(BoundStatement.class);
    when(boundStatement.getPreparedStatement()).thenReturn(preparedStatement);
    when(boundStatement.getValues()).thenReturn(values);
    when(boundStatement.getQueryTimestamp()).thenReturn(Statement.NO_DEFAULT_TIMESTAMP);
    when(boundStatement.getNowInSeconds()).thenReturn(Statement.NO_NOW_IN_SECONDS);
    return boundStatement;
  }

  private DriverExecutionProfile profile() {
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)).thenReturn("LOCAL_ONE");
    when(profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE)).thenReturn(5000);
    when(profile.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY)).thenReturn("SERIAL");
    return profile;
  }

  private InternalDriverContext context() {
    ProtocolVersionRegistry protocolVersionRegistry = mock(ProtocolVersionRegistry.class);
    when(protocolVersionRegistry.supports(any(), any())).thenReturn(true);
    InternalDriverContext context = mock(InternalDriverContext.class);
    when(context.getConsistencyLevelRegistry()).thenReturn(new DefaultConsistencyLevelRegistry());
    when(context.getTimestampGenerator()).thenReturn(mock(TimestampGenerator.class));
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(context.getProtocolVersion()).thenReturn(DefaultProtocolVersion.V4);
    when(context.getProtocolVersionRegistry()).thenReturn(protocolVersionRegistry);
    return context;
  }
}
