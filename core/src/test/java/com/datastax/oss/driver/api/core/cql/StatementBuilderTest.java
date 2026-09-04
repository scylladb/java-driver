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
package com.datastax.oss.driver.api.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultBoundStatement;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Test;

public class StatementBuilderTest {

  private static class MockSimpleStatementBuilder
      extends StatementBuilder<MockSimpleStatementBuilder, SimpleStatement> {

    public MockSimpleStatementBuilder() {
      super();
    }

    public MockSimpleStatementBuilder(SimpleStatement template) {
      super(template);
    }

    @NonNull
    @Override
    public SimpleStatement build() {

      SimpleStatement rv = mock(SimpleStatement.class);
      when(rv.isTracing()).thenReturn(this.tracing);
      when(rv.getRoutingKey()).thenReturn(this.routingKey);
      return rv;
    }
  }

  @Test
  public void should_handle_set_tracing_without_args() {

    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder();
    assertThat(builder.build().isTracing()).isFalse();
    builder.setTracing();
    assertThat(builder.build().isTracing()).isTrue();
  }

  @Test
  public void should_handle_set_tracing_with_args() {

    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder();
    assertThat(builder.build().isTracing()).isFalse();
    builder.setTracing(true);
    assertThat(builder.build().isTracing()).isTrue();
    builder.setTracing(false);
    assertThat(builder.build().isTracing()).isFalse();
  }

  @Test
  public void should_override_set_tracing_in_template() {

    SimpleStatement template = SimpleStatement.builder("select * from system.peers").build();
    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder(template);
    assertThat(builder.build().isTracing()).isFalse();
    builder.setTracing(true);
    assertThat(builder.build().isTracing()).isTrue();

    template = SimpleStatement.builder("select * from system.peers").setTracing().build();
    builder = new MockSimpleStatementBuilder(template);
    assertThat(builder.build().isTracing()).isTrue();
    builder.setTracing(false);
    assertThat(builder.build().isTracing()).isFalse();
  }

  @Test
  public void should_match_set_routing_key_vararg() {

    ByteBuffer buff1 = ByteBuffer.wrap("the quick brown fox".getBytes(Charsets.UTF_8));
    ByteBuffer buff2 = ByteBuffer.wrap("jumped over the lazy dog".getBytes(Charsets.UTF_8));

    Statement<?> expectedStmt =
        SimpleStatement.builder("select * from system.peers").build().setRoutingKey(buff1, buff2);

    MockSimpleStatementBuilder builder = new MockSimpleStatementBuilder();
    Statement<?> builderStmt = builder.setRoutingKey(buff1, buff2).build();
    assertThat(expectedStmt.getRoutingKey()).isEqualTo(builderStmt.getRoutingKey());

    /* Confirm that order matters here */
    builderStmt = builder.setRoutingKey(buff2, buff1).build();
    assertThat(expectedStmt.getRoutingKey()).isNotEqualTo(builderStmt.getRoutingKey());
  }

  @Test
  public void should_not_copy_inferred_simple_routing_type_as_explicit() {
    SimpleStatement serialStatement =
        SimpleStatement.builder("select * from test.foo")
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();

    assertThat(serialStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);

    SimpleStatement regularStatement =
        SimpleStatement.builder(serialStatement)
            .setConsistencyLevel(DefaultConsistencyLevel.ONE)
            .build();

    assertThat(regularStatement.getRequestRoutingType()).isNull();
  }

  @Test
  public void should_not_copy_inferred_bound_routing_type_as_explicit() {
    BoundStatement serialStatement = newRegularBoundStatement(DefaultConsistencyLevel.LOCAL_SERIAL);

    assertThat(serialStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);

    BoundStatement regularStatement =
        new BoundStatementBuilder(serialStatement)
            .setConsistencyLevel(DefaultConsistencyLevel.ONE)
            .build();

    assertThat(regularStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.REGULAR);
  }

  @Test
  public void should_not_copy_inferred_batch_routing_type_as_explicit() {
    BatchStatement serialStatement =
        BatchStatement.builder(BatchType.LOGGED)
            .setConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();

    assertThat(serialStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);

    BatchStatement regularStatement =
        BatchStatement.builder(serialStatement)
            .setConsistencyLevel(DefaultConsistencyLevel.ONE)
            .build();

    assertThat(regularStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.REGULAR);
  }

  private BoundStatement newRegularBoundStatement(DefaultConsistencyLevel consistencyLevel) {
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    ColumnDefinitions variableDefinitions = mock(ColumnDefinitions.class);
    when(preparedStatement.isLWT()).thenReturn(false);
    when(preparedStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.REGULAR);
    when(preparedStatement.getVariableDefinitions()).thenReturn(variableDefinitions);
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        new ByteBuffer[0],
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        consistencyLevel,
        null,
        null,
        CodecRegistry.DEFAULT,
        DefaultProtocolVersion.DEFAULT,
        null,
        Statement.NO_NOW_IN_SECONDS,
        null);
  }
}
