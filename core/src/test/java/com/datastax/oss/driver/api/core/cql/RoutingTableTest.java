package com.datastax.oss.driver.api.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultBoundStatement;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Test;

public class RoutingTableTest {

  private static final CqlIdentifier ORDERS = CqlIdentifier.fromCql("orders");
  private static final CqlIdentifier USERS = CqlIdentifier.fromCql("users");

  @Test
  public void should_infer_bound_routing_table_from_variables() {
    BoundStatement statement =
        newBoundStatement(definitions(ORDERS), EmptyColumnDefinitions.INSTANCE);

    assertThat(statement.getRoutingTable()).isEqualTo(ORDERS);
  }

  @Test
  public void should_infer_bound_routing_table_from_result_definitions_when_variables_empty() {
    BoundStatement statement =
        newBoundStatement(EmptyColumnDefinitions.INSTANCE, definitions(ORDERS));

    assertThat(statement.getRoutingTable()).isEqualTo(ORDERS);
  }

  @Test
  public void should_return_null_bound_routing_table_when_no_metadata() {
    BoundStatement statement =
        newBoundStatement(EmptyColumnDefinitions.INSTANCE, EmptyColumnDefinitions.INSTANCE);

    assertThat(statement.getRoutingTable()).isNull();
  }

  @Test
  public void should_prefer_explicit_bound_routing_table() {
    BoundStatement statement =
        newBoundStatement(definitions(ORDERS), EmptyColumnDefinitions.INSTANCE)
            .setRoutingTable(USERS);

    assertThat(statement.getRoutingTable()).isEqualTo(USERS);
    assertThat(statement.setRoutingTable((CqlIdentifier) null).getRoutingTable()).isEqualTo(ORDERS);
  }

  @Test
  public void should_set_simple_statement_routing_table() {
    SimpleStatement statement = SimpleStatement.newInstance("SELECT * FROM ks.orders");

    assertThat(statement.getRoutingTable()).isNull();
    assertThat(statement.setRoutingTable("orders").getRoutingTable()).isEqualTo(ORDERS);
    assertThat(
            SimpleStatement.builder("SELECT * FROM ks.orders")
                .setRoutingTable(ORDERS)
                .build()
                .getRoutingTable())
        .isEqualTo(ORDERS);
  }

  @Test
  public void should_prefer_explicit_batch_routing_table() {
    BoundStatement child = newBoundStatement(definitions(ORDERS), EmptyColumnDefinitions.INSTANCE);
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED, child);

    assertThat(batch.getRoutingTable()).isEqualTo(ORDERS);
    assertThat(batch.setRoutingTable(USERS).getRoutingTable()).isEqualTo(USERS);
  }

  private static ColumnDefinitions definitions(CqlIdentifier table) {
    ColumnDefinition definition = mock(ColumnDefinition.class);
    when(definition.getTable()).thenReturn(table);
    ColumnDefinitions definitions = mock(ColumnDefinitions.class);
    when(definitions.size()).thenReturn(1);
    when(definitions.get(0)).thenReturn(definition);
    return definitions;
  }

  private static BoundStatement newBoundStatement(
      ColumnDefinitions variables, ColumnDefinitions results) {
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.getVariableDefinitions()).thenReturn(variables);
    when(preparedStatement.getResultSetDefinitions()).thenReturn(results);
    return new DefaultBoundStatement(
        preparedStatement,
        variables,
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
        null,
        null,
        null,
        CodecRegistry.DEFAULT,
        DefaultProtocolVersion.DEFAULT,
        null,
        Statement.NO_NOW_IN_SECONDS,
        null);
  }
}
