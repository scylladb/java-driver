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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import org.junit.Test;

public class DefaultBatchStatementTest {

  @Test
  public void should_issue_log_warn_if_statement_have_consistency_level_set() {
    SimpleStatement simpleStatement =
        SimpleStatement.builder("SELECT * FROM some_table WHERE a = ?")
            .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
            .build();

    BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
    batchStatementBuilder.addStatement(simpleStatement);

    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(DefaultBatchStatement.class, Level.WARN);

    batchStatementBuilder.build();

    verify(logger.appender).doAppend(logger.loggingEventCaptor.capture());
    assertThat(
            logger.loggingEventCaptor.getAllValues().stream()
                .map(ILoggingEvent::getFormattedMessage))
        .contains(
            "You have submitted statement with non-default [serial] consistency level to the DefaultBatchStatement. "
                + "Be aware that [serial] consistency level of child statements is not preserved by the DefaultBatchStatement. "
                + "Use DefaultBatchStatement.setConsistencyLevel()/DefaultBatchStatement.setSerialConsistencyLevel() instead.");
  }

  @Test
  public void should_issue_log_warn_if_statement_have_serial_consistency_level_set() {
    SimpleStatement simpleStatement =
        SimpleStatement.builder("SELECT * FROM some_table WHERE a = ?")
            .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();

    BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
    batchStatementBuilder.addStatement(simpleStatement);

    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(DefaultBatchStatement.class, Level.WARN);

    batchStatementBuilder.build();

    verify(logger.appender).doAppend(logger.loggingEventCaptor.capture());
    assertThat(
            logger.loggingEventCaptor.getAllValues().stream()
                .map(ILoggingEvent::getFormattedMessage))
        .contains(
            "You have submitted statement with non-default [serial] consistency level to the DefaultBatchStatement. "
                + "Be aware that [serial] consistency level of child statements is not preserved by the DefaultBatchStatement. "
                + "Use DefaultBatchStatement.setConsistencyLevel()/DefaultBatchStatement.setSerialConsistencyLevel() instead.");
  }

  @Test
  public void should_not_issue_log_warn_if_statement_have_no_consistency_level_set() {
    SimpleStatement simpleStatement =
        SimpleStatement.builder("SELECT * FROM some_table WHERE a = ?").build();

    BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.LOGGED);
    batchStatementBuilder.addStatement(simpleStatement);

    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(DefaultBatchStatement.class, Level.WARN);

    batchStatementBuilder.build();

    verify(logger.appender, times(0)).doAppend(logger.loggingEventCaptor.capture());
  }

  @Test
  public void should_not_infer_lwt_status_from_serial_consistency_level_option() {
    BatchStatement batch =
        BatchStatement.builder(BatchType.LOGGED)
            .addStatement(SimpleStatement.newInstance("UPDATE foo SET v = ? WHERE pk = ?", 1, 1))
            .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
            .build();

    assertThat(batch.getRequestRoutingType()).isEqualTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();
  }

  @Test
  public void should_infer_lwt_status() {
    // SELECT is not allowed in practice but is sufficient for unit testing
    SimpleStatement simpleStatement =
        SimpleStatement.builder("SELECT * FROM some_table WHERE a = ?").build();
    BoundStatement lwtBoundStatement = mock(DefaultBoundStatement.class);
    when(lwtBoundStatement.isLWT()).thenReturn(true);
    when(lwtBoundStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.LWT);

    // Without LWT statements added
    BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
    batchStatementBuilder.addStatement(simpleStatement);
    BatchStatement batch = batchStatementBuilder.build();
    assertThat(batch.isLWT()).isFalse();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);

    // Check if implicitly set to true after adding LWT bound statement
    batchStatementBuilder.addStatement(lwtBoundStatement);
    assertThat(batchStatementBuilder.build().isLWT()).isTrue();

    // Check if explicit set overrides implicit resolution
    batchStatementBuilder.setRequestRoutingType(RequestRoutingType.REGULAR);
    batch = batchStatementBuilder.build();
    assertThat(batch.isLWT()).isFalse();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
    batchStatementBuilder.addStatement(simpleStatement);
    batchStatementBuilder.setRequestRoutingType(RequestRoutingType.LWT);
    batch = batchStatementBuilder.build();
    assertThat(batch.isLWT()).isTrue();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);

    // Check if explicit set remains after clear
    assertThat(batchStatementBuilder.build().clear().isLWT()).isTrue();

    // Similar checks without using builder
    batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.add(simpleStatement);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.add(lwtBoundStatement);
    assertThat(batch.isLWT()).isTrue();
    batch = batch.setRequestRoutingType(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.add(lwtBoundStatement);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.setRequestRoutingType(RequestRoutingType.LWT);
    assertThat(batch.isLWT()).isTrue();
    batch = batch.clear();
    assertThat(batch.isLWT()).isTrue();
    batch = batch.setRequestRoutingType(null);
    assertThat(batch.isLWT()).isFalse();

    assertThat(BatchStatement.newInstance(BatchType.UNLOGGED).isLWT()).isFalse();
    assertThat(BatchStatement.newInstance(BatchType.LOGGED).isLWT()).isFalse();
    assertThat(BatchStatement.newInstance(BatchType.COUNTER).isLWT()).isFalse();
    assertThat(BatchStatement.newInstance(BatchType.UNLOGGED, lwtBoundStatement).isLWT()).isTrue();
    assertThat(BatchStatement.newInstance(BatchType.LOGGED, lwtBoundStatement).isLWT()).isTrue();
    assertThat(BatchStatement.newInstance(BatchType.COUNTER, lwtBoundStatement).isLWT()).isTrue();
  }

  @Test
  public void should_handle_null_routing_type_in_empty_batch() {
    // Empty batch should return REGULAR (not null) and isLWT should be false
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    assertThat(batch.getRequestRoutingType()).isNotNull();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();

    // Same for other batch types
    batch = BatchStatement.newInstance(BatchType.LOGGED);
    assertThat(batch.getRequestRoutingType()).isNotNull();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();

    batch = BatchStatement.newInstance(BatchType.COUNTER);
    assertThat(batch.getRequestRoutingType()).isNotNull();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();
  }

  @Test
  public void should_handle_statements_with_null_routing_types() {
    // Create statements that return null routing type
    BoundStatement nullRoutingStatement1 = mock(DefaultBoundStatement.class);
    when(nullRoutingStatement1.isLWT()).thenReturn(false);
    when(nullRoutingStatement1.getRequestRoutingType()).thenReturn(null);

    BoundStatement nullRoutingStatement2 = mock(DefaultBoundStatement.class);
    when(nullRoutingStatement2.isLWT()).thenReturn(false);
    when(nullRoutingStatement2.getRequestRoutingType()).thenReturn(null);

    // Batch with only null routing type statements should return REGULAR
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(nullRoutingStatement1);
    batch = batch.add(nullRoutingStatement2);

    assertThat(batch.getRequestRoutingType()).isNotNull();
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();
  }

  @Test
  public void should_handle_mixed_null_and_non_null_routing_types() {
    // Create statements with different routing types
    BoundStatement nullRoutingStatement = mock(DefaultBoundStatement.class);
    when(nullRoutingStatement.isLWT()).thenReturn(false);
    when(nullRoutingStatement.getRequestRoutingType()).thenReturn(null);

    BoundStatement regularStatement = mock(DefaultBoundStatement.class);
    when(regularStatement.isLWT()).thenReturn(false);
    when(regularStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.REGULAR);

    BoundStatement lwtStatement = mock(DefaultBoundStatement.class);
    when(lwtStatement.isLWT()).thenReturn(true);
    when(lwtStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.LWT);

    // Test 1: null + regular -> REGULAR
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(nullRoutingStatement);
    batch = batch.add(regularStatement);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();

    // Test 2: null + LWT -> LWT (LWT should be detected)
    batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(nullRoutingStatement);
    batch = batch.add(lwtStatement);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);
    assertThat(batch.isLWT()).isTrue();

    // Test 3: regular + null + LWT -> LWT (LWT should be detected regardless of order)
    batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(regularStatement);
    batch = batch.add(nullRoutingStatement);
    batch = batch.add(lwtStatement);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);
    assertThat(batch.isLWT()).isTrue();

    // Test 4: LWT + null + regular -> LWT (order shouldn't matter)
    batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(lwtStatement);
    batch = batch.add(nullRoutingStatement);
    batch = batch.add(regularStatement);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);
    assertThat(batch.isLWT()).isTrue();
  }

  @Test
  public void should_handle_explicit_null_routing_type_override() {
    BoundStatement lwtStatement = mock(DefaultBoundStatement.class);
    when(lwtStatement.isLWT()).thenReturn(true);
    when(lwtStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.LWT);

    BoundStatement regularStatement = mock(DefaultBoundStatement.class);
    when(regularStatement.isLWT()).thenReturn(false);
    when(regularStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.REGULAR);

    // Test 1: Batch with LWT statement, then set routing type to null
    // Should fall back to inference and detect LWT
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(lwtStatement);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);
    assertThat(batch.isLWT()).isTrue();

    batch = batch.setRequestRoutingType(null);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);
    assertThat(batch.isLWT()).isTrue();

    // Test 2: Batch with regular statement, set routing type to null
    // Should infer REGULAR
    batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.add(regularStatement);
    batch = batch.setRequestRoutingType(null);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();

    // Test 3: Empty batch with explicit null routing type
    // Should return REGULAR
    batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    batch = batch.setRequestRoutingType(null);
    assertThat(batch.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
    assertThat(batch.isLWT()).isFalse();
  }

  @Test
  public void should_return_non_null_routing_type_consistently() {
    // Verify that getRequestRoutingType never returns null
    SimpleStatement simpleStatement =
        SimpleStatement.builder("SELECT * FROM some_table WHERE a = ?").build();

    BoundStatement lwtStatement = mock(DefaultBoundStatement.class);
    when(lwtStatement.isLWT()).thenReturn(true);
    when(lwtStatement.getRequestRoutingType()).thenReturn(RequestRoutingType.LWT);

    BoundStatement nullRoutingStatement = mock(DefaultBoundStatement.class);
    when(nullRoutingStatement.isLWT()).thenReturn(false);
    when(nullRoutingStatement.getRequestRoutingType()).thenReturn(null);

    // Test various batch configurations
    BatchStatement batch1 = BatchStatement.newInstance(BatchType.UNLOGGED);
    assertThat(batch1.getRequestRoutingType()).isNotNull();

    BatchStatement batch2 = batch1.add(simpleStatement);
    assertThat(batch2.getRequestRoutingType()).isNotNull();

    BatchStatement batch3 = batch2.add(lwtStatement);
    assertThat(batch3.getRequestRoutingType()).isNotNull();

    BatchStatement batch4 = batch3.setRequestRoutingType(null);
    assertThat(batch4.getRequestRoutingType()).isNotNull();

    BatchStatement batch5 =
        BatchStatement.newInstance(BatchType.UNLOGGED).add(nullRoutingStatement);
    assertThat(batch5.getRequestRoutingType()).isNotNull();
    assertThat(batch5.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);

    BatchStatement batch6 = batch5.setRequestRoutingType(RequestRoutingType.LWT);
    assertThat(batch6.getRequestRoutingType()).isNotNull();
    assertThat(batch6.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.LWT);

    BatchStatement batch7 = batch6.setRequestRoutingType(null);
    assertThat(batch7.getRequestRoutingType()).isNotNull();
    assertThat(batch7.getRequestRoutingType()).isEqualByComparingTo(RequestRoutingType.REGULAR);
  }
}
