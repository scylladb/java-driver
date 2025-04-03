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
  public void should_infer_lwt_status() {
    // SELECT is not allowed in practice but is sufficient for unit testing
    SimpleStatement simpleStatement =
        SimpleStatement.builder("SELECT * FROM some_table WHERE a = ?").build();
    BoundStatement lwtBoundStatement = mock(DefaultBoundStatement.class);
    when(lwtBoundStatement.isLWT()).thenReturn(true);

    // Without LWT statements added
    BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
    batchStatementBuilder.addStatement(simpleStatement);
    assertThat(batchStatementBuilder.build().isLWT()).isFalse();

    // Check if implicitly set to true after adding LWT bound statement
    batchStatementBuilder.addStatement(lwtBoundStatement);
    assertThat(batchStatementBuilder.build().isLWT()).isTrue();

    // Check if explicit set overrides implicit resolution
    batchStatementBuilder.setIsLWT(false);
    assertThat(batchStatementBuilder.build().isLWT()).isFalse();
    batchStatementBuilder = new BatchStatementBuilder(BatchType.UNLOGGED);
    batchStatementBuilder.addStatement(simpleStatement);
    batchStatementBuilder.setIsLWT(true);
    assertThat(batchStatementBuilder.build().isLWT()).isTrue();

    // Check if explicit set remains after clear
    assertThat(batchStatementBuilder.build().clear().isLWT()).isTrue();

    // Similar checks without using builder
    BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.add(simpleStatement);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.add(lwtBoundStatement);
    assertThat(batch.isLWT()).isTrue();
    batch = batch.setIsLWT(false);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.add(lwtBoundStatement);
    assertThat(batch.isLWT()).isFalse();
    batch = batch.setIsLWT(true);
    assertThat(batch.isLWT()).isTrue();
    batch = batch.clear();
    assertThat(batch.isLWT()).isTrue();
    batch = batch.setIsLWT(null);
    assertThat(batch.isLWT()).isFalse();

    assertThat(BatchStatement.newInstance(BatchType.UNLOGGED).isLWT()).isFalse();
    assertThat(BatchStatement.newInstance(BatchType.LOGGED).isLWT()).isFalse();
    assertThat(BatchStatement.newInstance(BatchType.COUNTER).isLWT()).isFalse();
    assertThat(BatchStatement.newInstance(BatchType.UNLOGGED, lwtBoundStatement).isLWT()).isTrue();
    assertThat(BatchStatement.newInstance(BatchType.LOGGED, lwtBoundStatement).isLWT()).isTrue();
    assertThat(BatchStatement.newInstance(BatchType.COUNTER, lwtBoundStatement).isLWT()).isTrue();
  }
}
