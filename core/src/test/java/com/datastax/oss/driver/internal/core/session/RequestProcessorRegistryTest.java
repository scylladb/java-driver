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
package com.datastax.oss.driver.internal.core.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import com.datastax.dse.driver.internal.core.graph.UnsupportedGraphRequestProcessor;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class RequestProcessorRegistryTest {

  private final GraphStatement<?> graphStatement = mock(GraphStatement.class);
  private final List<RequestProcessor<?, ?>> processors =
      BuiltInRequestProcessors.createDefaultProcessors(
          mock(DefaultDriverContext.class, RETURNS_DEEP_STUBS));

  @Test
  public void should_reject_unhandled_graph_requests_with_migration_guidance() throws Exception {
    assertThat(processors.get(processors.size() - 1))
        .isInstanceOf(UnsupportedGraphRequestProcessor.class);
    DefaultSession session = session(false);
    assertUnsupported(() -> session.execute(graphStatement, GraphStatement.SYNC));
    assertUnsupported(() -> session.execute(graphStatement, GraphStatement.ASYNC));
    assertUnsupported(() -> session.execute(graphStatement, GenericType.STRING));
  }

  @Test
  public void should_reject_unhandled_graph_requests_when_session_is_closed() throws Exception {
    DefaultSession session = session(true);
    assertUnsupported(() -> session.execute(graphStatement, GraphStatement.SYNC));
    assertUnsupported(() -> session.execute(graphStatement, GraphStatement.ASYNC));
  }

  @Test
  public void should_return_custom_graph_results_from_earlier_processors() throws Exception {
    GenericType<CompletionStage<String>> asyncType = new GenericType<CompletionStage<String>>() {};
    processors.add(0, customProcessor(GenericType.STRING, "custom graph result"));
    CompletionStage<String> asyncResult = CompletableFuture.completedFuture("custom async result");
    RequestProcessor<GraphStatement<?>, CompletionStage<String>> asyncProcessor =
        customProcessor(asyncType, asyncResult);
    processors.add(0, asyncProcessor);
    assertThat(registry().processorFor(graphStatement, asyncType)).isSameAs(asyncProcessor);
    DefaultSession session = session(false);
    assertThat(session.execute(graphStatement, GenericType.STRING))
        .isEqualTo("custom graph result");
    assertThat(session.execute(graphStatement, asyncType)).isSameAs(asyncResult);
    assertThat(session.execute(graphStatement, asyncType).toCompletableFuture().join())
        .isEqualTo("custom async result");
    // Unsupported result types still reach the fallback.
    assertUnsupported(() -> session.execute(graphStatement, GraphStatement.SYNC));
  }

  @Test
  public void should_use_custom_failure_when_session_is_closed() throws Exception {
    processors.add(0, customProcessor(GenericType.STRING, "unused"));
    assertThatThrownBy(() -> session(true).execute(graphStatement, GenericType.STRING))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Session is closed");
  }

  @Test
  public void should_preserve_cql_dispatch() {
    SimpleStatement statement = SimpleStatement.newInstance("SELECT 1");
    assertThat(registry().processorFor(statement, Statement.SYNC))
        .isInstanceOf(CqlRequestSyncProcessor.class);
    assertThat(registry().processorFor(statement, Statement.ASYNC))
        .isInstanceOf(CqlRequestAsyncProcessor.class);
  }

  @Test
  public void should_not_reject_graph_in_a_registry_without_the_fallback() {
    RequestProcessor<GraphStatement<?>, String> custom =
        customProcessor(GenericType.STRING, "custom");
    RequestProcessorRegistry registry = new RequestProcessorRegistry("test", custom);
    assertThat(registry.processorFor(graphStatement, GenericType.STRING)).isSameAs(custom);
    assertThatThrownBy(() -> registry.processorFor(graphStatement, GraphStatement.SYNC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No request processor found");
  }

  private RequestProcessorRegistry registry() {
    return new RequestProcessorRegistry("test", processors.toArray(new RequestProcessor[0]));
  }

  private DefaultSession session(boolean closed) throws Exception {
    DefaultSession session = mock(DefaultSession.class, CALLS_REAL_METHODS);
    doReturn(closed).when(session).isClosed();
    Field registryField = DefaultSession.class.getDeclaredField("processorRegistry");
    registryField.setAccessible(true);
    registryField.set(session, registry());
    return session;
  }

  private <T> RequestProcessor<GraphStatement<?>, T> customProcessor(
      GenericType<T> resultType, T result) {
    return new RequestProcessor<GraphStatement<?>, T>() {
      @Override
      public boolean canProcess(Request request, GenericType<?> requestedType) {
        return request instanceof GraphStatement && requestedType.equals(resultType);
      }

      @Override
      public T process(
          GraphStatement<?> request,
          DefaultSession session,
          InternalDriverContext context,
          String sessionLogPrefix) {
        assertThat(request).isSameAs(graphStatement);
        return result;
      }

      @Override
      public T newFailure(RuntimeException error) {
        throw error;
      }
    };
  }

  private static void assertUnsupported(Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
  }
}
