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
package com.datastax.dse.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveResultSet;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphResultSet;
import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@SuppressWarnings("deprecation")
public class UnsupportedDseExecutionTest {

  private final CqlSession session = mock(CqlSession.class, CALLS_REAL_METHODS);
  private final SimpleStatement statement = SimpleStatement.newInstance("SELECT 1");
  private final GraphStatement<?> graphStatement = mock(GraphStatement.class);

  @Test
  public void should_deliver_continuous_failure_to_stage_handler() {
    CompletionStage<ContinuousAsyncResultSet> result = session.executeContinuouslyAsync(statement);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    assertThat(
            result
                .exceptionally(
                    error -> {
                      failure.set(error);
                      return null;
                    })
                .toCompletableFuture()
                .join())
        .isNull();
    assertThat(failure.get())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("use ordinary CQL paging");
  }

  @Test
  public void should_deliver_graph_failure_to_stage_handler() {
    CompletionStage<AsyncGraphResultSet> result = session.executeAsync(graphStatement);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    assertThat(
            result
                .exceptionally(
                    error -> {
                      failure.set(error);
                      return null;
                    })
                .toCompletableFuture()
                .join())
        .isNull();
    assertThat(failure.get())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
  }

  @Test
  public void should_deliver_continuous_failure_to_row_and_metadata_subscribers() {
    assertContinuousFailure(session.executeContinuouslyReactive(statement));
    assertContinuousFailure(session.executeContinuouslyReactive("SELECT 1"));
  }

  private void assertContinuousFailure(ContinuousReactiveResultSet result) {
    // Metadata can be subscribed before rows, and errors do not require demand.
    Throwable failure = assertPublisherFailure(result.getColumnDefinitions());
    assertThat(failure).hasMessageContaining("use ordinary reactive CQL execution");
    assertThat(assertPublisherFailure(result.getExecutionInfos())).isSameAs(failure);
    assertThat(assertPublisherFailure(result.wasApplied())).isSameAs(failure);
    assertThat(assertPublisherFailure(result)).isSameAs(failure);
  }

  @Test
  public void should_deliver_graph_failure_to_node_and_metadata_subscribers() {
    ReactiveGraphResultSet result = session.executeReactive(graphStatement);
    Throwable failure = assertPublisherFailure(result.getExecutionInfos());
    assertThat(failure).hasMessage(GraphSupportRemoved.MESSAGE);
    assertThat(assertPublisherFailure(result)).isSameAs(failure);
  }

  @Test
  public void should_keep_synchronous_execution_failures() {
    assertThatThrownBy(() -> session.executeContinuously(statement))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> session.execute(graphStatement))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
  }

  private <T> Throwable assertPublisherFailure(Publisher<T> publisher) {
    @SuppressWarnings("unchecked")
    Subscriber<T> subscriber = mock(Subscriber.class);
    publisher.subscribe(subscriber);
    InOrder signals = inOrder(subscriber);
    signals.verify(subscriber).onSubscribe(any(Subscription.class));
    ArgumentCaptor<Throwable> failure = ArgumentCaptor.forClass(Throwable.class);
    signals.verify(subscriber).onError(failure.capture());
    signals.verifyNoMoreInteractions();
    assertThat(failure.getValue()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> publisher.subscribe(null)).isInstanceOf(NullPointerException.class);
    return failure.getValue();
  }
}
