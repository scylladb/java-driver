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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.api.core.tracker.RequestIdGenerator;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.util.concurrent.CapturingTimer.CapturedTimeout;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.util.Timer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class CqlRequestHandlerTest extends CqlRequestHandlerTestBase {

  @Test
  public void should_complete_result_if_first_node_replies_immediately() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    node1Behavior.setWriteSuccess();
    node1Behavior.setResponseSuccess(defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet -> {
                Iterator<Row> rows = resultSet.currentPage().iterator();
                assertThat(rows.hasNext()).isTrue();
                assertThat(rows.next().getString("message")).isEqualTo("hello, world");

                ExecutionInfo executionInfo = resultSet.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
                assertThat(executionInfo.getErrors()).isEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getPagingState()).isNull();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();
              });
      node1Behavior.verifyPreAcquireNotCancelled();
    }
  }

  @Test
  public void should_try_next_node_if_channel_selection_fails() {
    RuntimeException failure = new RuntimeException("mock failure");
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withEmptyPool(node1)
            .withResponse(node2, defaultFrameOf(singleRow()))
            .build()) {
      when(harness.getSession().getChannel(eq(node1), anyString(), any(), any()))
          .thenThrow(failure);

      CompletionStage<AsyncResultSet> result =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(result)
          .isSuccess(
              resultSet -> {
                assertThat(resultSet.getExecutionInfo().getCoordinator()).isEqualTo(node2);
                List<Map.Entry<Node, Throwable>> errors = resultSet.getExecutionInfo().getErrors();
                assertThat(errors).hasSize(1);
                assertThat(errors.get(0).getKey()).isEqualTo(node1);
                assertThat(errors.get(0).getValue()).isSameAs(failure);
              });
    }
  }

  @Test
  public void should_fail_if_no_node_available() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            // Mock no responses => this will produce an empty query plan
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isFailed(error -> assertThat(error).isInstanceOf(NoNodeAvailableException.class));
    }
  }

  @Test
  public void should_fail_if_nodes_unavailable() {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    try (RequestHandlerTestHarness harness =
        harnessBuilder.withEmptyPool(node1).withEmptyPool(node2).build()) {
      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();
      assertThatStage(resultSetFuture)
          .isFailed(
              error -> {
                assertThat(error).isInstanceOf(AllNodesFailedException.class);
                Map<Node, List<Throwable>> allErrors =
                    ((AllNodesFailedException) error).getAllErrors();
                assertThat(allErrors).hasSize(2);
                assertThat(allErrors)
                    .hasEntrySatisfying(
                        node1,
                        nodeErrors ->
                            assertThat(nodeErrors)
                                .singleElement()
                                .isInstanceOf(NodeUnavailableException.class));
                assertThat(allErrors)
                    .hasEntrySatisfying(
                        node2,
                        nodeErrors ->
                            assertThat(nodeErrors)
                                .singleElement()
                                .isInstanceOf(NodeUnavailableException.class));
              });
    }
  }

  @Test
  public void should_complete_result_and_cleanup_if_immediate_request_setup_fails() {
    RequestIdGenerator requestIdGenerator = mock(RequestIdGenerator.class);
    RequestThrottler throttler = mock(RequestThrottler.class);
    RuntimeException failure = new RuntimeException("mock failure");
    when(requestIdGenerator.getSessionRequestId()).thenReturn("session");
    when(requestIdGenerator.getNodeRequestId(any(), eq("session"))).thenReturn("node");
    when(requestIdGenerator.getDecoratedStatement(any(), eq("node"))).thenThrow(failure);
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              return null;
            })
        .when(throttler)
        .register(any());

    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withRequestIdGenerator(requestIdGenerator);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
      node1Behavior.verifyNoWrite();
      node1Behavior.verifyPreAcquireCancelled();
      assertThat(harness.nextScheduledTimeout().isCancelled()).isTrue();
      verify(throttler).signalError(handler, failure);
    }
  }

  @Test
  public void should_not_release_throttler_if_request_was_not_admitted() {
    RequestThrottler throttler = mock(RequestThrottler.class);
    RequestThrottlingException failure = new RequestThrottlingException("mock failure");
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleFailure(failure);
              return null;
            })
        .when(throttler)
        .register(any());

    try (RequestHandlerTestHarness harness = RequestHandlerTestHarness.builder().build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
      assertThat(harness.nextScheduledTimeout().isCancelled()).isTrue();
      verify(throttler, never()).signalSuccess(any());
      verify(throttler, never()).signalError(any(), any());
      verify(throttler, never()).signalTimeout(any());
      verify(throttler, never()).signalCancel(any());
    }
  }

  @Test
  public void should_schedule_timeout_before_throttler_registration() {
    RequestThrottler throttler = mock(RequestThrottler.class);
    RequestThrottlingException failure = new RequestThrottlingException("mock failure");
    AtomicReference<CapturedTimeout> timeoutSeenDuringRegistration = new AtomicReference<>();

    try (RequestHandlerTestHarness harness = RequestHandlerTestHarness.builder().build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      doAnswer(
              invocation -> {
                timeoutSeenDuringRegistration.set(harness.nextScheduledTimeout());
                invocation.getArgument(0, Throttled.class).onThrottleFailure(failure);
                return null;
              })
          .when(throttler)
          .register(any());

      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      assertThat(timeoutSeenDuringRegistration.get()).isNotNull();
      assertThat(timeoutSeenDuringRegistration.get().isCancelled()).isTrue();
      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
    }
  }

  @Test
  public void should_defer_queued_timeout_release_until_registration_returns() {
    RequestThrottler throttler = mock(RequestThrottler.class);
    AtomicBoolean timeoutSignaled = new AtomicBoolean();
    doAnswer(
            invocation -> {
              timeoutSignaled.set(true);
              return null;
            })
        .when(throttler)
        .signalTimeout(any());

    try (RequestHandlerTestHarness harness = RequestHandlerTestHarness.builder().build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      doAnswer(
              invocation -> {
                CapturedTimeout timeout = harness.nextScheduledTimeout();
                timeout.task().run(timeout);
                assertThat(timeoutSignaled).isFalse();
                return null;
              })
          .when(throttler)
          .register(any());

      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      assertThatStage(handler.handle())
          .isFailed(error -> assertThat(error).isInstanceOf(DriverTimeoutException.class));
      assertThat(timeoutSignaled).isTrue();
      verify(throttler).signalTimeout(handler);
    }
  }

  @Test
  public void should_release_permit_when_registration_throws_after_admission() {
    RequestThrottler throttler = mock(RequestThrottler.class);
    RuntimeException failure = new RuntimeException("mock failure");
    AtomicReference<CqlRequestHandler> admittedHandler = new AtomicReference<>();
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      doAnswer(
              invocation -> {
                CqlRequestHandler handler = invocation.getArgument(0);
                admittedHandler.set(handler);
                handler.onThrottleReady(false);
                node1Behavior.setWriteSuccess();
                throw failure;
              })
          .when(throttler)
          .register(any());

      assertThatThrownBy(
              () ->
                  new CqlRequestHandler(
                      UNDEFINED_IDEMPOTENCE_STATEMENT,
                      harness.getSession(),
                      harness.getContext(),
                      "test"))
          .isSameAs(failure);

      CqlRequestHandler handler = admittedHandler.get();
      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
      assertThat(harness.nextScheduledTimeout().isCancelled()).isTrue();
      node1Behavior.verifyWrite();
      node1Behavior.verifyCancellation();
      verify(throttler).signalError(handler, failure);
    }
  }

  @Test
  public void
      should_cancel_timeout_without_releasing_permit_when_registration_fails_before_admission() {
    RequestThrottler throttler = mock(RequestThrottler.class);
    RuntimeException failure = new RuntimeException("mock failure");
    doAnswer(
            invocation -> {
              throw failure;
            })
        .when(throttler)
        .register(any());

    try (RequestHandlerTestHarness harness = RequestHandlerTestHarness.builder().build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);

      assertThatThrownBy(
              () ->
                  new CqlRequestHandler(
                      UNDEFINED_IDEMPOTENCE_STATEMENT,
                      harness.getSession(),
                      harness.getContext(),
                      "test"))
          .isSameAs(failure);

      assertThat(harness.nextScheduledTimeout().isCancelled()).isTrue();
      verify(throttler, never()).signalSuccess(any());
      verify(throttler, never()).signalError(any(), any());
      verify(throttler, never()).signalTimeout(any());
      verify(throttler, never()).signalCancel(any());
    }
  }

  @Test
  public void should_complete_result_and_cleanup_if_delayed_request_setup_fails() {
    RequestIdGenerator requestIdGenerator = mock(RequestIdGenerator.class);
    RequestThrottler throttler = mock(RequestThrottler.class);
    AtomicReference<Throttled> registeredRequest = new AtomicReference<>();
    RuntimeException failure = new RuntimeException("mock failure");
    when(requestIdGenerator.getSessionRequestId()).thenReturn("session");
    when(requestIdGenerator.getNodeRequestId(any(), eq("session"))).thenReturn("node");
    when(requestIdGenerator.getDecoratedStatement(any(), eq("node"))).thenThrow(failure);
    doAnswer(
            invocation -> {
              registeredRequest.set(invocation.getArgument(0));
              return null;
            })
        .when(throttler)
        .register(any());

    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withRequestIdGenerator(requestIdGenerator);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      registeredRequest.get().onThrottleReady(true);

      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
      node1Behavior.verifyNoWrite();
      node1Behavior.verifyPreAcquireCancelled();
      assertThat(harness.nextScheduledTimeout().isCancelled()).isTrue();
      verify(throttler).signalError(handler, failure);
    }
  }

  @Test
  public void should_not_register_request_if_timeout_scheduling_fails() {
    RequestThrottler throttler = mock(RequestThrottler.class);
    Timer timer = mock(Timer.class);
    IllegalStateException failure = new IllegalStateException("mock failure");
    when(timer.newTimeout(any(), anyLong(), any())).thenThrow(failure);

    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    harnessBuilder.customBehavior(node1);
    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      when(harness.getContext().getNettyOptions().getTimer()).thenReturn(timer);

      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
      verify(throttler, never()).register(any());
      verify(throttler, never()).signalSuccess(any());
      verify(throttler, never()).signalError(any(), any());
      verify(throttler, never()).signalTimeout(any());
      verify(throttler, never()).signalCancel(any());
    }
  }

  @Test
  public void should_cleanup_if_request_setup_fails_after_write_retry() {
    RequestIdGenerator requestIdGenerator = mock(RequestIdGenerator.class);
    RequestThrottler throttler = mock(RequestThrottler.class);
    RuntimeException writeFailure = new RuntimeException("mock write failure");
    RuntimeException setupFailure = new RuntimeException("mock setup failure");
    when(requestIdGenerator.getSessionRequestId()).thenReturn("session");
    when(requestIdGenerator.getNodeRequestId(any(), eq("session"))).thenReturn("node1", "node2");
    doReturn(UNDEFINED_IDEMPOTENCE_STATEMENT)
        .doThrow(setupFailure)
        .when(requestIdGenerator)
        .getDecoratedStatement(any(), any());
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              return null;
            })
        .when(throttler)
        .register(any());

    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withRequestIdGenerator(requestIdGenerator);
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    PoolBehavior node2Behavior = harnessBuilder.customBehavior(node2);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");
      CompletionStage<AsyncResultSet> result = handler.handle();

      node1Behavior.setWriteFailure(writeFailure);

      assertThatStage(result).isFailed(error -> assertThat(error).isSameAs(setupFailure));
      node2Behavior.verifyNoWrite();
      node2Behavior.verifyPreAcquireCancelled();
      assertThat(harness.nextScheduledTimeout().isCancelled()).isTrue();
      verify(throttler).signalError(handler, setupFailure);
    }
  }

  @Test
  public void should_release_cancelled_request_only_once() {
    RequestIdGenerator requestIdGenerator = mock(RequestIdGenerator.class);
    RequestThrottler throttler = mock(RequestThrottler.class);
    CancellationException failure = new CancellationException("mock cancellation");
    when(requestIdGenerator.getSessionRequestId()).thenReturn("session");
    when(requestIdGenerator.getNodeRequestId(any(), eq("session"))).thenReturn("node");
    when(requestIdGenerator.getDecoratedStatement(any(), eq("node"))).thenThrow(failure);
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              return null;
            })
        .when(throttler)
        .register(any());

    RequestHandlerTestHarness.Builder harnessBuilder =
        RequestHandlerTestHarness.builder().withRequestIdGenerator(requestIdGenerator);
    harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      CqlRequestHandler handler =
          new CqlRequestHandler(
              UNDEFINED_IDEMPOTENCE_STATEMENT, harness.getSession(), harness.getContext(), "test");

      assertThat(handler.handle().toCompletableFuture()).isCancelled();
      verify(throttler).signalCancel(handler);
      verify(throttler, never()).signalError(eq(handler), any());
    }
  }

  @Test
  public void should_time_out_if_first_node_takes_too_long_to_respond() throws Exception {
    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    node1Behavior.setWriteSuccess();

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      // First scheduled task is the timeout, run it before node1 has responded
      CapturedTimeout requestTimeout = harness.nextScheduledTimeout();
      Duration configuredTimeoutDuration =
          harness
              .getContext()
              .getConfig()
              .getDefaultProfile()
              .getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
      assertThat(requestTimeout.getDelay(TimeUnit.NANOSECONDS))
          .isEqualTo(configuredTimeoutDuration.toNanos());
      requestTimeout.task().run(requestTimeout);

      assertThatStage(resultSetFuture)
          .isFailed(t -> assertThat(t).isInstanceOf(DriverTimeoutException.class));
    }
  }

  @Test
  public void should_switch_keyspace_on_session_after_successful_use_statement() {
    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withResponse(node1, defaultFrameOf(new SetKeyspace("newKeyspace")))
            .build()) {

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(
                  UNDEFINED_IDEMPOTENCE_STATEMENT,
                  harness.getSession(),
                  harness.getContext(),
                  "test")
              .handle();

      assertThatStage(resultSetFuture)
          .isSuccess(
              resultSet ->
                  verify(harness.getSession())
                      .setKeyspace(CqlIdentifier.fromInternal("newKeyspace")));
    }
  }

  @Test
  public void should_reprepare_on_the_fly_if_not_prepared() throws InterruptedException {
    ByteBuffer mockId = Bytes.fromHexString("0xffff");

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.getId()).thenReturn(mockId);
    ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
    when(columnDefinitions.size()).thenReturn(0);
    when(preparedStatement.getResultSetDefinitions()).thenReturn(columnDefinitions);
    BoundStatement boundStatement = mock(BoundStatement.class);
    when(boundStatement.getPreparedStatement()).thenReturn(preparedStatement);
    when(boundStatement.getValues()).thenReturn(Collections.emptyList());
    when(boundStatement.getNowInSeconds()).thenReturn(Statement.NO_NOW_IN_SECONDS);

    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    // For the first attempt that gets the UNPREPARED response
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);
    // For the second attempt that succeeds
    harnessBuilder.withResponse(node1, defaultFrameOf(singleRow()));

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {

      // The handler will look for the info to reprepare in the session's cache, put it there
      ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads = new ConcurrentHashMap<>();
      repreparePayloads.put(
          mockId, new RepreparePayload(mockId, "mock query", null, Collections.emptyMap()));
      when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);

      CompletionStage<AsyncResultSet> resultSetFuture =
          new CqlRequestHandler(boundStatement, harness.getSession(), harness.getContext(), "test")
              .handle();

      // Before we proceed, mock the PREPARE exchange that will occur as soon as we complete the
      // first response.
      node1Behavior.mockFollowupRequest(
          Prepare.class, defaultFrameOf(new Prepared(Bytes.getArray(mockId), null, null, null)));

      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(
          defaultFrameOf(new Unprepared("mock message", Bytes.getArray(mockId))));

      // Should now re-prepare, re-execute and succeed.
      assertThatStage(resultSetFuture).isSuccess();
    }
  }

  @Test
  public void should_release_outer_request_if_reprepare_is_throttled() {
    ByteBuffer mockId = Bytes.fromHexString("0xffff");
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(preparedStatement.getId()).thenReturn(mockId);
    ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
    when(columnDefinitions.size()).thenReturn(0);
    when(preparedStatement.getResultSetDefinitions()).thenReturn(columnDefinitions);
    BoundStatement boundStatement = mock(BoundStatement.class);
    when(boundStatement.getPreparedStatement()).thenReturn(preparedStatement);
    when(boundStatement.getValues()).thenReturn(Collections.emptyList());
    when(boundStatement.getNowInSeconds()).thenReturn(Statement.NO_NOW_IN_SECONDS);

    RequestThrottler throttler = mock(RequestThrottler.class);
    RequestThrottlingException failure = new RequestThrottlingException("mock failure");
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              return null;
            })
        .doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleFailure(failure);
              return null;
            })
        .when(throttler)
        .register(any());

    RequestHandlerTestHarness.Builder harnessBuilder = RequestHandlerTestHarness.builder();
    PoolBehavior node1Behavior = harnessBuilder.customBehavior(node1);

    try (RequestHandlerTestHarness harness = harnessBuilder.build()) {
      when(harness.getContext().getRequestThrottler()).thenReturn(throttler);
      ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads = new ConcurrentHashMap<>();
      repreparePayloads.put(
          mockId, new RepreparePayload(mockId, "mock query", null, Collections.emptyMap()));
      when(harness.getSession().getRepreparePayloads()).thenReturn(repreparePayloads);

      CqlRequestHandler handler =
          new CqlRequestHandler(boundStatement, harness.getSession(), harness.getContext(), "test");
      node1Behavior.setWriteSuccess();
      node1Behavior.setResponseSuccess(
          defaultFrameOf(new Unprepared("mock message", Bytes.getArray(mockId))));

      assertThatStage(handler.handle()).isFailed(error -> assertThat(error).isSameAs(failure));
      verify(throttler).signalError(handler, failure);
      verify(throttler).signalError(any(), eq(failure));
    }
  }
}
