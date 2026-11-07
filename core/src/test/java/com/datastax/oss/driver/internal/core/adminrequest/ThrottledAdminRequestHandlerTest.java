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

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ThrottledAdminRequestHandlerTest {

  @Mock private DriverChannel channel;
  @Mock private RequestThrottler throttler;
  @Mock private SessionMetricUpdater metricUpdater;
  private final AtomicInteger availableIds = new AtomicInteger(1);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(channel.preAcquireId()).thenAnswer(invocation -> availableIds.compareAndSet(1, 0));
    when(channel.closeFuture()).thenReturn(mock(ChannelFuture.class));
    when(channel.eventLoop()).thenReturn(mock(EventLoop.class));
    doAnswer(
            invocation -> {
              if (!availableIds.compareAndSet(0, 1)) {
                throw new AssertionError("No caller-owned reservation to cancel");
              }
              return null;
            })
        .when(channel)
        .cancelPreAcquireId();
  }

  @Test
  public void should_release_permit_and_reservation_when_metric_update_throws() {
    RuntimeException failure = new RuntimeException("mock failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler();
    handler.start();
    doThrow(failure)
        .when(metricUpdater)
        .updateTimer(
            eq(DefaultSessionMetric.THROTTLING_DELAY),
            isNull(),
            anyLong(),
            eq(TimeUnit.NANOSECONDS));

    handler.onThrottleReady(true);

    assertThat(availableIds.get()).isEqualTo(1);
    verify(throttler).signalError(handler, failure);
    verify(channel, never()).write(any(), anyBoolean(), anyMap(), any());
    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));
  }

  @Test
  public void should_rethrow_fatal_admission_failure_after_cleanup() {
    AssertionError failure = new AssertionError("fatal metric failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler();
    handler.start();
    doThrow(failure)
        .when(metricUpdater)
        .updateTimer(
            eq(DefaultSessionMetric.THROTTLING_DELAY),
            isNull(),
            anyLong(),
            eq(TimeUnit.NANOSECONDS));

    assertThatThrownBy(() -> handler.onThrottleReady(true)).isSameAs(failure);

    assertThat(availableIds.get()).isEqualTo(1);
    verify(throttler).signalError(handler, failure);
    verify(channel, never()).write(any(), anyBoolean(), anyMap(), any());
    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));
  }

  @Test
  public void should_release_permit_when_synchronous_write_throws() {
    RuntimeException failure = new RuntimeException("mock failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler();
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              return null;
            })
        .when(throttler)
        .register(handler);
    doThrow(failure).when(channel).write(any(), anyBoolean(), anyMap(), eq(handler));

    handler.start();

    assertThat(availableIds.get()).isEqualTo(1);
    verify(throttler).signalError(handler, failure);
    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));
  }

  @Test
  public void should_complete_result_without_releasing_permit_when_registration_throws() {
    RuntimeException failure = new RuntimeException("mock failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler();
    doThrow(failure).when(throttler).register(handler);

    assertThatThrownBy(handler::start).isSameAs(failure);

    assertThat(availableIds.get()).isEqualTo(1);
    verify(throttler, never()).signalError(handler, failure);
    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));
  }

  @Test
  public void should_cancel_submitted_write_when_registration_throws_after_admission() {
    RuntimeException failure = new RuntimeException("mock failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler(Duration.ofSeconds(1));
    Promise<Void> writeFuture = ImmediateEventExecutor.INSTANCE.newPromise();
    when(channel.write(any(), anyBoolean(), anyMap(), eq(handler))).thenReturn(writeFuture);
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              throw failure;
            })
        .when(throttler)
        .register(handler);

    assertThatThrownBy(handler::start).isSameAs(failure);

    assertThat(availableIds.get()).isZero();
    verify(channel, never()).cancelPreAcquireId();
    verify(throttler).signalError(handler, failure);
    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));

    writeFuture.setSuccess(null);

    verify(channel, atLeastOnce()).cancel(handler);
    verify(channel.eventLoop(), never()).schedule(any(Runnable.class), anyLong(), any());
  }

  @Test
  public void should_cancel_submitted_write_when_registration_cleanup_throws() {
    verifySubmittedRequestCleanup(true, Duration.ZERO);
  }

  @Test
  public void should_cancel_submitted_write_and_timeout_when_registration_cleanup_throws() {
    verifySubmittedRequestCleanup(true, Duration.ofSeconds(1));
  }

  @Test
  public void should_cancel_submitted_write_when_admission_cleanup_throws() {
    verifySubmittedRequestCleanup(false, Duration.ZERO);
  }

  @Test
  public void should_cancel_submitted_write_and_timeout_when_admission_cleanup_throws() {
    verifySubmittedRequestCleanup(false, Duration.ofSeconds(1));
  }

  private void verifySubmittedRequestCleanup(boolean registrationFailure, Duration timeout) {
    verifySubmittedRequestCleanup(registrationFailure, timeout, false);
  }

  @Test
  public void should_preserve_fatal_registration_failure_when_permit_release_also_fails() {
    verifySubmittedRequestCleanup(true, Duration.ofSeconds(1), true);
  }

  @Test
  public void should_preserve_fatal_admission_failure_when_permit_release_also_fails() {
    verifySubmittedRequestCleanup(false, Duration.ofSeconds(1), true);
  }

  private void verifySubmittedRequestCleanup(
      boolean registrationFailure, Duration timeout, boolean fatalOriginal) {
    Throwable failure =
        fatalOriginal
            ? new AssertionError("fatal setup failure")
            : new RuntimeException("mock setup failure");
    AssertionError fatal = new AssertionError("fatal next callback failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler(timeout);
    ScheduledFuture<?> timeoutFuture = mock(ScheduledFuture.class);
    EventLoop eventLoop = channel.eventLoop();
    doReturn(timeoutFuture)
        .when(eventLoop)
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    Future<Void> successfulWrite = ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
    if (registrationFailure) {
      when(channel.write(any(), anyBoolean(), anyMap(), eq(handler))).thenReturn(successfulWrite);
    } else {
      Future<Void> writeFuture = mock(Future.class);
      doAnswer(
              invocation -> {
                GenericFutureListener<Future<Void>> listener = invocation.getArgument(0);
                listener.operationComplete(successfulWrite);
                throw failure;
              })
          .when(writeFuture)
          .addListener(any());
      when(channel.write(any(), anyBoolean(), anyMap(), eq(handler))).thenReturn(writeFuture);
    }
    doAnswer(
            invocation -> {
              handler.onThrottleReady(false);
              if (registrationFailure) {
                throw failure;
              }
              return null;
            })
        .when(throttler)
        .register(handler);
    doThrow(fatal).when(throttler).signalError(handler, failure);

    assertThatThrownBy(handler::start).isSameAs(fatalOriginal ? failure : fatal);

    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));
    if (fatalOriginal) {
      assertThat(failure.getSuppressed()).containsExactly(fatal);
    }
    verify(channel, atLeastOnce()).cancel(handler);
    verify(channel, never()).cancelPreAcquireId();
    verify(throttler).signalError(handler, failure);
    if (timeout.isZero()) {
      verify(channel.eventLoop(), never()).schedule(any(Runnable.class), anyLong(), any());
    } else {
      verify(timeoutFuture, atLeastOnce()).cancel(true);
    }
  }

  @Test
  public void should_release_permit_for_throttling_exception_after_admission() {
    RequestThrottlingException failure = new RequestThrottlingException("mock failure");
    ThrottledAdminRequestHandler<AdminResult> handler = newHandler();
    Promise<Void> writeFuture = ImmediateEventExecutor.INSTANCE.newPromise();
    when(channel.write(any(), anyBoolean(), anyMap(), eq(handler))).thenReturn(writeFuture);
    doAnswer(
            invocation -> {
              invocation.getArgument(0, Throttled.class).onThrottleReady(false);
              throw failure;
            })
        .when(throttler)
        .register(handler);

    assertThatThrownBy(handler::start).isSameAs(failure);

    verify(throttler).signalError(handler, failure);
    assertThatStage(handler.result).isFailed(error -> assertThat(error).isSameAs(failure));
  }

  private ThrottledAdminRequestHandler<AdminResult> newHandler() {
    return newHandler(Duration.ZERO);
  }

  private ThrottledAdminRequestHandler<AdminResult> newHandler(Duration timeout) {
    assertThat(channel.preAcquireId()).isTrue();
    assertThat(availableIds.get()).isZero();
    return ThrottledAdminRequestHandler.query(
        channel,
        false,
        new Query("mock query"),
        Frame.NO_PAYLOAD,
        timeout,
        throttler,
        metricUpdater,
        "test",
        "mock query");
  }
}
