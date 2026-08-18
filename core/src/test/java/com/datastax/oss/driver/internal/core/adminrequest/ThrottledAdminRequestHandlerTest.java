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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
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

    assertThatThrownBy(handler::start).isSameAs(failure);

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

  private ThrottledAdminRequestHandler<AdminResult> newHandler() {
    assertThat(channel.preAcquireId()).isTrue();
    assertThat(availableIds.get()).isZero();
    return ThrottledAdminRequestHandler.query(
        channel,
        false,
        new Query("mock query"),
        Frame.NO_PAYLOAD,
        Duration.ZERO,
        throttler,
        metricUpdater,
        "test",
        "mock query");
  }
}
