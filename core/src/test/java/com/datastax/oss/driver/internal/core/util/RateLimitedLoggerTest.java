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
package com.datastax.oss.driver.internal.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

public class RateLimitedLoggerTest {
  @Test
  public void should_log_first_warning_and_then_once_per_interval() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    AtomicLong clock = new AtomicLong(1);
    Duration interval = Duration.ofMinutes(1);
    RateLimitedLogger logger = new RateLimitedLogger(delegate, () -> interval, clock::get);

    logger.warn("message {}", "value");
    logger.warn("message {}", "value");
    clock.addAndGet(interval.toNanos() - 1);
    logger.warn("message {}", "value");
    clock.incrementAndGet();
    logger.warn("message {}", "value");

    verify(delegate).warn(eq("message {}"), any(Object[].class));
    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    verify(delegate).warn(eq("message {} (suppressed {} similar messages)"), arguments.capture());
    assertThat(arguments.getValue()).containsExactly("value", 2L);
  }

  @Test
  public void zero_interval_should_log_every_warning() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    RateLimitedLogger logger = new RateLimitedLogger(delegate, () -> Duration.ZERO);

    logger.warn("message");
    logger.warn("message");

    verify(delegate, times(2)).warn(eq("message"), any(Object[].class));
  }

  @Test
  public void missing_interval_should_use_default_limit() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    DriverContext context = mock(DriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    RateLimitedLogger logger = new RateLimitedLogger(delegate, context);

    logger.warn("message");
    logger.warn("message");

    verify(delegate).warn(eq("message"), any(Object[].class));
  }

  @Test
  public void should_share_windows_within_context_and_component() {
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(config.getDefaultProfile()).thenReturn(profile);
    DriverContext context = mock(DriverContext.class);
    DriverContext otherContext = mock(DriverContext.class);
    when(context.getConfig()).thenReturn(config);
    when(otherContext.getConfig()).thenReturn(config);

    Logger first = mock(Logger.class);
    Logger second = mock(Logger.class);
    Logger otherComponent = mock(Logger.class);
    Logger otherDriver = mock(Logger.class);
    when(first.getName()).thenReturn("ChannelPool");
    when(second.getName()).thenReturn("ChannelPool");
    when(otherComponent.getName()).thenReturn("Reconnection");
    when(otherDriver.getName()).thenReturn("ChannelPool");
    when(first.isWarnEnabled()).thenReturn(true);
    when(second.isWarnEnabled()).thenReturn(true);
    when(otherComponent.isWarnEnabled()).thenReturn(true);
    when(otherDriver.isWarnEnabled()).thenReturn(true);

    new RateLimitedLogger(first, context).warn("message");
    new RateLimitedLogger(second, context).warn("message");
    new RateLimitedLogger(otherComponent, context).warn("message");
    new RateLimitedLogger(otherDriver, otherContext).warn("message");

    verify(first).warn(eq("message"), any(Object[].class));
    verify(second, never()).warn(eq("message"), any(Object[].class));
    verify(otherComponent).warn(eq("message"), any(Object[].class));
    verify(otherDriver).warn(eq("message"), any(Object[].class));
  }

  @Test
  public void disabling_limit_should_report_pending_count_then_log_every_warning() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    AtomicReference<Duration> interval = new AtomicReference<>(Duration.ofMinutes(1));
    RateLimitedLogger logger = new RateLimitedLogger(delegate, interval::get, () -> 1);

    logger.warn("message");
    logger.warn("message");
    interval.set(Duration.ZERO);
    logger.warn("message");
    logger.warn("message");

    verify(delegate, times(2)).warn(eq("message"), any(Object[].class));
    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    verify(delegate).warn(eq("message (suppressed {} similar messages)"), arguments.capture());
    assertThat(arguments.getValue()).containsExactly(1L);
  }

  @Test
  public void disabled_warn_level_should_not_consume_interval() {
    Logger delegate = mock(Logger.class);
    AtomicLong clock = new AtomicLong(1);
    RateLimitedLogger logger =
        new RateLimitedLogger(delegate, () -> Duration.ofMinutes(1), clock::get);

    logger.warn("message");
    verify(delegate, never()).warn(eq("message"), any(Object[].class));
    when(delegate.isWarnEnabled()).thenReturn(true);
    logger.warn("message");

    verify(delegate, times(1)).warn(eq("message"), any(Object[].class));
  }

  @Test
  public void different_templates_and_levels_should_have_separate_intervals() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    when(delegate.isErrorEnabled()).thenReturn(true);
    RateLimitedLogger logger =
        new RateLimitedLogger(delegate, () -> Duration.ofMinutes(1), () -> 1);

    logger.warn("first");
    logger.warn("first");
    logger.warn("second");
    logger.error("first");
    logger.error("first");

    verify(delegate, times(1)).warn(eq("first"), any(Object[].class));
    verify(delegate, times(1)).warn(eq("second"), any(Object[].class));
    verify(delegate, times(1)).error(eq("first"), any(Object[].class));
  }

  @Test
  public void should_limit_exception_warnings_without_losing_exception_formatting() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    AtomicLong clock = new AtomicLong(1);
    RateLimitedLogger logger =
        new RateLimitedLogger(delegate, () -> Duration.ofMinutes(1), clock::get);

    logger.warnWithException("Failed on {}", "node", new IllegalStateException("unavailable"));
    logger.warnWithException("Failed on {}", "node", new IllegalStateException("unavailable"));
    clock.addAndGet(Duration.ofMinutes(1).toNanos());
    logger.warnWithException("Failed on {}", "node", new IllegalStateException("unavailable"));

    verify(delegate, times(1)).warn(eq("Failed on {} ({})"), any(Object[].class));
    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    verify(delegate)
        .warn(eq("Failed on {} (suppressed {} similar messages) ({})"), arguments.capture());
    assertThat(arguments.getValue())
        .containsExactly("node", 1L, "IllegalStateException: unavailable");
  }

  @Test
  public void broken_interval_configuration_should_not_hide_an_error() {
    Logger delegate = mock(Logger.class);
    when(delegate.isErrorEnabled()).thenReturn(true);
    RateLimitedLogger logger =
        new RateLimitedLogger(
            delegate,
            () -> {
              throw new IllegalArgumentException("invalid interval");
            });

    logger.error("Configuration reload failed");

    verify(delegate).error(eq("Configuration reload failed"), any(Object[].class));
  }

  @Test
  public void error_summary_should_preserve_cause_as_last_argument() {
    Logger delegate = mock(Logger.class);
    when(delegate.isErrorEnabled()).thenReturn(true);
    AtomicLong clock = new AtomicLong(1);
    RateLimitedLogger logger =
        new RateLimitedLogger(delegate, () -> Duration.ofMinutes(1), clock::get);
    IllegalStateException cause = new IllegalStateException("unavailable");

    logger.error("Failure on {}", "node", cause);
    logger.error("Failure on {}", "node", cause);
    clock.addAndGet(Duration.ofMinutes(1).toNanos());
    logger.error("Failure on {}", "node", cause);

    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    verify(delegate)
        .error(eq("Failure on {} (suppressed {} similar messages)"), arguments.capture());
    assertThat(arguments.getValue()).containsExactly("node", 1L, cause);
  }

  @Test
  public void should_count_concurrent_suppressed_warnings() throws Exception {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    AtomicLong clock = new AtomicLong(1);
    RateLimitedLogger logger =
        new RateLimitedLogger(delegate, () -> Duration.ofMinutes(1), clock::get);
    logger.warn("message");

    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> tasks = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        tasks.add(
            executor.submit(
                () -> {
                  for (int j = 0; j < 250; j++) {
                    logger.warn("message");
                  }
                }));
      }
      for (Future<?> task : tasks) {
        task.get();
      }
    } finally {
      executor.shutdownNow();
    }

    clock.addAndGet(Duration.ofMinutes(1).toNanos());
    logger.warn("message");

    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    verify(delegate).warn(eq("message (suppressed {} similar messages)"), arguments.capture());
    assertThat(arguments.getValue()).containsExactly(1000L);
  }

  @Test
  public void exception_summary_should_preserve_debug_stack_trace() {
    Logger delegate = mock(Logger.class);
    when(delegate.isWarnEnabled()).thenReturn(true);
    when(delegate.isDebugEnabled()).thenReturn(true);
    AtomicLong clock = new AtomicLong(1);
    RateLimitedLogger logger =
        new RateLimitedLogger(delegate, () -> Duration.ofMinutes(1), clock::get);
    IllegalStateException cause = new IllegalStateException("unavailable");

    logger.warnWithException("Failure on {}", "node", cause);
    logger.warnWithException("Failure on {}", "node", cause);
    clock.addAndGet(Duration.ofMinutes(1).toNanos());
    logger.warnWithException("Failure on {}", "node", cause);

    ArgumentCaptor<Object[]> arguments = ArgumentCaptor.forClass(Object[].class);
    verify(delegate)
        .warn(eq("Failure on {} (suppressed {} similar messages)"), arguments.capture());
    assertThat(arguments.getValue()).containsExactly("node", 1L, cause);
  }
}
