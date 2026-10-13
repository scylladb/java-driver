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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;

/** Limits repeated WARN and ERROR messages by level and message template. */
@ThreadSafe
public final class RateLimitedLogger {
  private static final String SUPPRESSED_SUFFIX = " (suppressed {} similar messages)";
  // The values do not reference their context, so closed driver contexts can be collected.
  private static final Map<DriverContext, Map<String, ConcurrentMap<String, Window>>>
      WINDOWS_BY_CONTEXT = new WeakHashMap<>();

  @NonNull private final Logger delegate;
  @NonNull private final Supplier<Duration> interval;
  @NonNull private final ConcurrentMap<String, Window> windows;
  @NonNull private final LongSupplier clock;

  public RateLimitedLogger(@NonNull Logger delegate, @NonNull DriverContext context) {
    this(
        delegate,
        () -> configuredInterval(context),
        System::nanoTime,
        sharedWindows(context, delegate));
  }

  @NonNull
  private static ConcurrentMap<String, Window> sharedWindows(
      @NonNull DriverContext context, @NonNull Logger delegate) {
    synchronized (WINDOWS_BY_CONTEXT) {
      Map<String, ConcurrentMap<String, Window>> byComponent = WINDOWS_BY_CONTEXT.get(context);
      if (byComponent == null) {
        byComponent = new HashMap<>();
        WINDOWS_BY_CONTEXT.put(context, byComponent);
      }
      return byComponent.computeIfAbsent(delegate.getName(), ignored -> new ConcurrentHashMap<>());
    }
  }

  @NonNull
  private static Duration configuredInterval(@NonNull DriverContext context) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    return profile.isDefined(DefaultDriverOption.REPEATED_LOG_INTERVAL)
        ? profile.getDuration(DefaultDriverOption.REPEATED_LOG_INTERVAL)
        : Duration.ofMinutes(1);
  }

  public RateLimitedLogger(@NonNull Logger delegate, @NonNull Supplier<Duration> interval) {
    this(delegate, interval, System::nanoTime);
  }

  RateLimitedLogger(
      @NonNull Logger delegate, @NonNull Supplier<Duration> interval, @NonNull LongSupplier clock) {
    this(delegate, interval, clock, new ConcurrentHashMap<>());
  }

  private RateLimitedLogger(
      @NonNull Logger delegate,
      @NonNull Supplier<Duration> interval,
      @NonNull LongSupplier clock,
      @NonNull ConcurrentMap<String, Window> windows) {
    this.delegate = delegate;
    this.interval = interval;
    this.clock = clock;
    this.windows = windows;
  }

  public void warn(@NonNull String format, @NonNull Object... arguments) {
    if (delegate.isWarnEnabled()) {
      long suppressed = record("warn:" + format);
      if (suppressed >= 0) {
        delegate.warn(
            suppressed == 0 ? format : format + SUPPRESSED_SUFFIX,
            suppressed == 0 ? arguments : withSuppressedCount(arguments, suppressed));
      }
    }
  }

  public void warnWithException(@NonNull String format, @NonNull Object... arguments) {
    if (delegate.isWarnEnabled()) {
      long suppressed = record("warn:" + format);
      if (suppressed >= 0) {
        Loggers.warnWithException(
            delegate,
            suppressed == 0 ? format : format + SUPPRESSED_SUFFIX,
            suppressed == 0 ? arguments : withSuppressedCount(arguments, suppressed));
      }
    }
  }

  public void error(@NonNull String format, @NonNull Object... arguments) {
    if (delegate.isErrorEnabled()) {
      long suppressed = record("error:" + format);
      if (suppressed >= 0) {
        delegate.error(
            suppressed == 0 ? format : format + SUPPRESSED_SUFFIX,
            suppressed == 0 ? arguments : withSuppressedCount(arguments, suppressed));
      }
    }
  }

  private long record(@NonNull String key) {
    Duration interval;
    try {
      interval = this.interval.get();
    } catch (RuntimeException e) {
      // A broken configuration must not prevent reporting the original problem.
      interval = Duration.ZERO;
    }
    long nanos = 0;
    if (interval != null && !interval.isZero() && !interval.isNegative()) {
      try {
        nanos = interval.toNanos();
      } catch (ArithmeticException e) {
        nanos = Long.MAX_VALUE;
      }
    }
    long now = clock.getAsLong();
    return windows.computeIfAbsent(key, ignored -> new Window()).record(now, nanos);
  }

  @NonNull
  private static Object[] withSuppressedCount(@NonNull Object[] arguments, long suppressed) {
    int size = arguments.length;
    Object[] result = Arrays.copyOf(arguments, size + 1);
    if (size > 0 && arguments[size - 1] instanceof Throwable) {
      result[size] = arguments[size - 1];
      result[size - 1] = suppressed;
    } else {
      result[size] = suppressed;
    }
    return result;
  }

  @ThreadSafe
  private static final class Window {
    private long lastLogTime = Long.MIN_VALUE;
    private long suppressed;

    synchronized long record(long now, long nanos) {
      if (nanos > 0 && lastLogTime != Long.MIN_VALUE && now - lastLogTime < nanos) {
        if (suppressed != Long.MAX_VALUE) {
          suppressed++;
        }
        return -1;
      }
      long count = suppressed;
      suppressed = 0;
      lastLogTime = now;
      return count;
    }
  }
}
