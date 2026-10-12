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
import java.time.Duration;
import java.util.Arrays;
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
  private final Logger delegate;
  private final Supplier<Duration> interval;
  private final ConcurrentMap<String, Window> windows = new ConcurrentHashMap<>();
  private final LongSupplier clock;

  public RateLimitedLogger(Logger delegate, DriverContext context) {
    this(delegate, () -> configuredInterval(context));
  }

  private static Duration configuredInterval(DriverContext context) {
    DriverExecutionProfile profile = context.getConfig().getDefaultProfile();
    return profile.isDefined(DefaultDriverOption.REPEATED_LOG_INTERVAL)
        ? profile.getDuration(DefaultDriverOption.REPEATED_LOG_INTERVAL)
        : Duration.ofMinutes(1);
  }

  public RateLimitedLogger(Logger delegate, Supplier<Duration> interval) {
    this(delegate, interval, System::nanoTime);
  }

  RateLimitedLogger(Logger delegate, Supplier<Duration> interval, LongSupplier clock) {
    this.delegate = delegate;
    this.interval = interval;
    this.clock = clock;
  }

  public void warn(String format, Object... arguments) {
    if (delegate.isWarnEnabled()) {
      long suppressed = record("warn:" + format);
      if (suppressed >= 0) {
        delegate.warn(
            suppressed == 0 ? format : format + SUPPRESSED_SUFFIX,
            suppressed == 0 ? arguments : withSuppressedCount(arguments, suppressed));
      }
    }
  }

  public void warnWithException(String format, Object... arguments) {
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

  public void error(String format, Object... arguments) {
    if (delegate.isErrorEnabled()) {
      long suppressed = record("error:" + format);
      if (suppressed >= 0) {
        delegate.error(
            suppressed == 0 ? format : format + SUPPRESSED_SUFFIX,
            suppressed == 0 ? arguments : withSuppressedCount(arguments, suppressed));
      }
    }
  }

  private long record(String key) {
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

  private static Object[] withSuppressedCount(Object[] arguments, long suppressed) {
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
