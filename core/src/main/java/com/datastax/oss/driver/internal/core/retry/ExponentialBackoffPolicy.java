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
package com.datastax.oss.driver.internal.core.retry;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.BackoffRetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.SplittableRandom;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default retry policy.
 *
 * <p>This is a very conservative implementation: it triggers a maximum of one retry per request,
 * and only in cases that have a high chance of success (see the method javadocs for detailed
 * explanations of each case).
 *
 * <p>To activate this policy, modify the {@code advanced.retry-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.retry-policy {
 *     class = DefaultRetryPolicy
 *   }
 * }
 * </pre>
 *
 * <p>See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class ExponentialBackoffPolicy implements BackoffRetryPolicy {
  private static final SplittableRandom random = new SplittableRandom();

  private static final Logger LOG = LoggerFactory.getLogger(ExponentialBackoffPolicy.class);

  @VisibleForTesting
  public static final String BACKOFF_ON_READ_TIMEOUT =
      "[{}] Delaying retry on read timeout for {}ms (consistency: {}, required responses: {}, "
          + "received responses: {}, data retrieved: {}, retries: {})";

  @VisibleForTesting
  public static final String BACKOFF_ON_WRITE_TIMEOUT =
      "[{}] Delaying retry on write timeout for {}ms (consistency: {}, write type: {}, "
          + "required acknowledgments: {}, received acknowledgments: {}, retries: {})";

  @VisibleForTesting
  public static final String BACKOFF_ON_UNAVAILABLE =
      "[{}] Delaying retry on unavailable exception for {}ms (consistency: {}, "
          + "required replica: {}, alive replica: {}, retries: {})";

  @VisibleForTesting
  public static final String BACKOFF_ON_ABORTED =
      "[{}] Delaying retry on aborted request for {}ms (retries: {})";

  @VisibleForTesting
  public static final String BACKOFF_ON_ERROR =
      "[{}] Delaying on node error for {}ms (retries: {})";

  private final String logPrefix;
  private final int baseDelayMs;
  private final int maxDelayMs;
  private final double jitterRatio;

  public ExponentialBackoffPolicy(DriverContext context, String profileName) {
    DriverExecutionProfile profile = context.getConfig().getProfile(profileName);
    this.logPrefix = context.getSessionName() + "|" + profileName;
    this.baseDelayMs = profile.getInt(DefaultDriverOption.BACKOFF_RETRY_BASE_BACKOFF_MS);
    this.maxDelayMs = profile.getInt(DefaultDriverOption.BACKOFF_RETRY_MAX_BACKOFF_MS);
    this.jitterRatio = profile.getDouble(DefaultDriverOption.BACKOFF_RETRY_JITTER_RATIO);
  }

  public ExponentialBackoffPolicy(
      String logPrefix, int baseDelayMs, int maxDelayMs, double jitterRatio) {
    this.logPrefix = logPrefix;
    this.baseDelayMs = baseDelayMs;
    this.maxDelayMs = maxDelayMs;
    this.jitterRatio = jitterRatio;
  }

  @Override
  public int onReadTimeoutBackoffMs(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount,
      RetryVerdict verdict) {
    int backoffMs = calculateBackoffMs(retryCount);
    if (LOG.isTraceEnabled() && backoffMs != 0) {
      LOG.trace(
          BACKOFF_ON_READ_TIMEOUT, logPrefix, backoffMs, cl, blockFor, received, false, retryCount);
    }
    return backoffMs;
  }

  @Override
  public int onWriteTimeoutBackoffMs(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount,
      RetryVerdict verdict) {
    int backoffMs = calculateBackoffMs(retryCount);
    if (LOG.isTraceEnabled() && backoffMs != 0) {
      LOG.trace(
          BACKOFF_ON_WRITE_TIMEOUT,
          logPrefix,
          backoffMs,
          cl,
          blockFor,
          received,
          false,
          retryCount);
    }
    return backoffMs;
  }

  @Override
  public int onUnavailableBackoffMs(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount,
      RetryVerdict verdict) {
    int backoffMs = calculateBackoffMs(retryCount);
    if (LOG.isTraceEnabled() && backoffMs != 0) {
      LOG.trace(BACKOFF_ON_UNAVAILABLE, logPrefix, backoffMs, cl, required, alive, retryCount);
    }
    return backoffMs;
  }

  @Override
  public int onRequestAbortedBackoffMs(
      @NonNull Request request, @NonNull Throwable error, int retryCount, RetryVerdict verdict) {
    int backoffMs = calculateBackoffMs(retryCount);
    if (LOG.isTraceEnabled() && backoffMs != 0) {
      LOG.trace(BACKOFF_ON_ABORTED, logPrefix, backoffMs, retryCount, error);
    }
    return backoffMs;
  }

  @Override
  public int onErrorResponseBackoff(
      @NonNull Request request,
      @NonNull CoordinatorException error,
      int retryCount,
      RetryVerdict verdict) {
    int backoffMs = calculateBackoffMs(retryCount);
    if (LOG.isTraceEnabled() && backoffMs != 0) {
      LOG.trace(BACKOFF_ON_ERROR, logPrefix, backoffMs, retryCount, error);
    }
    return backoffMs;
  }

  private int calculateBackoffMs(int attempt) {
    int expDelay = (int) (baseDelayMs * Math.pow(2, attempt - 1));
    int jitter = random.nextInt((int) (jitterRatio * expDelay));
    return Math.min(expDelay + jitter, maxDelayMs);
  }

  @Override
  public void close() {}
}
