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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.BackoffRetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

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
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class NoBackoffPolicy implements BackoffRetryPolicy {

  public NoBackoffPolicy(DriverContext context, String profileName) {}

  public NoBackoffPolicy() {}

  @Override
  public int onReadTimeoutBackoffMs(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount,
      RetryVerdict verdict) {
    return 0;
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
    return 0;
  }

  @Override
  public int onUnavailableBackoffMs(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount,
      RetryVerdict verdict) {
    return 0;
  }

  @Override
  public int onRequestAbortedBackoffMs(
      @NonNull Request request, @NonNull Throwable error, int retryCount, RetryVerdict verdict) {
    return 0;
  }

  @Override
  public int onErrorResponseBackoff(
      @NonNull Request request,
      @NonNull CoordinatorException error,
      int retryCount,
      RetryVerdict verdict) {
    return 0;
  }

  @Override
  public void close() {}
}
