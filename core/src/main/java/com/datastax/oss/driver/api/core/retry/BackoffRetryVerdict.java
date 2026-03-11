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
package com.datastax.oss.driver.api.core.retry;

import java.time.Duration;

/**
 * The verdict returned by a {@link RetryPolicy} determining what to do when a request failed. A
 * verdict contains a {@link RetryDecision} indicating if a retry should be attempted at all and
 * where, with what delay, and a method that allows the original request to be modified before the
 * retry.
 */
public interface BackoffRetryVerdict extends RetryVerdict {

  /** @return a delay that request needs to take before retrying. */
  Duration getRetryBackoff();
}
