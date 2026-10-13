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
package com.datastax.dse.driver.api.core.cql.continuous;

import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;

/**
 * Retained session signatures for DSE continuous paging.
 *
 * @deprecated DSE continuous paging is no longer supported. Default execution methods fail with
 *     {@link UnsupportedOperationException}; use ordinary CQL paging through {@code CqlSession}.
 */
@Deprecated
public interface ContinuousSession extends Session {

  /**
   * Retained synchronous continuous paging signature.
   *
   * @throws UnsupportedOperationException always, because DSE continuous paging was removed.
   * @deprecated DSE continuous paging is no longer supported.
   */
  @Deprecated
  @NonNull
  default ContinuousResultSet executeContinuously(@NonNull Statement<?> statement) {
    throw new UnsupportedOperationException("DSE continuous paging is no longer supported");
  }

  /**
   * Retained asynchronous continuous paging signature.
   *
   * <p>Returns a stage that fails with {@link UnsupportedOperationException}; use ordinary CQL
   * paging through {@code CqlSession}.
   *
   * @deprecated DSE continuous paging is no longer supported.
   */
  @Deprecated
  @NonNull
  default CompletionStage<ContinuousAsyncResultSet> executeContinuouslyAsync(
      @NonNull Statement<?> statement) {
    return CompletableFutures.failedFuture(
        new UnsupportedOperationException(
            "DSE continuous paging is no longer supported; use ordinary CQL paging through CqlSession"));
  }
}
