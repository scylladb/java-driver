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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;

/**
 * Retained compatibility interface for DSE Graph execution methods.
 *
 * <p>{@link CqlSession} retains this interface for source and binary compatibility. Its default
 * execution methods fail because DSE Graph execution is no longer supported.
 *
 * @deprecated DSE Graph is not supported starting with driver 4.19.2.3.
 */
@SuppressWarnings("DoNotCallSuggester")
@Deprecated
public interface GraphSession extends Session {

  /**
   * Retained signature for synchronous graph execution.
   *
   * @param graphStatement the graph query
   * @throws UnsupportedOperationException always; use a driver that supports DSE Graph execution
   */
  @NonNull
  default GraphResultSet execute(@NonNull GraphStatement<?> graphStatement) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Retained signature for asynchronous graph execution.
   *
   * <p>Returns a stage that fails with {@link UnsupportedOperationException} and migration
   * guidance; use a driver that supports DSE Graph execution.
   *
   * @param graphStatement the graph query
   */
  @NonNull
  default CompletionStage<AsyncGraphResultSet> executeAsync(
      @NonNull GraphStatement<?> graphStatement) {
    return CompletableFutures.failedFuture(GraphSupportRemoved.exception());
  }
}
