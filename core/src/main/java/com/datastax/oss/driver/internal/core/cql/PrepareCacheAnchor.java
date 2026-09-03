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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Internal hook allowing a {@link PreparedStatement} to keep its prepare cache entry reachable.
 *
 * <p>{@link CqlPrepareAsyncProcessor} caches prepare futures with weak values, so an entry can be
 * collected while the application still holds the resulting statement, causing a needless
 * re-PREPARE. Storing the cached future on the statement ties the entry's lifetime to the
 * statement's: the cache holds a weak reference to the future, the future references the statement,
 * and the statement references the future back. The cycle stays reachable while the application
 * holds the statement, and becomes collectible as a whole once it does not.
 *
 * <p>Implementations only need to retain the reference; the anchor is never read back.
 */
public interface PrepareCacheAnchor {

  /**
   * Retains the prepare cache entry for this statement, preventing its weak-value eviction for as
   * long as this statement is reachable.
   */
  void setPrepareCacheAnchor(@Nullable CompletableFuture<PreparedStatement> anchor);
}
