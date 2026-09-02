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
package com.datastax.oss.driver.api.core;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveSession;
import com.datastax.oss.driver.api.core.cql.AsyncCqlSession;
import com.datastax.oss.driver.api.core.cql.SyncCqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The default session type built by the driver.
 *
 * <p>It provides user-friendly execution methods for synchronous, asynchronous, and reactive CQL
 * requests.
 */
public interface CqlSession extends Session, SyncCqlSession, AsyncCqlSession, ReactiveSession {

  /**
   * Returns a builder to create a new instance.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   *
   * @return {@code CqlSessionBuilder} to create a new instance.
   */
  @NonNull
  static CqlSessionBuilder builder() {
    return new CqlSessionBuilder();
  }
}
