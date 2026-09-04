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

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveSession;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveSession;
import com.datastax.dse.driver.api.core.graph.GraphSession;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphSession;
import com.datastax.oss.driver.api.core.cql.AsyncCqlSession;
import com.datastax.oss.driver.api.core.cql.SyncCqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The default session type built by the driver.
 *
 * <p>It provides user-friendly execution methods for synchronous, asynchronous, reactive, and
 * continuous-paging CQL requests.
 *
 * <p>Client applications can use this interface even if they don't need all the features. In
 * particular, it can be used with a regular Apache Cassandra &reg; cluster, as long as you don't
 * call continuous-paging methods.
 */
public interface CqlSession
    extends Session,
        SyncCqlSession,
        AsyncCqlSession,
        ReactiveSession,
        ContinuousSession,
        GraphSession,
        ContinuousReactiveSession,
        ReactiveGraphSession {

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
