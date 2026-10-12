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
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

/**
 * A builder to create a batch graph statement.
 *
 * <p>This class is mutable and not thread-safe.
 *
 * @deprecated DSE Graph is not supported starting with driver 4.19.2.2.
 */
@NotThreadSafe
@SuppressWarnings("DoNotCallSuggester")
@Deprecated
public class BatchGraphStatementBuilder
    extends GraphStatementBuilderBase<BatchGraphStatementBuilder, BatchGraphStatement> {

  public BatchGraphStatementBuilder() {
    throw GraphSupportRemoved.exception();
  }

  public BatchGraphStatementBuilder(BatchGraphStatement template) {
    super(template);
    throw GraphSupportRemoved.exception();
  }

  /** Add a traversal to this builder to include in the generated {@link BatchGraphStatement}. */
  @NonNull
  public BatchGraphStatementBuilder addTraversal(@NonNull GraphTraversal traversal) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Add several traversals to this builder to include in the generated {@link BatchGraphStatement}.
   */
  @NonNull
  public BatchGraphStatementBuilder addTraversals(@NonNull Iterable<GraphTraversal> traversals) {
    throw GraphSupportRemoved.exception();
  }

  /**
   * Add several traversals to this builder to include in the generated {@link BatchGraphStatement}.
   */
  @NonNull
  public BatchGraphStatementBuilder addTraversals(@NonNull GraphTraversal... traversals) {
    throw GraphSupportRemoved.exception();
  }

  /** Clears all the traversals previously added to this builder. */
  @NonNull
  public BatchGraphStatementBuilder clearTraversals() {
    throw GraphSupportRemoved.exception();
  }

  /** Returns the number of traversals added to this statement so far. */
  public int getTraversalsCount() {
    throw GraphSupportRemoved.exception();
  }

  @NonNull
  @Override
  public BatchGraphStatement build() {
    throw GraphSupportRemoved.exception();
  }
}
