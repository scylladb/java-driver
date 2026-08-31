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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import com.datastax.dse.driver.api.core.graph.predicates.CqlCollection;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.dse.driver.api.core.graph.predicates.Search;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphNode;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphResultSet;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphSession;
import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class GraphApiCompatibilityTest {

  @Test
  public void should_retain_deprecated_graph_api_types() {
    Class<?>[] types = {
      AsyncGraphResultSet.class,
      BatchGraphStatement.class,
      BatchGraphStatementBuilder.class,
      DseGraph.class,
      DseGraphRemoteConnectionBuilder.class,
      FluentGraphStatement.class,
      FluentGraphStatementBuilder.class,
      GraphExecutionInfo.class,
      GraphNode.class,
      GraphResultSet.class,
      GraphSession.class,
      GraphStatement.class,
      GraphStatementBuilderBase.class,
      PagingEnabledOptions.class,
      ScriptGraphStatement.class,
      ScriptGraphStatementBuilder.class,
      CqlCollection.class,
      Geo.class,
      Search.class,
      ReactiveGraphNode.class,
      ReactiveGraphResultSet.class,
      ReactiveGraphSession.class,
    };

    assertThat(types).allMatch(type -> type.isAnnotationPresent(Deprecated.class));
    assertThat(GraphSession.class).isAssignableFrom(CqlSession.class);
    assertThat(ReactiveGraphSession.class).isAssignableFrom(CqlSession.class);
  }

  @Test
  public void should_fail_fast_when_graph_api_is_used() {
    assertUnsupported(() -> ScriptGraphStatement.newInstance("g.V()"));
    assertUnsupported(BatchGraphStatement::newInstance);
    assertUnsupported(() -> BatchGraphStatement.newInstance((GraphTraversal[]) null));
    assertUnsupported(BatchGraphStatement::builder);
    assertUnsupported(() -> FluentGraphStatement.newInstance(null));
    assertUnsupported(() -> DseGraph.remoteConnectionBuilder(null));
    assertUnsupported(() -> Search.token("value"));
    GraphSession graphSession = mock(GraphSession.class, CALLS_REAL_METHODS);
    ReactiveGraphSession reactiveGraphSession =
        mock(ReactiveGraphSession.class, CALLS_REAL_METHODS);
    assertUnsupported(() -> graphSession.execute((GraphStatement<?>) null));
    assertUnsupported(() -> reactiveGraphSession.executeReactive(null));
  }

  private static void assertUnsupported(Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
  }
}
