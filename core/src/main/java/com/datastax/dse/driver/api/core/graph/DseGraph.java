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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * General purpose utility class for interaction with DSE Graph via the DataStax Enterprise Java
 * driver.
 *
 * @deprecated DSE Graph is not supported starting with driver 4.19.2.2.
 */
@SuppressWarnings("DoNotCallSuggester")
@Deprecated
public class DseGraph {

  /**
   * Retained for binary compatibility. Graph traversal methods throw with migration guidance when
   * Gremlin is present. If the optional Gremlin dependency is absent, this field is {@code null}.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated public static final GraphTraversalSource g = unsupportedTraversalSource();

  /**
   * Previously returned a remote connection builder for DSE Graph. This method now always throws
   * with migration guidance.
   *
   * @throws UnsupportedOperationException because DSE Graph is no longer supported. The message
   *     gives migration guidance.
   */
  public static DseGraphRemoteConnectionBuilder remoteConnectionBuilder(CqlSession dseSession) {
    throw GraphSupportRemoved.exception();
  }

  private static GraphTraversalSource unsupportedTraversalSource() {
    try {
      Class<?> type = Class.forName(DseGraph.class.getName() + "$UnsupportedTraversalSource");
      return (GraphTraversalSource) type.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException | LinkageError ignored) {
      // Keep Graph entry points loadable when the optional Gremlin dependency is absent.
      return null;
    }
  }

  @SuppressWarnings("UnusedNestedClass")
  private static final class UnsupportedTraversalSource extends GraphTraversalSource {
    private UnsupportedTraversalSource() {
      super(EmptyGraph.instance());
    }

    @Override
    public GraphTraversalSource clone() {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public Graph getGraph() {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> V(Object... vertexIds) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Edge, Edge> E(Object... edgeIds) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> addV(String label) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> addV(Traversal<?, String> label) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Vertex, Vertex> addV() {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Edge, Edge> addE(String label) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public GraphTraversal<Edge, Edge> addE(Traversal<?, String> label) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public <S> GraphTraversal<S, S> inject(S... starts) {
      throw GraphSupportRemoved.exception();
    }

    @Override
    public <S> GraphTraversal<S, S> io(String file) {
      throw GraphSupportRemoved.exception();
    }
  }

  private DseGraph() {
    // nothing to do
  }
}
