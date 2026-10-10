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

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.graph.predicates.CqlCollection;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.dse.driver.api.core.graph.predicates.Search;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphNode;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphResultSet;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphSession;
import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import java.io.InputStream;
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
    assertUnsupported(() -> DseGraph.g.V());
    assertUnsupported(() -> DseGraph.g.with("legacy"));
    assertUnsupported(() -> DseGraph.g.getGraph());
    assertUnsupported(() -> Search.token("value"));
    GraphSession graphSession = mock(GraphSession.class, CALLS_REAL_METHODS);
    ReactiveGraphSession reactiveGraphSession =
        mock(ReactiveGraphSession.class, CALLS_REAL_METHODS);
    assertUnsupported(() -> graphSession.execute((GraphStatement<?>) null));
    assertUnsupported(() -> reactiveGraphSession.executeReactive(null));
  }

  @Test
  public void should_keep_graph_geometry_factory_shortcuts() {
    Point first = Geo.point(1, 2);
    Point second = Geo.point(3, 4);
    Point third = Geo.point(5, 6);

    assertThat(first).isEqualTo(Point.fromCoordinates(1, 2));
    assertThat(Geo.lineString(first, second)).isEqualTo(Geo.lineString(1, 2, 3, 4));
    assertThat(Geo.polygon(first, second, third).asWellKnownText())
        .isEqualTo(Geo.polygon(1, 2, 3, 4, 5, 6).asWellKnownText());
    assertThat(Geo.Unit.DEGREES.toDegrees(1)).isEqualTo(1);
    assertThat(Geo.Unit.KILOMETERS.toDegrees(1)).isPositive();
    assertUnsupported(() -> Geo.inside(first, 1));
  }

  @Test
  public void should_fail_with_migration_guidance_when_generic_execution_is_used() {
    GraphStatement<?> statement = mock(GraphStatement.class);
    RequestProcessorRegistry registry = new RequestProcessorRegistry("test");

    assertUnsupported(() -> registry.processorFor(statement, GraphStatement.SYNC));
    assertUnsupported(() -> registry.processorFor(statement, GraphStatement.ASYNC));
  }

  @Test
  public void should_fail_with_migration_guidance_without_gremlin_runtime() throws Exception {
    byte[] graphClassBytes;
    try (InputStream input = DseGraph.class.getResourceAsStream("DseGraph.class")) {
      graphClassBytes = input.readAllBytes();
    }
    ClassLoader withoutGremlin =
        new ClassLoader(DseGraph.class.getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.tinkerpop.")) {
              throw new ClassNotFoundException(name);
            }
            if (name.equals(DseGraph.class.getName())
                || name.equals(DseGraph.class.getName() + "$UnsupportedTraversalSource")) {
              synchronized (getClassLoadingLock(name)) {
                Class<?> type = findLoadedClass(name);
                if (type == null) {
                  byte[] classBytes = graphClassBytes;
                  if (!name.equals(DseGraph.class.getName())) {
                    try (InputStream input =
                        DseGraph.class
                            .getClassLoader()
                            .getResourceAsStream(name.replace('.', '/') + ".class")) {
                      classBytes = input.readAllBytes();
                    } catch (Exception e) {
                      throw new ClassNotFoundException(name, e);
                    }
                  }
                  type = defineClass(name, classBytes, 0, classBytes.length);
                }
                if (resolve) {
                  resolveClass(type);
                }
                return type;
              }
            }
            return super.loadClass(name, resolve);
          }
        };

    Class<?> graphClass = Class.forName(DseGraph.class.getName(), true, withoutGremlin);
    assertThatThrownBy(
            () ->
                graphClass
                    .getMethod("remoteConnectionBuilder", CqlSession.class)
                    .invoke(null, (Object) null))
        .hasCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage(GraphSupportRemoved.MESSAGE);
  }

  private static void assertUnsupported(Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
  }
}
