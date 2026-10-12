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

import com.datastax.dse.driver.api.core.auth.ProxyAuthentication;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphSession;
import com.datastax.dse.driver.internal.core.graph.GraphSupportRemoved;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class GraphApiCompatibilityTest {

  @Test
  public void should_retain_graph_session_interfaces() {
    assertThat(GraphSession.class).isAssignableFrom(CqlSession.class);
    assertThat(ReactiveGraphSession.class).isAssignableFrom(CqlSession.class);
    assertThat(GraphStatement.class).hasAnnotation(Deprecated.class);
    assertThat(Geo.point(1, 2)).isEqualTo(Point.fromCoordinates(1, 2));
  }

  @Test
  public void should_fail_fast_at_graph_entry_points() {
    GraphSession graphSession = mock(GraphSession.class, CALLS_REAL_METHODS);
    ReactiveGraphSession reactiveGraphSession =
        mock(ReactiveGraphSession.class, CALLS_REAL_METHODS);
    GraphStatement<?> statement = mock(GraphStatement.class);
    ScriptGraphStatement scriptStatement = mock(ScriptGraphStatement.class);

    assertThatThrownBy(() -> ScriptGraphStatement.newInstance("g.V()"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
    assertThatThrownBy(() -> graphSession.execute(statement))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
    assertThatThrownBy(() -> reactiveGraphSession.executeReactive(statement))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
    assertThatThrownBy(() -> ProxyAuthentication.executeAs("role", scriptStatement))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(GraphSupportRemoved.MESSAGE);
  }
}
