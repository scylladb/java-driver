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
package com.datastax.dse.driver.api.core.metadata.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.internal.core.type.PrimitiveType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants.DataType;
import java.util.Optional;
import org.junit.Test;

public class DseGraphTableMetadataTest {

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_restored_graph_metadata() {
    DseGraphTableMetadata table = table();

    assertThat(table.describe(false))
        .contains("CREATE TABLE \"ks\".\"vertices\"", "PRIMARY KEY (\"id\")");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_vertex_label() {
    DseGraphTableMetadata table = table();
    DseVertexMetadata vertex = mock(DseVertexMetadata.class);
    when(vertex.getLabelName()).thenReturn(CqlIdentifier.fromCql("person"));
    when(table.getVertex()).thenReturn(Optional.of(vertex));

    assertThat(table.describe(false)).contains("VERTEX LABEL \"person\"");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_edge_label_and_sides() {
    DseGraphTableMetadata table = table();
    DseEdgeMetadata edge = edge();
    when(table.getEdge()).thenReturn(Optional.of(edge));

    assertThat(table.describe(false))
        .contains("EDGE LABEL \"knows\"")
        .contains("FROM \"person\"(\"from_id\")")
        .contains("TO \"software\"(\"to_id\")");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_pretty_edge_label_and_sides() {
    DseGraphTableMetadata table = table();
    DseEdgeMetadata edge = edge();
    when(table.getEdge()).thenReturn(Optional.of(edge));

    assertThat(table.describe(true))
        .contains("EDGE LABEL knows")
        .contains("FROM person(from_id)")
        .contains("TO software(to_id)");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_composite_edge_endpoints_with_clustering() {
    DseGraphTableMetadata table = table();
    DseEdgeMetadata edge = edge();
    when(table.getEdge()).thenReturn(Optional.of(edge));
    when(edge.getFromPartitionKeyColumns())
        .thenReturn(
            ImmutableList.of(
                CqlIdentifier.fromCql("\"Region\""), CqlIdentifier.fromCql("from_id")));
    when(edge.getFromClusteringColumns())
        .thenReturn(ImmutableList.of(CqlIdentifier.fromCql("from_time")));
    when(edge.getToPartitionKeyColumns())
        .thenReturn(
            ImmutableList.of(CqlIdentifier.fromCql("to_region"), CqlIdentifier.fromCql("to_id")));
    when(edge.getToClusteringColumns())
        .thenReturn(ImmutableList.of(CqlIdentifier.fromCql("to_time")));

    assertThat(table.describe(false))
        .contains("FROM \"person\"((\"Region\", \"from_id\"), \"from_time\")")
        .contains("TO \"software\"((\"to_region\", \"to_id\"), \"to_time\")");
    assertThat(table.describe(true))
        .contains("FROM person((\"Region\", from_id), from_time)")
        .contains("TO software((to_region, to_id), to_time)");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_graph_clauses_with_clustering_order_and_options() {
    DseGraphTableMetadata table = table();
    DseEdgeMetadata edge = edge();
    ColumnMetadata id = table.getPartitionKey().get(0);
    ColumnMetadata time = mock(ColumnMetadata.class);
    when(time.getName()).thenReturn(CqlIdentifier.fromCql("event_time"));
    when(time.getType()).thenReturn(new PrimitiveType(DataType.INT));
    when(table.getColumns())
        .thenReturn(
            ImmutableMap.of(
                CqlIdentifier.fromCql("id"), id, CqlIdentifier.fromCql("event_time"), time));
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of(time, ClusteringOrder.DESC));
    when(table.getOptions())
        .thenReturn(ImmutableMap.of(CqlIdentifier.fromCql("comment"), "graph's table"));
    when(table.getEdge()).thenReturn(Optional.of(edge));

    assertThat(table.describe(false))
        .contains("PRIMARY KEY (\"id\", \"event_time\")")
        .endsWith(
            "WITH CLUSTERING ORDER BY (\"event_time\" DESC)"
                + " AND EDGE LABEL \"knows\" FROM \"person\"(\"from_id\") TO \"software\"(\"to_id\")"
                + " AND comment = 'graph''s table';");
    assertThat(table.describe(true))
        .contains("PRIMARY KEY (id, event_time)")
        .endsWith(
            "WITH CLUSTERING ORDER BY (event_time DESC)\n"
                + "    AND EDGE LABEL knows FROM person(from_id) TO software(to_id)\n"
                + "    AND comment = 'graph''s table';");
  }

  private static DseEdgeMetadata edge() {
    DseEdgeMetadata edge = mock(DseEdgeMetadata.class);
    when(edge.getLabelName()).thenReturn(CqlIdentifier.fromCql("knows"));
    when(edge.getFromTable()).thenReturn(CqlIdentifier.fromCql("vertices"));
    when(edge.getFromLabel()).thenReturn(CqlIdentifier.fromCql("person"));
    when(edge.getFromPartitionKeyColumns())
        .thenReturn(ImmutableList.of(CqlIdentifier.fromCql("from_id")));
    when(edge.getFromClusteringColumns()).thenReturn(ImmutableList.of());
    when(edge.getToTable()).thenReturn(CqlIdentifier.fromCql("vertices"));
    when(edge.getToLabel()).thenReturn(CqlIdentifier.fromCql("software"));
    when(edge.getToPartitionKeyColumns())
        .thenReturn(ImmutableList.of(CqlIdentifier.fromCql("to_id")));
    when(edge.getToClusteringColumns()).thenReturn(ImmutableList.of());
    return edge;
  }

  private static DseGraphTableMetadata table() {
    DseGraphTableMetadata table = mock(DseGraphTableMetadata.class);
    ColumnMetadata id = mock(ColumnMetadata.class);
    when(id.getName()).thenReturn(CqlIdentifier.fromCql("id"));
    when(id.getType()).thenReturn(new PrimitiveType(DataType.INT));
    when(table.getKeyspace()).thenReturn(CqlIdentifier.fromCql("ks"));
    when(table.getName()).thenReturn(CqlIdentifier.fromCql("vertices"));
    when(table.getColumns()).thenReturn(ImmutableMap.of(CqlIdentifier.fromCql("id"), id));
    when(table.getPartitionKey()).thenReturn(ImmutableList.of(id));
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of());
    when(table.getOptions()).thenReturn(ImmutableMap.of());
    when(table.getVertex()).thenReturn(Optional.empty());
    when(table.getEdge()).thenReturn(Optional.empty());
    when(table.describe(false)).thenCallRealMethod();
    when(table.describe(true)).thenCallRealMethod();
    return table;
  }
}
