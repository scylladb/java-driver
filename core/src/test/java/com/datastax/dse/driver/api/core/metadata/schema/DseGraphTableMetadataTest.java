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

import static com.datastax.oss.driver.api.core.CqlIdentifier.fromCql;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    when(vertex.getLabelName()).thenReturn(fromCql("person"));
    when(table.getVertex()).thenReturn(Optional.of(vertex));

    assertThat(table.describe(false)).contains("VERTEX LABEL \"person\"");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_describe_edge_label_and_sides() {
    DseGraphTableMetadata table = table();
    DseEdgeMetadata edge = mock(DseEdgeMetadata.class);
    when(edge.getLabelName()).thenReturn(fromCql("knows"));
    when(edge.getFromTable()).thenReturn(fromCql("vertices"));
    when(edge.getFromLabel()).thenReturn(fromCql("person"));
    when(edge.getFromPartitionKeyColumns()).thenReturn(ImmutableList.of(fromCql("from_id")));
    when(edge.getFromClusteringColumns()).thenReturn(ImmutableList.of());
    when(edge.getToTable()).thenReturn(fromCql("vertices"));
    when(edge.getToLabel()).thenReturn(fromCql("person"));
    when(edge.getToPartitionKeyColumns()).thenReturn(ImmutableList.of(fromCql("to_id")));
    when(edge.getToClusteringColumns()).thenReturn(ImmutableList.of());
    when(table.getEdge()).thenReturn(Optional.of(edge));

    assertThat(table.describe(false))
        .contains("EDGE LABEL \"knows\"")
        .contains("FROM\"person\"(\"from_id\")")
        .contains("TO\"person\"(\"to_id\")");
  }

  private static DseGraphTableMetadata table() {
    DseGraphTableMetadata table = mock(DseGraphTableMetadata.class);
    ColumnMetadata id = mock(ColumnMetadata.class);
    when(id.getName()).thenReturn(fromCql("id"));
    when(id.getType()).thenReturn(new PrimitiveType(DataType.INT));
    when(table.getKeyspace()).thenReturn(fromCql("ks"));
    when(table.getName()).thenReturn(fromCql("vertices"));
    when(table.getColumns()).thenReturn(ImmutableMap.of(fromCql("id"), id));
    when(table.getPartitionKey()).thenReturn(ImmutableList.of(id));
    when(table.getClusteringColumns()).thenReturn(ImmutableMap.of());
    when(table.getOptions()).thenReturn(ImmutableMap.of());
    when(table.getVertex()).thenReturn(Optional.empty());
    when(table.getEdge()).thenReturn(Optional.empty());
    when(table.describe(false)).thenCallRealMethod();
    return table;
  }
}
