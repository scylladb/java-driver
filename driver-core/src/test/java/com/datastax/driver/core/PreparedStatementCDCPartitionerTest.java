/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PreparedStatementCDCPartitionerTest {

  private Cluster cluster;
  private Metadata metadata;

  @BeforeMethod(groups = "unit")
  public void setUp() throws Exception {
    // Real Metadata.getKeyspace / KeyspaceMetadata.getTable, so the identifier handling is real.
    metadata = mock(Metadata.class);
    when(metadata.getKeyspace(anyString())).thenCallRealMethod();
    Field keyspaces = Metadata.class.getDeclaredField("keyspaces");
    keyspaces.setAccessible(true);
    keyspaces.set(metadata, new ConcurrentHashMap<String, KeyspaceMetadata>());

    cluster = mock(Cluster.class);
    when(cluster.getMetadata()).thenReturn(metadata);
  }

  @Test(groups = "unit")
  public void should_use_cdc_partitioner_for_log_table_of_cdc_enabled_table() throws Exception {
    addTable("ks", "tbl", true);

    assertThat(partitioner(defs("ks", "tbl_scylla_cdc_log"))).isSameAs(Token.CDCToken.FACTORY);
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_for_base_table() throws Exception {
    addTable("ks", "tbl", true);

    assertThat(partitioner(defs("ks", "tbl"))).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_when_base_table_has_cdc_disabled() throws Exception {
    addTable("ks", "tbl", false);

    assertThat(partitioner(defs("ks", "tbl_scylla_cdc_log"))).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_when_base_table_is_unknown() throws Exception {
    addTable("ks", "other", true);

    assertThat(partitioner(defs("ks", "tbl_scylla_cdc_log"))).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_when_keyspace_is_unknown() throws Exception {
    addTable("ks", "tbl", true);

    assertThat(partitioner(defs("other", "tbl_scylla_cdc_log"))).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_for_bare_suffix_table_name() throws Exception {
    addTable("ks", "tbl", true);

    assertThat(partitioner(defs("ks", "_scylla_cdc_log"))).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_without_column_definitions() throws Exception {
    assertThat(partitioner(null)).isNull();
    assertThat(partitioner(ColumnDefinitions.EMPTY)).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_for_case_sensitive_keyspace() throws Exception {
    addTable("MyKs", "tbl", true);

    // Pins current behaviour (#1178): partitioner() passes the case-sensitive keyspace name
    // unquoted to Metadata.getKeyspace, which lowercases it, so routing falls back to Murmur3
    assertThat(partitioner(defs("MyKs", "tbl_scylla_cdc_log"))).isNull();
  }

  @Test(groups = "unit")
  public void should_not_use_cdc_partitioner_for_case_sensitive_base_table() throws Exception {
    addTable("ks", "MyTbl", true);

    // Pins current behaviour (#1178): partitioner() passes the case-sensitive base table name
    // unquoted to KeyspaceMetadata.getTable, which lowercases it, so routing falls back to Murmur3
    assertThat(partitioner(defs("ks", "MyTbl_scylla_cdc_log"))).isNull();
  }

  private void addTable(String keyspace, String table, boolean cdcEnabled) throws Exception {
    KeyspaceMetadata ksm = metadata.keyspaces.get(keyspace);
    if (ksm == null) {
      ksm = new KeyspaceMetadata(keyspace, true, Collections.<String, String>emptyMap(), false);
      metadata.keyspaces.put(keyspace, ksm);
    }
    TableOptionsMetadata options = mock(TableOptionsMetadata.class);
    when(options.isScyllaCDC()).thenReturn(cdcEnabled);
    ksm.add(newTable(ksm, table, options));
  }

  private static TableMetadata newTable(
      KeyspaceMetadata ksm, String name, TableOptionsMetadata options) throws Exception {
    Constructor<TableMetadata> constructor =
        TableMetadata.class.getDeclaredConstructor(
            KeyspaceMetadata.class,
            String.class,
            UUID.class,
            List.class,
            List.class,
            Map.class,
            Map.class,
            TableOptionsMetadata.class,
            List.class,
            VersionNumber.class);
    constructor.setAccessible(true);
    return constructor.newInstance(
        ksm,
        name,
        new UUID(0L, 0L),
        Collections.<ColumnMetadata>emptyList(),
        Collections.<ColumnMetadata>emptyList(),
        Collections.<String, ColumnMetadata>emptyMap(),
        Collections.<String, IndexMetadata>emptyMap(),
        options,
        Collections.<ClusteringOrder>emptyList(),
        VersionNumber.parse("3.0.8"));
  }

  private static ColumnDefinitions defs(String keyspace, String table) {
    return new ColumnDefinitions(
        new ColumnDefinitions.Definition[] {
          new ColumnDefinitions.Definition(keyspace, table, "pk", DataType.blob())
        },
        CodecRegistry.DEFAULT_INSTANCE);
  }

  private Token.Factory partitioner(ColumnDefinitions defs) throws Exception {
    Method method =
        DefaultPreparedStatement.class.getDeclaredMethod(
            "partitioner", ColumnDefinitions.class, Cluster.class);
    method.setAccessible(true);
    return (Token.Factory) method.invoke(null, defs, cluster);
  }
}
