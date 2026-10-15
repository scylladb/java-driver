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
package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultKeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.CDCTokenFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Covers the CDC-log detection that decides whether a statement needs the CDC partitioner. */
@RunWith(MockitoJUnitRunner.class)
public class PartitionerFactoryTest {

  private static final String KEYSPACE = "ks";
  private static final String BASE_TABLE = "tbl";
  private static final String CDC_LOG_SUFFIX = "_scylla_cdc_log";
  private static final String CDC_LOG_TABLE = BASE_TABLE + CDC_LOG_SUFFIX;

  @Mock private InternalDriverContext context;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;
  @Mock private ColumnDefinitions variableDefinitions;
  @Mock private ColumnDefinition variableDefinition;
  @Mock private KeyspaceMetadata keyspaceMetadata;
  @Mock private TableMetadata tableMetadata;

  @Before
  public void setup() {
    when(variableDefinitions.size()).thenReturn(1);
    when(variableDefinitions.get(0)).thenReturn(variableDefinition);
    when(variableDefinition.getKeyspace()).thenReturn(CqlIdentifier.fromInternal(KEYSPACE));
    when(variableDefinition.getTable()).thenReturn(CqlIdentifier.fromInternal(CDC_LOG_TABLE));
  }

  @Test
  public void should_return_cdc_partitioner_for_cdc_log_table() {
    givenTableOptions(ImmutableMap.of(CqlIdentifier.fromInternal("extensions"), cdcExtension()));

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context))
        .isInstanceOf(CDCTokenFactory.class);
    // The CDC extension is read from the base table, never from the log table itself
    verify(keyspaceMetadata).getTable(BASE_TABLE);
    verify(keyspaceMetadata, never()).getTable(CDC_LOG_TABLE);
  }

  @Test
  public void should_return_null_when_variable_definitions_are_null() {
    assertThat(PartitionerFactory.partitioner(null, context)).isNull();
  }

  @Test
  public void should_return_null_when_there_are_no_variable_definitions() {
    when(variableDefinitions.size()).thenReturn(0);

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_return_null_when_table_is_not_a_cdc_log() {
    when(variableDefinition.getTable()).thenReturn(CqlIdentifier.fromInternal(BASE_TABLE));

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_return_null_when_keyspace_metadata_is_absent() {
    givenMetadata();
    when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.empty());

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_return_null_when_base_table_is_absent() {
    givenKeyspace();
    when(keyspaceMetadata.getTable(BASE_TABLE)).thenReturn(Optional.empty());

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_return_null_when_base_table_has_no_extensions() {
    givenTableOptions(Collections.emptyMap());

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_return_null_when_extensions_option_is_not_a_map() {
    givenTableOptions(
        ImmutableMap.of(CqlIdentifier.fromInternal("extensions"), "not a map at all"));

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_return_null_when_extensions_do_not_mention_cdc() {
    givenTableOptions(
        ImmutableMap.of(
            CqlIdentifier.fromInternal("extensions"),
            ImmutableMap.of("other", ByteBuffer.allocate(0))));

    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
  }

  @Test
  public void should_not_find_cdc_extension_when_keyspace_name_is_case_sensitive() {
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal("MyKs");
    CqlIdentifier baseTableId = CqlIdentifier.fromInternal(BASE_TABLE);
    Metadata realMetadata = givenRealCdcMetadata(keyspaceId, baseTableId);
    when(variableDefinition.getKeyspace()).thenReturn(keyspaceId);

    // Documents current, incorrect behaviour, see scylladb/java-driver#1087: partitioner()
    // stringifies both identifiers and looks them up through the String overloads, which run
    // CqlIdentifier.fromCql and lowercase. Here it is the keyspace lookup that misses, so the
    // statement silently routes on Murmur3 instead of the CDC partitioner.
    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
    assertCdcExtensionIsReachable(realMetadata, keyspaceId, baseTableId);
  }

  @Test
  public void should_not_find_cdc_extension_when_base_table_name_is_case_sensitive() {
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(KEYSPACE);
    CqlIdentifier baseTableId = CqlIdentifier.fromInternal("MyTbl");
    Metadata realMetadata = givenRealCdcMetadata(keyspaceId, baseTableId);
    when(variableDefinition.getTable())
        .thenReturn(CqlIdentifier.fromInternal("MyTbl" + CDC_LOG_SUFFIX));

    // The other half of #1087. The keyspace name survives the round-trip here, so the base-table
    // lookup is the one that lowercases and misses. Pinned on its own, because fixing only one of
    // the two lookups would otherwise leave the suite green.
    assertThat(PartitionerFactory.partitioner(variableDefinitions, context)).isNull();
    assertCdcExtensionIsReachable(realMetadata, keyspaceId, baseTableId);
  }

  @Test
  public void should_throw_when_base_table_name_is_the_whole_cdc_suffix() {
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(KEYSPACE);
    CqlIdentifier baseTableId = CqlIdentifier.fromInternal(BASE_TABLE);
    givenRealCdcMetadata(keyspaceId, baseTableId);
    when(variableDefinition.getTable()).thenReturn(CqlIdentifier.fromInternal(CDC_LOG_SUFFIX));

    // Same family as #1087: a table named exactly "_scylla_cdc_log" leaves baseTableName empty,
    // and CqlIdentifier.fromCql("") crashes inside Strings.needsDoubleQuotes rather than
    // resolving to "no CDC partitioner".
    assertThatThrownBy(() -> PartitionerFactory.partitioner(variableDefinitions, context))
        .isInstanceOfAny(AssertionError.class, StringIndexOutOfBoundsException.class);
  }

  @Test
  public void should_throw_when_keyspace_name_needs_double_quotes() {
    when(variableDefinition.getKeyspace()).thenReturn(CqlIdentifier.fromInternal("my-ks"));
    givenMetadata(DefaultMetadata.EMPTY);

    // Documents current, incorrect behaviour, see scylladb/java-driver#1087: a keyspace whose
    // name is legal only in quoted form aborts the prepare path outright, where resolving no CDC
    // partitioner would do.
    assertThatThrownBy(() -> PartitionerFactory.partitioner(variableDefinitions, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid CQL form [my-ks]: needs double quotes");
  }

  @Test
  public void should_throw_when_base_table_name_needs_double_quotes() {
    CqlIdentifier keyspaceId = CqlIdentifier.fromInternal(KEYSPACE);
    CqlIdentifier baseTableId = CqlIdentifier.fromInternal("my-tbl");
    givenRealCdcMetadata(keyspaceId, baseTableId);
    when(variableDefinition.getTable())
        .thenReturn(CqlIdentifier.fromInternal("my-tbl" + CDC_LOG_SUFFIX));

    // The other half again: the keyspace resolves, so it is the base-table lookup that reaches
    // Strings.needsDoubleQuotes and aborts the prepare path. Pinned separately for the same reason
    // as the case-sensitivity pair above.
    assertThatThrownBy(() -> PartitionerFactory.partitioner(variableDefinitions, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid CQL form [my-tbl]: needs double quotes");
  }

  /** The base table and its CDC extension are there all along, under the real identifiers. */
  private static void assertCdcExtensionIsReachable(
      Metadata realMetadata, CqlIdentifier keyspaceId, CqlIdentifier baseTableId) {
    assertThat(realMetadata.getKeyspace(keyspaceId).flatMap(ks -> ks.getTable(baseTableId)))
        .hasValueSatisfying(
            table ->
                assertThat(table.getOptions())
                    .containsKey(CqlIdentifier.fromInternal("extensions")));
  }

  private static Map<String, Object> cdcExtension() {
    return ImmutableMap.of("cdc", ByteBuffer.wrap(new byte[] {1}));
  }

  /**
   * Real metadata rather than mocks: the lookups under test are default methods on the metadata
   * interfaces, and Mockito would intercept them instead of letting CqlIdentifier.fromCql run.
   */
  private Metadata givenRealCdcMetadata(CqlIdentifier keyspaceId, CqlIdentifier baseTableId) {
    TableMetadata baseTable =
        new DefaultTableMetadata(
            keyspaceId,
            baseTableId,
            null,
            false,
            false,
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            ImmutableMap.of(CqlIdentifier.fromInternal("extensions"), cdcExtension()),
            Collections.emptyMap());
    KeyspaceMetadata keyspace =
        new DefaultKeyspaceMetadata(
            keyspaceId,
            true,
            false,
            Collections.emptyMap(),
            Collections.emptyMap(),
            ImmutableMap.of(baseTableId, baseTable),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap());
    Metadata realMetadata =
        new DefaultMetadata(
            Collections.emptyMap(), ImmutableMap.of(keyspaceId, keyspace), null, null);
    givenMetadata(realMetadata);
    return realMetadata;
  }

  private void givenMetadata() {
    givenMetadata(metadata);
  }

  private void givenMetadata(Metadata exposedMetadata) {
    when(context.getMetadataManager()).thenReturn(metadataManager);
    when(metadataManager.getMetadata()).thenReturn(exposedMetadata);
  }

  private void givenKeyspace() {
    givenMetadata();
    when(metadata.getKeyspace(KEYSPACE)).thenReturn(Optional.of(keyspaceMetadata));
  }

  private void givenTableOptions(Map<CqlIdentifier, Object> options) {
    givenKeyspace();
    when(keyspaceMetadata.getTable(BASE_TABLE)).thenReturn(Optional.of(tableMetadata));
    when(tableMetadata.getOptions()).thenReturn(options);
  }
}
