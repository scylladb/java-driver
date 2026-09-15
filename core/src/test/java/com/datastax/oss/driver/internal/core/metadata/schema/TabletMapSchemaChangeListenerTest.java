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
package com.datastax.oss.driver.internal.core.metadata.schema;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * TabletMapSchemaChangesIT already proves the forwarding end to end: the driver registers a real
 * listener alongside the mock the IT installs, so the IT's eviction assertions only pass because
 * the real one ran. These cover what it cannot -- they need no CCM or tablets-capable Scylla, they
 * reach the no-tablet-map early return, and they tell the previous identifier from the current one,
 * which an ALTER leaves unchanged.
 */
@RunWith(MockitoJUnitRunner.Strict.class)
public class TabletMapSchemaChangeListenerTest {

  private static final CqlIdentifier KS = CqlIdentifier.fromCql("ks");
  private static final CqlIdentifier PREVIOUS_KS = CqlIdentifier.fromCql("ks_previous");
  private static final CqlIdentifier TABLE = CqlIdentifier.fromCql("tab");
  private static final CqlIdentifier PREVIOUS_TABLE = CqlIdentifier.fromCql("tab_previous");

  @Mock private MetadataManager manager;
  @Mock private Metadata metadata;
  @Mock private TabletMap tabletMap;
  @Mock private KeyspaceMetadata keyspace;
  @Mock private KeyspaceMetadata previousKeyspace;
  @Mock private TableMetadata table;
  @Mock private TableMetadata previousTable;

  private TabletMapSchemaChangeListener listener;

  @Before
  public void setup() {
    when(manager.getMetadata()).thenReturn(metadata);
    listener = new TabletMapSchemaChangeListener(manager);
  }

  @Test
  public void should_evict_keyspace_when_it_is_dropped() {
    givenTabletMap();
    when(keyspace.getName()).thenReturn(KS);

    listener.onKeyspaceDropped(keyspace);

    verify(tabletMap).removeByKeyspace(KS);
  }

  /**
   * An update evicts under the PREVIOUS name, since that is what the cached tablets are keyed by.
   */
  @Test
  public void should_evict_previous_keyspace_when_it_is_updated() {
    givenTabletMap();
    when(previousKeyspace.getName()).thenReturn(PREVIOUS_KS);

    listener.onKeyspaceUpdated(keyspace, previousKeyspace);

    verify(tabletMap).removeByKeyspace(PREVIOUS_KS);
    // Not never(KS): the current mock is unstubbed, so evicting under it would pass null and slip
    // through. This catches both that and an eviction under two names.
    verifyNoMoreInteractions(tabletMap);
  }

  @Test
  public void should_evict_table_when_it_is_dropped() {
    givenTabletMap();
    when(table.getName()).thenReturn(TABLE);

    listener.onTableDropped(table);

    verify(tabletMap).removeByTable(TABLE);
  }

  @Test
  public void should_evict_previous_table_when_it_is_updated() {
    givenTabletMap();
    when(previousTable.getName()).thenReturn(PREVIOUS_TABLE);

    listener.onTableUpdated(table, previousTable);

    verify(tabletMap).removeByTable(PREVIOUS_TABLE);
    verifyNoMoreInteractions(tabletMap);
  }

  @Test
  public void should_do_nothing_when_there_is_no_tablet_map() {
    // Server without tablets: every callback must be a no-op rather than an NPE
    when(metadata.getTabletMap()).thenReturn(Optional.empty());

    listener.onKeyspaceDropped(keyspace);
    listener.onKeyspaceUpdated(keyspace, previousKeyspace);
    listener.onTableDropped(table);
    listener.onTableUpdated(table, previousTable);

    verifyNoInteractions(tabletMap, keyspace, previousKeyspace, table, previousTable);
  }

  private void givenTabletMap() {
    when(metadata.getTabletMap()).thenReturn(Optional.of(tabletMap));
  }
}
