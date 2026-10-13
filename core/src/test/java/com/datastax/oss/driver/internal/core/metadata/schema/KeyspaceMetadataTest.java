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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.metadata.schema.DseGraphKeyspaceMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Optional;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class KeyspaceMetadataTest {

  @Test
  public void should_escape_replication_datacenter_name() {
    assertDatacenterEscaped(KeyspaceMetadata.class);
  }

  @Test
  public void should_escape_legacy_dse_replication_datacenter_name() {
    assertDatacenterEscaped(DseGraphKeyspaceMetadata.class);
  }

  @Test
  public void should_comment_virtual_keyspace_description_with_empty_replication() {
    assertVirtualKeyspaceDescriptionCommented(KeyspaceMetadata.class);
  }

  @Test
  public void should_comment_legacy_virtual_keyspace_description_with_empty_replication() {
    assertVirtualKeyspaceDescriptionCommented(DseGraphKeyspaceMetadata.class);
  }

  private void assertVirtualKeyspaceDescriptionCommented(
      Class<? extends KeyspaceMetadata> metadataType) {
    KeyspaceMetadata keyspace = mock(metadataType, CALLS_REAL_METHODS);
    when(keyspace.getName()).thenReturn(CqlIdentifier.fromCql("ks"));
    when(keyspace.getReplication()).thenReturn(ImmutableMap.of());
    when(keyspace.isVirtual()).thenReturn(true);
    if (keyspace instanceof DseGraphKeyspaceMetadata) {
      when(((DseGraphKeyspaceMetadata) keyspace).getGraphEngine()).thenReturn(Optional.empty());
    }

    assertThat(keyspace.describe(false)).startsWith("/* VIRTUAL KEYSPACE").endsWith(" */");
    assertThat(keyspace.describe(true)).startsWith("/* VIRTUAL KEYSPACE").endsWith(" */");
  }

  private void assertDatacenterEscaped(Class<? extends KeyspaceMetadata> metadataType) {
    KeyspaceMetadata keyspace = mock(metadataType, CALLS_REAL_METHODS);
    when(keyspace.getName()).thenReturn(CqlIdentifier.fromCql("ks"));
    when(keyspace.getReplication())
        .thenReturn(ImmutableMap.of("class", "NetworkTopologyStrategy", "dc'west", "3"));
    when(keyspace.isDurableWrites()).thenReturn(true);
    if (keyspace instanceof DseGraphKeyspaceMetadata) {
      when(((DseGraphKeyspaceMetadata) keyspace).getGraphEngine()).thenReturn(Optional.of("Core"));
      assertThat(keyspace.describe(false)).contains("graph_engine ='Core'");
    }

    assertThat(keyspace.describe(false)).contains("'dc''west': '3'");
    assertThat(keyspace.describe(true)).contains("'dc''west': '3'");
  }
}
