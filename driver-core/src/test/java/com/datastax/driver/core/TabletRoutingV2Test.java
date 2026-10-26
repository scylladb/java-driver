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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TabletRoutingV2Test {
  private static final String V1 = "TABLETS_ROUTING_V1";
  private static final String V2 = "TABLETS_ROUTING_V2_EXPERIMENTAL";

  static ProtocolFeatureStore features(String... advertised) {
    Map<String, List<String>> supported = new HashMap<>();
    for (String option : advertised) {
      supported.put(option, Collections.singletonList(""));
    }
    return ProtocolFeatureStore.parseSupportedOptions(supported);
  }

  @Test(groups = "unit")
  public void should_negotiate_only_highest_supported_extension_per_connection() {
    ProtocolFeatureStore v2 = features(V1, V2);
    ProtocolFeatureStore v1 = features(V1);
    Map<String, String> options = new HashMap<>();
    v2.populateStartupOptions(ProtocolVersion.V4, options);
    assertEquals(options, Collections.singletonMap(V2, ""));
    options.clear();
    v1.populateStartupOptions(ProtocolVersion.V4, options);
    assertEquals(options, Collections.singletonMap(V1, ""));
    assertTrue(v2.getTabletInfo().isV2());
    assertFalse(v1.getTabletInfo().isV2());
    options.clear();
    v2.populateStartupOptions(ProtocolVersion.V3, options);
    assertTrue(options.isEmpty());
    assertFalse(v2.usesTabletRoutingV2(ProtocolVersion.V3));
    options.clear();
    features().populateStartupOptions(ProtocolVersion.V4, options);
    assertTrue(options.isEmpty());
  }

  @Test(groups = "unit")
  public void should_accept_server_supported_empty_list_and_prefer_v2() {
    ProtocolFeatureStore features =
        ProtocolFeatureStore.parseSupportedOptions(
            ImmutableMap.<String, List<String>>of(
                V1, Collections.<String>emptyList(), V2, Collections.<String>emptyList()));
    Map<String, String> options = new HashMap<>();
    features.populateStartupOptions(ProtocolVersion.V4, options);
    assertEquals(options, Collections.singletonMap(V2, ""));
    assertTrue(features.getTabletInfo().isV2());
    ProtocolFeatureStore v1 =
        ProtocolFeatureStore.parseSupportedOptions(
            ImmutableMap.<String, List<String>>of(V1, Collections.<String>emptyList()));
    options.clear();
    v1.populateStartupOptions(ProtocolVersion.V4, options);
    assertEquals(options, Collections.singletonMap(V1, ""));
  }

  @Test(groups = "unit")
  public void should_sample_cached_version_using_effective_keyspace_and_table() {
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    SessionManager session = new SessionManager(cluster);
    session.poolsState.keyspace = "SessionKs";
    BoundStatement bound = mock(BoundStatement.class);
    PreparedStatement prepared = mock(PreparedStatement.class);
    ColumnDefinitions variables = mock(ColumnDefinitions.class);
    when(bound.preparedStatement()).thenReturn(prepared);
    when(prepared.getVariables()).thenReturn(variables);
    when(variables.size()).thenReturn(1);
    when(variables.getTable(0)).thenReturn("TableName");
    ByteBuffer key = ByteBuffer.wrap(new byte[] {1});
    when(bound.getRoutingKey(ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE)).thenReturn(key);
    TabletMap.Tablet tablet = mock(TabletMap.Tablet.class);
    when(metadata.getTablet(Metadata.quote("SessionKs"), Metadata.quote("TableName"), null, key))
        .thenReturn(tablet);
    when(tablet.getVersion()).thenReturn(0L);
    assertEquals(
        session.tabletVersionBlock(bound, ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE) & 15,
        0);
    verify(metadata).getTablet(Metadata.quote("SessionKs"), Metadata.quote("TableName"), null, key);
    when(tablet.getVersion()).thenReturn(-1L);
    assertEquals(
        session.tabletVersionBlock(bound, ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE) & 15,
        15);
    when(bound.getKeyspace()).thenReturn("ExplicitKs");
    session.tabletVersionBlock(bound, ProtocolVersion.V4, CodecRegistry.DEFAULT_INSTANCE);
    verify(metadata)
        .getTablet(Metadata.quote("ExplicitKs"), Metadata.quote("TableName"), null, key);
  }

  @DataProvider
  public Object[][] executeVersions() {
    return new Object[][] {{ProtocolVersion.V4}, {ProtocolVersion.V5}, {ProtocolVersion.V6}};
  }

  @Test(groups = "unit", dataProvider = "executeVersions")
  public void should_append_one_byte_only_on_v2_connections_and_preserve_it_on_retries(
      ProtocolVersion protocol) {
    Requests.Execute request =
        new Requests.Execute(
            MD5Digest.wrap(new byte[] {1, 2}),
            null,
            Requests.QueryProtocolOptions.DEFAULT,
            false,
            (byte) 0xf9);
    ProtocolFeatureStore v1 = features(V1);
    ProtocolFeatureStore v2 = features(V1, V2);
    ByteBuf baseline = Unpooled.buffer();
    try {
      Requests.Execute.coder.encode(request, baseline, protocol, v1);
      for (Requests.Execute retry :
          new Requests.Execute[] {
            request,
            (Requests.Execute) request.copy(),
            (Requests.Execute) request.copy(ConsistencyLevel.QUORUM)
          }) {
        ByteBuf encoded = Unpooled.buffer();
        try {
          Requests.Execute.coder.encode(retry, encoded, protocol, v2);
          assertEquals(
              encoded.writerIndex(), Requests.Execute.coder.encodedSize(retry, protocol, v2));
          assertEquals(encoded.writerIndex(), baseline.writerIndex() + 1);
          assertEquals(encoded.getUnsignedByte(encoded.writerIndex() - 1), (short) 0xf9);
          // The consistency-changing retry has different options but the same trailing block.
          if (retry == request) {
            assertEquals(encoded.slice(0, baseline.writerIndex()), baseline);
          }
        } finally {
          encoded.release();
        }
      }
      ByteBuf plain = Unpooled.buffer();
      try {
        Requests.Execute.coder.encode(request, plain, protocol, ProtocolFeatureStore.EMPTY);
        assertEquals(plain, baseline);
        assertEquals(
            plain.readableBytes(),
            Requests.Execute.coder.encodedSize(request, protocol, ProtocolFeatureStore.EMPTY));
      } finally {
        plain.release();
      }
    } finally {
      baseline.release();
    }
  }

  @Test(groups = "unit")
  public void should_sample_all_version_nibbles_including_zero_and_sign_bit() {
    long version = 0xfedcba9876543210L;
    boolean[] seen = new boolean[16];
    for (int i = 0; i < 4096; i++) {
      int sample = TabletMap.sampleVersionBlock(version) & 255;
      int block = sample >>> 4;
      seen[block] = true;
      assertEquals(sample & 15, (int) ((version >>> (block * 4)) & 15));
      assertEquals(TabletMap.sampleVersionBlock(0) & 15, 0);
    }
    for (boolean blockSeen : seen) {
      assertTrue(blockSeen);
    }
  }

  @Test(groups = "unit")
  public void should_use_entire_byte_for_unknown_versions() {
    boolean[] seen = new boolean[256];
    for (int i = 0; i < 65536; i++) {
      seen[TabletMap.randomVersionBlock() & 255] = true;
    }
    for (boolean valueSeen : seen) {
      assertTrue(valueSeen);
    }
  }

  @Test(groups = "unit")
  public void should_cache_only_connection_negotiated_payload_in_effective_keyspace() {
    Cluster cluster = mock(Cluster.class);
    Metadata metadata = mock(Metadata.class);
    TabletMap tablets = mock(TabletMap.class);
    SessionManager session = mock(SessionManager.class);
    when(session.getCluster()).thenReturn(cluster);
    when(session.getLoggedKeyspace()).thenReturn("session_ks");
    when(cluster.getMetadata()).thenReturn(metadata);
    when(metadata.getTabletMap()).thenReturn(tablets);
    BoundStatement bound = mock(BoundStatement.class);
    PreparedStatement prepared = mock(PreparedStatement.class);
    ColumnDefinitions variables = mock(ColumnDefinitions.class);
    when(bound.preparedStatement()).thenReturn(prepared);
    when(prepared.getVariables()).thenReturn(variables);
    when(variables.size()).thenReturn(1);
    when(variables.getTable(0)).thenReturn("tbl");
    ByteBuffer v1Payload = ByteBuffer.wrap(new byte[] {1});
    ByteBuffer v2Payload = ByteBuffer.wrap(new byte[] {2});
    Responses.Result.Void response = new Responses.Result.Void();
    response.setCustomPayload(
        ImmutableMap.of(
            TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY, v1Payload,
            TabletInfo.TABLETS_ROUTING_V2_CUSTOM_PAYLOAD_KEY, v2Payload));
    Connection connection = mock(Connection.class);
    DefaultResultSetFuture future = new DefaultResultSetFuture(session, ProtocolVersion.V4, null);
    when(connection.getProtocolFeatureStore()).thenReturn(features(V1, V2));
    future.cacheTabletRouting(connection, response, bound);
    verify(tablets).processTabletsRoutingV2Payload("session_ks", "tbl", v2Payload);
    verify(tablets, never())
        .processTabletsRoutingV1Payload(
            any(String.class), any(String.class), any(ByteBuffer.class));
    when(bound.getKeyspace()).thenReturn("explicit_ks");
    when(connection.getProtocolFeatureStore()).thenReturn(features(V1));
    future.cacheTabletRouting(connection, response, bound);
    verify(tablets).processTabletsRoutingV1Payload("explicit_ks", "tbl", v1Payload);
    verify(tablets, never())
        .processTabletsRoutingV2Payload(
            eq("explicit_ks"), any(String.class), any(ByteBuffer.class));
    when(bound.getKeyspace()).thenReturn("unnegotiated_ks");
    when(connection.getProtocolFeatureStore()).thenReturn(features());
    future.cacheTabletRouting(connection, response, bound);
    verify(tablets, never())
        .processTabletsRoutingV1Payload(
            eq("unnegotiated_ks"), any(String.class), any(ByteBuffer.class));
    verify(tablets, never())
        .processTabletsRoutingV2Payload(
            eq("unnegotiated_ks"), any(String.class), any(ByteBuffer.class));
    when(variables.size()).thenReturn(0);
    when(connection.getProtocolFeatureStore()).thenReturn(features(V2));
    future.cacheTabletRouting(connection, response, bound);
    verify(tablets, never())
        .processTabletsRoutingV2Payload(
            eq("unnegotiated_ks"), any(String.class), any(ByteBuffer.class));
  }

  @Test(groups = "unit")
  public void should_read_optional_consistency_and_clear_routing_on_mode_change() {
    KeyspaceMetadata eventual =
        new KeyspaceMetadata(
            "ks", true, ImmutableMap.of("class", "SimpleStrategy", "replication_factor", "1"));
    KeyspaceMetadata global =
        new KeyspaceMetadata(
            "ks", true, ImmutableMap.of("class", "SimpleStrategy", "replication_factor", "1"));
    Row row = mock(Row.class);
    ColumnDefinitions definitions = mock(ColumnDefinitions.class);
    when(row.getColumnDefinitions()).thenReturn(definitions);
    when(definitions.contains("consistency")).thenReturn(true);
    when(row.getString("consistency")).thenReturn("global");
    global.setScyllaOptions(row);
    assertTrue(global.isGloballyConsistent());
    assertTrue(global.asCQLQuery().contains("AND consistency = 'global'"));
    assertFalse(eventual.isGloballyConsistent());
    assertFalse(eventual.asCQLQuery().contains("consistency"));
    assertFalse(global.equals(eventual));
    Metadata metadata = mock(Metadata.class);
    TabletMap tablets = mock(TabletMap.class);
    when(metadata.getTabletMap()).thenReturn(tablets);
    Map<String, KeyspaceMetadata> old = new HashMap<>();
    old.put("ks", eventual);
    SchemaParser.forVersion(VersionNumber.parse("3.0.0"))
        .updateKeyspaces(metadata, old, Collections.singletonMap("ks", global), "ks");
    verify(tablets).removeTableMappings("ks");
    assertEquals(old.get("ks"), global);
    when(row.getString("consistency")).thenReturn("eventual");
    global.setScyllaOptions(row);
    assertFalse(global.isGloballyConsistent());
    when(definitions.contains("consistency")).thenReturn(false);
    global.setScyllaOptions(row);
    assertFalse(global.isGloballyConsistent());
  }
}
