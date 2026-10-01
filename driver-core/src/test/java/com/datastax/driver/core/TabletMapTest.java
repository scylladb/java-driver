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

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.driver.core.TabletMap.HostShardPair;
import com.datastax.driver.core.TabletMap.KeyspaceTableNamePair;
import com.datastax.driver.core.TabletMap.Tablet;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TabletMapTest {

  private static final UUID HOST1_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final UUID HOST2_ID = UUID.fromString("00000000-0000-0000-0000-000000000002");
  private static final UUID UNKNOWN_HOST_ID =
      UUID.fromString("00000000-0000-0000-0000-0000000000ff");

  private Cluster cluster;
  private Cluster.Manager manager;
  private Host host1;
  private Host host2;
  private TabletMap tabletMap;

  @BeforeClass(groups = "unit")
  public void setUpCluster() {
    // Never initialized: the manager keeps its real configuration, but metadata and the protocol
    // version only exist after init(), so they are supplied here.
    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    manager = cluster.manager;
    manager.connectionFactory = mock(Connection.Factory.class);
    manager.connectionFactory.protocolVersion = ProtocolVersion.V4;
    manager.metadata = new Metadata(manager);
    host1 = addHost(HOST1_ID, "127.0.0.1");
    host2 = addHost(HOST2_ID, "127.0.0.2");
  }

  @AfterClass(groups = "unit", alwaysRun = true)
  public void closeCluster() {
    if (cluster != null) {
      cluster.close();
    }
  }

  @BeforeMethod(groups = "unit")
  public void setUpMap() {
    tabletMap = TabletMap.emptyMap(manager);
  }

  @Test(groups = "unit")
  public void should_return_no_replicas_when_mapping_is_null() {
    TabletMap map = new TabletMap(manager, null);

    assertThat(map.getReplicas("ks", "tbl", 0L)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_return_no_replicas_for_unknown_table() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);

    assertThat(tabletMap.getReplicas("ks", "other", 5L)).isEmpty();
    assertThat(tabletMap.getReplicas("other", "tbl", 5L)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_resolve_replicas_for_token_in_half_open_range() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID, HOST2_ID);

    assertThat(tabletMap.getReplicas("ks", "tbl", 1L)).containsExactly(host1, host2);
    assertThat(tabletMap.getReplicas("ks", "tbl", 10L)).containsExactly(host1, host2);
    assertThat(tabletMap.getReplicas("ks", "tbl", 0L)).isEmpty();
    assertThat(tabletMap.getReplicas("ks", "tbl", 11L)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_return_no_replicas_when_token_falls_in_gap_between_tablets() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);
    addTablet("ks", "tbl", 20L, 30L, HOST2_ID);

    assertThat(tabletMap.getReplicas("ks", "tbl", 15L)).isEmpty();
    assertThat(tabletMap.getReplicas("ks", "tbl", 25L)).containsExactly(host2);
  }

  @Test(groups = "unit")
  public void should_return_no_replicas_when_any_replica_host_is_unknown() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID, UNKNOWN_HOST_ID);

    assertThat(tabletMap.getReplicas("ks", "tbl", 5L)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_deserialize_payload_into_tablet() {
    addTablet("ks", "tbl", -5L, 5L, HOST2_ID, HOST1_ID);

    List<Tablet> tablets = tablets("ks", "tbl");
    assertThat(tablets).hasSize(1);
    Tablet tablet = tablets.get(0);
    assertThat(tablet.getFirstToken()).isEqualTo(-5L);
    assertThat(tablet.getLastToken()).isEqualTo(5L);
    assertThat(tablet.getReplicas()).hasSize(2);
    assertThat(tablet.getReplicas().get(0).getHost()).isEqualTo(HOST2_ID);
    assertThat(tablet.getReplicas().get(0).getShard()).isEqualTo(2);
    assertThat(tablet.getReplicas().get(1).getHost()).isEqualTo(HOST1_ID);
    assertThat(tablet.getReplicas().get(1).getShard()).isEqualTo(1);
  }

  @Test(groups = "unit")
  public void should_keep_adjacent_tablets_when_adding() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);
    addTablet("ks", "tbl", 20L, 30L, HOST1_ID);
    addTablet("ks", "tbl", 10L, 20L, HOST2_ID);

    assertThat(tokenRanges("ks", "tbl")).containsExactly("(0,10]", "(10,20]", "(20,30]");
  }

  @Test(groups = "unit")
  public void should_keep_tablet_starting_at_new_upper_bound() {
    addTablet("ks", "tbl", 10L, 20L, HOST1_ID);
    addTablet("ks", "tbl", 0L, 10L, HOST2_ID);

    assertThat(tokenRanges("ks", "tbl")).containsExactly("(0,10]", "(10,20]");
    assertThat(tabletMap.getReplicas("ks", "tbl", 15L)).containsExactly(host1);
  }

  @Test(groups = "unit")
  public void should_remove_tablets_overlapping_new_tablet() {
    addTablet("ks", "tbl", -10L, 0L, HOST1_ID);
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);
    addTablet("ks", "tbl", 10L, 20L, HOST1_ID);
    addTablet("ks", "tbl", 20L, 30L, HOST1_ID);
    addTablet("ks", "tbl", 30L, 40L, HOST1_ID);
    addTablet("ks", "tbl", 5L, 25L, HOST2_ID);

    assertThat(tokenRanges("ks", "tbl")).containsExactly("(-10,0]", "(5,25]", "(30,40]");
    assertThat(tabletMap.getReplicas("ks", "tbl", 3L)).isEmpty();
    assertThat(tabletMap.getReplicas("ks", "tbl", 27L)).isEmpty();
    assertThat(tabletMap.getReplicas("ks", "tbl", 25L)).containsExactly(host2);
  }

  @Test(groups = "unit")
  public void should_remove_tablet_straddling_new_upper_bound() {
    addTablet("ks", "tbl", 0L, 20L, HOST1_ID);
    addTablet("ks", "tbl", 0L, 10L, HOST2_ID);

    assertThat(tokenRanges("ks", "tbl")).containsExactly("(0,10]");
    assertThat(tabletMap.getReplicas("ks", "tbl", 15L)).isEmpty();
  }

  @Test(groups = "unit")
  public void should_replace_tablet_with_same_range() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);
    addTablet("ks", "tbl", 0L, 10L, HOST2_ID);

    assertThat(tokenRanges("ks", "tbl")).containsExactly("(0,10]");
    assertThat(tabletMap.getReplicas("ks", "tbl", 5L)).containsExactly(host2);
  }

  @Test(groups = "unit")
  public void should_keep_tablets_of_other_tables_when_adding() {
    addTablet("ks", "tbl1", 0L, 10L, HOST1_ID);
    addTablet("ks", "tbl2", 0L, 10L, HOST2_ID);

    assertThat(tabletMap.getReplicas("ks", "tbl1", 5L)).containsExactly(host1);
    assertThat(tabletMap.getReplicas("ks", "tbl2", 5L)).containsExactly(host2);
  }

  @Test(groups = "unit")
  public void should_cache_payload_types() {
    TupleType inner = tabletMap.getPayloadInnerTuple();
    TupleType outer = tabletMap.getPayloadOuterTuple();

    assertThat(tabletMap.getPayloadInnerTuple()).isSameAs(inner);
    assertThat(tabletMap.getPayloadOuterTuple()).isSameAs(outer);
    assertThat(inner.getComponentTypes()).containsExactly(DataType.uuid(), DataType.cint());
    assertThat(outer.getComponentTypes())
        .containsExactly(DataType.bigint(), DataType.bigint(), DataType.list(inner));
  }

  @Test(groups = "unit")
  public void should_remove_table_mappings_by_pair_and_by_names() {
    addTablet("ks1", "events", 0L, 10L, HOST1_ID);
    addTablet("ks1", "users", 0L, 10L, HOST1_ID);
    addTablet("ks2", "events", 0L, 10L, HOST1_ID);

    tabletMap.removeTableMappings("ks1", "events");

    assertThat(tabletMap.getMapping().keySet())
        .containsOnly(
            new KeyspaceTableNamePair("ks1", "users"), new KeyspaceTableNamePair("ks2", "events"));

    tabletMap.removeTableMappings(new KeyspaceTableNamePair("ks2", "events"));

    assertThat(tabletMap.getMapping().keySet())
        .containsOnly(new KeyspaceTableNamePair("ks1", "users"));
  }

  @Test(groups = "unit")
  public void should_remove_only_tables_of_given_keyspace() {
    addTablet("ks1", "events", 0L, 10L, HOST1_ID);
    addTablet("ks1", "users", 0L, 10L, HOST1_ID);
    addTablet("ks2", "events", 0L, 10L, HOST1_ID);
    addTablet("ks2", "ks1", 0L, 10L, HOST1_ID);

    tabletMap.removeTableMappings("ks1");

    assertThat(tabletMap.getMapping().keySet())
        .containsOnly(
            new KeyspaceTableNamePair("ks2", "events"), new KeyspaceTableNamePair("ks2", "ks1"));
  }

  @Test(groups = "unit")
  public void should_implement_keyspace_table_name_pair_value_semantics() {
    KeyspaceTableNamePair pair = new KeyspaceTableNamePair("ks", "tbl");

    assertThat(pair.getKeyspace()).isEqualTo("ks");
    assertThat(pair.getTableName()).isEqualTo("tbl");
    assertThat(pair.equals(pair)).isTrue();
    assertThat(pair.equals(new KeyspaceTableNamePair("ks", "tbl"))).isTrue();
    assertThat(pair.hashCode()).isEqualTo(new KeyspaceTableNamePair("ks", "tbl").hashCode());
    assertThat(pair.equals(null)).isFalse();
    assertThat(pair.equals("ks.tbl")).isFalse();
    assertThat(pair.equals(new KeyspaceTableNamePair("other", "tbl"))).isFalse();
    assertThat(pair.equals(new KeyspaceTableNamePair("ks", "other"))).isFalse();
    assertThat(pair.toString()).isEqualTo("KeyspaceTableNamePair{keyspace='ks', tableName='tbl'}");
  }

  @Test(groups = "unit")
  public void should_describe_host_shard_pair() {
    HostShardPair pair = new HostShardPair(HOST1_ID, 3);

    assertThat(pair.getHost()).isEqualTo(HOST1_ID);
    assertThat(pair.getShard()).isEqualTo(3);
    assertThat(pair.toString()).isEqualTo("HostShardPair{host=" + HOST1_ID + ", shard=3}");
  }

  @Test(groups = "unit")
  public void should_create_malformed_tablet_for_lookup() {
    Tablet tablet = Tablet.malformedTablet(42L);

    assertThat(tablet.getFirstToken()).isEqualTo(42L);
    assertThat(tablet.getLastToken()).isEqualTo(42L);
    assertThat(tablet.getReplicas()).isNull();
    assertThat(tablet.toString()).isEqualTo("Tablet{firstToken=42, lastToken=42, replicas=null}");
  }

  @Test(groups = "unit")
  public void should_describe_tablet() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);

    assertThat(tablets("ks", "tbl").get(0).toString())
        .isEqualTo(
            "Tablet{firstToken=0, lastToken=10, replicas=[HostShardPair{host="
                + HOST1_ID
                + ", shard=1}]}");
  }

  @Test(groups = "unit")
  public void should_implement_tablet_value_semantics() {
    addTablet("ks", "a", 0L, 10L);
    addTablet("ks", "b", 0L, 10L);
    addTablet("ks", "c", 0L, 11L);
    addTablet("ks", "d", 0L, 10L, HOST1_ID);
    addTablet("ks", "e", 1L, 10L);
    Tablet tablet = tablets("ks", "a").get(0);
    Tablet sameAsTablet = tablets("ks", "b").get(0);

    assertThat(tablet.equals(tablet)).isTrue();
    assertThat(tablet.equals(sameAsTablet)).isTrue();
    assertThat(tablet.hashCode()).isEqualTo(sameAsTablet.hashCode());
    assertThat(tablet.equals(null)).isFalse();
    assertThat(tablet.equals("tablet")).isFalse();
    assertThat(tablet.equals(Tablet.malformedTablet(10L))).isFalse();
    assertThat(tablet.equals(tablets("ks", "c").get(0))).isFalse();
    assertThat(tablet.equals(tablets("ks", "d").get(0))).isFalse();
    assertThat(tablet.equals(tablets("ks", "e").get(0))).isFalse();
    assertThat(Tablet.malformedTablet(10L)).isEqualTo(Tablet.malformedTablet(10L));
  }

  @Test(groups = "unit")
  public void should_not_consider_tablets_with_same_replicas_equal() {
    addTablet("ks", "a", 0L, 10L, HOST1_ID);
    addTablet("ks", "b", 0L, 10L, HOST1_ID);

    // Pins current behaviour (#1176): HostShardPair lacks equals/hashCode, so Tablet equality is
    // identity-based whenever a tablet has replicas.
    assertThat(new HostShardPair(HOST1_ID, 1)).isNotEqualTo(new HostShardPair(HOST1_ID, 1));
    assertThat(tablets("ks", "a").get(0)).isNotEqualTo(tablets("ks", "b").get(0));
  }

  @Test(groups = "unit")
  public void should_order_tablets_by_last_token_only() {
    addTablet("ks", "tbl", 0L, 10L, HOST1_ID);
    Tablet tablet = tablets("ks", "tbl").get(0);

    assertThat(tablet.compareTo(Tablet.malformedTablet(10L))).isEqualTo(0);
    assertThat(tablet.compareTo(Tablet.malformedTablet(11L))).isLessThan(0);
    assertThat(tablet.compareTo(Tablet.malformedTablet(9L))).isGreaterThan(0);
  }

  private Host addHost(UUID hostId, String address) {
    Host host =
        new Host(
            new TranslatedAddressEndPoint(new InetSocketAddress(address, 9042)),
            manager.convictionPolicyFactory,
            manager);
    host.setHostId(hostId);
    manager.metadata.addIfAbsent(host);
    return host;
  }

  /** Feeds a tablets-routing-v1 payload; each replica's shard is the last digit of its host id. */
  private void addTablet(
      String keyspace, String table, long firstToken, long lastToken, UUID... replicas) {
    List<TupleValue> replicaTuples = new ArrayList<TupleValue>();
    for (UUID replica : replicas) {
      replicaTuples.add(
          tabletMap
              .getPayloadInnerTuple()
              .newValue(replica, (int) (replica.getLeastSignificantBits() & 0xf)));
    }
    TupleValue payload =
        tabletMap.getPayloadOuterTuple().newValue(firstToken, lastToken, replicaTuples);
    ByteBuffer bytes = tabletMap.getTabletPayloadCodec().serialize(payload, ProtocolVersion.V4);
    tabletMap.processTabletsRoutingV1Payload(keyspace, table, bytes);
  }

  private List<Tablet> tablets(String keyspace, String table) {
    return new ArrayList<Tablet>(
        tabletMap.getMapping().get(new KeyspaceTableNamePair(keyspace, table)).tablets);
  }

  private List<String> tokenRanges(String keyspace, String table) {
    List<String> ranges = new ArrayList<String>();
    for (Tablet tablet : tablets(keyspace, table)) {
      ranges.add("(" + tablet.getFirstToken() + "," + tablet.getLastToken() + "]");
    }
    return ranges;
  }
}
