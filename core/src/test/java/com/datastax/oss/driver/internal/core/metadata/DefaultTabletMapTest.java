package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.KeyspaceTableNamePair;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.testng.Assert;

public class DefaultTabletMapTest {

  private static final CqlIdentifier KS = CqlIdentifier.fromCql("ks");
  private static final CqlIdentifier KS2 = CqlIdentifier.fromCql("ks2");
  private static final CqlIdentifier TABLE = CqlIdentifier.fromCql("tab");
  private static final CqlIdentifier TABLE2 = CqlIdentifier.fromCql("tab2");

  @Test
  public void should_remove_overlapping_tablets() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 =
        new DefaultTabletMap.DefaultTablet(0, 1, Collections.emptyList(), Collections.emptyMap());
    Tablet tablet2 =
        new DefaultTabletMap.DefaultTablet(1, 2, Collections.emptyList(), Collections.emptyMap());
    Tablet tablet3 =
        new DefaultTabletMap.DefaultTablet(2, 3, Collections.emptyList(), Collections.emptyMap());
    Tablet tablet4 =
        new DefaultTabletMap.DefaultTablet(
            -100, 100, Collections.emptyList(), Collections.emptyMap());

    Tablet tablet5 =
        new DefaultTabletMap.DefaultTablet(
            -10, 10, Collections.emptyList(), Collections.emptyMap());
    Tablet tablet6 =
        new DefaultTabletMap.DefaultTablet(9, 20, Collections.emptyList(), Collections.emptyMap());

    KeyspaceTableNamePair key1 =
        new KeyspaceTableNamePair(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"));

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 1);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet2);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 2);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet3);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 3);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet4);
    Assert.assertEquals(tabletMap.getMapping().size(), 1);
    Assert.assertEquals(tabletMap.getMapping().get(key1).size(), 1);

    KeyspaceTableNamePair key2 =
        new KeyspaceTableNamePair(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"));

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"), tablet5);
    Assert.assertEquals(tabletMap.getMapping().size(), 2);
    Assert.assertEquals(tabletMap.getMapping().get(key2).size(), 1);

    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"), tablet6);
    Assert.assertEquals(tabletMap.getMapping().size(), 2);
    Assert.assertEquals(tabletMap.getMapping().get(key2).size(), 1);
    Assert.assertTrue(tabletMap.getMapping().get(key2).contains(tablet6));
    Assert.assertEquals(
        tablet6,
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab2"), 10L));
  }

  @Test
  public void tablet_range_should_not_include_first_token() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 =
        new DefaultTabletMap.DefaultTablet(
            -123, 123, Collections.emptyList(), Collections.emptyMap());
    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Tablet result =
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), -123);
    Assert.assertEquals(result, null);
  }

  @Test
  public void tablet_range_should_include_last_token() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 =
        new DefaultTabletMap.DefaultTablet(
            -123, 456, Collections.emptyList(), Collections.emptyMap());
    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Tablet result =
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), 456);
    Assert.assertEquals(result, tablet1);
  }

  @Test
  public void should_return_correct_shard() {
    Node node1 = mock(DefaultNode.class);
    Node node2 = mock(DefaultNode.class);
    List<Node> replicaNodes = ImmutableList.of(node1, node2);
    Map<Node, Integer> replicaShards = new HashMap<>();
    replicaShards.put(node1, 1);
    replicaShards.put(node2, 2);
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet tablet1 = new DefaultTabletMap.DefaultTablet(-123, 456, replicaNodes, replicaShards);
    tabletMap.addTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), tablet1);
    Tablet result =
        tabletMap.getTablet(CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("tab"), 456);
    Assert.assertEquals(result.getShardForNode(node1), 1);
    Assert.assertEquals(result.getShardForNode(node2), 2);
  }

  // --- Removal --------------------------------------------------------------------------------

  @Test
  public void should_remove_tablets_by_node() {
    Node node1 = mock(DefaultNode.class);
    Node node2 = mock(DefaultNode.class);
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    tabletMap.addTablet(KS, TABLE, tablet(0, 10, node1));
    tabletMap.addTablet(KS, TABLE, tablet(10, 20, node2));
    tabletMap.addTablet(KS, TABLE2, tablet(0, 10, node1));

    tabletMap.removeByNode(node1);

    assertThat(tabletMap.getMapping().get(key(KS, TABLE))).hasSize(1);
    assertThat(tabletMap.getMapping().get(key(KS, TABLE))).allMatch(t -> !contains(t, node1));
    // removeByNode empties the set but leaves the key, unlike removeByKeyspace/removeByTable below
    assertThat(tabletMap.getMapping().get(key(KS, TABLE2))).isEmpty();
  }

  @Test
  public void should_remove_tablets_by_keyspace() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    tabletMap.addTablet(KS, TABLE, tablet(0, 10));
    tabletMap.addTablet(KS, TABLE2, tablet(0, 10));
    tabletMap.addTablet(KS2, TABLE, tablet(0, 10));

    tabletMap.removeByKeyspace(KS);

    assertThat(tabletMap.getMapping()).containsOnlyKeys(key(KS2, TABLE));
  }

  /**
   * removeByTable matches on the table name alone, so a table dropped in one keyspace also evicts
   * same-named tables in every other keyspace. The cost is an unnecessary refetch, not incorrect
   * routing; filed as scylladb/java-driver#1116 and pinned here until it is fixed.
   */
  @Test
  public void should_remove_tablets_by_table_name_across_keyspaces() {
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    tabletMap.addTablet(KS, TABLE, tablet(0, 10));
    tabletMap.addTablet(KS2, TABLE, tablet(0, 10));
    tabletMap.addTablet(KS, TABLE2, tablet(0, 10));

    tabletMap.removeByTable(TABLE);

    assertThat(tabletMap.getMapping()).containsOnlyKeys(key(KS, TABLE2));
  }

  // --- Overlap eviction, second sweep ----------------------------------------------------------

  @Test
  public void should_evict_tablet_that_starts_before_the_new_ones_last_token() {
    // The existing tablet survives the first sweep (its lastToken is beyond the new one) but
    // overlaps on its leading edge, so the second sweep must drop it.
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet wide = tablet(0, 100);
    tabletMap.addTablet(KS, TABLE, wide);

    Tablet overlapping = tablet(-50, 10);
    tabletMap.addTablet(KS, TABLE, overlapping);

    assertThat(tabletMap.getMapping().get(key(KS, TABLE))).containsExactly(overlapping);
  }

  @Test
  public void should_keep_tablet_that_starts_after_the_new_ones_last_token() {
    // Mirror image: the second sweep breaks out instead of removing.
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet later = tablet(20, 100);
    tabletMap.addTablet(KS, TABLE, later);

    Tablet earlier = tablet(-50, 10);
    tabletMap.addTablet(KS, TABLE, earlier);

    assertThat(tabletMap.getMapping().get(key(KS, TABLE))).containsExactly(earlier, later);
  }

  @Test
  public void should_keep_tablet_that_starts_exactly_at_the_new_ones_last_token() {
    // The boundary itself. Ranges are (firstToken, lastToken], so a tablet starting where the new
    // one ends is adjacent, not overlapping, and the sweep's >= must not become >.
    TabletMap tabletMap = DefaultTabletMap.emptyMap();
    Tablet adjacent = tablet(10, 100);
    tabletMap.addTablet(KS, TABLE, adjacent);

    Tablet earlier = tablet(-50, 10);
    tabletMap.addTablet(KS, TABLE, earlier);

    assertThat(tabletMap.getMapping().get(key(KS, TABLE))).containsExactly(earlier, adjacent);
  }

  // --- DefaultTablet value semantics -----------------------------------------------------------

  @Test
  public void should_compare_tablets_by_value() {
    Tablet tablet = tablet(0, 10);

    assertThat(tablet).isEqualTo(tablet);
    assertThat(tablet).isEqualTo(tablet(0, 10));
    assertThat(tablet).isNotEqualTo(tablet(0, 11));
    assertThat(tablet).isNotEqualTo(tablet(1, 10));
    assertThat(tablet).isNotEqualTo("not a tablet");
    assertThat(tablet).isNotEqualTo(null);
    assertThat(tablet.hashCode()).isEqualTo(tablet(0, 10).hashCode());
  }

  /**
   * compareTo orders on lastToken alone, so the sorted sets never consult equals and removeByNode
   * filters by replica list rather than identity -- equality is for the callers reading
   * getMapping(). Pinned because the replica list and shard drive routing, so two tablets over the
   * same range are not interchangeable.
   */
  @Test
  public void should_distinguish_tablets_with_same_range_but_different_replicas() {
    Node node1 = mock(DefaultNode.class);
    Node node2 = mock(DefaultNode.class);
    Map<Node, Integer> sharedShards = ImmutableMap.of(node1, 0, node2, 0);

    // Different replica list: order drives routing, so it is part of identity.
    assertThat(tablet(0, 10, sharedShards, node1, node2))
        .isNotEqualTo(tablet(0, 10, sharedShards, node2, node1));

    // Same replica list, different shard.
    assertThat(tablet(0, 10, ImmutableMap.of(node1, 0), node1))
        .isNotEqualTo(tablet(0, 10, ImmutableMap.of(node1, 1), node1));
  }

  @Test
  public void should_print_token_range_in_to_string() {
    assertThat(tablet(0, 10).toString())
        .contains("firstToken=0")
        .contains("lastToken=10")
        .contains("replicaNodes=")
        .contains("replicaShards=");
  }

  // --- Payload parsing -------------------------------------------------------------------------

  @Test
  public void should_skip_replica_with_unknown_host_id() {
    DefaultNode known = newNode(1);
    TupleValue payload =
        payload(0, 10, replica(UUID.randomUUID(), 3), replica(known.getHostId(), 7));

    Tablet tablet =
        DefaultTabletMap.DefaultTablet.parseTabletPayloadV1(
            payload, ImmutableMap.of(known.getHostId(), known));

    assertThat(tablet.getReplicaNodesList()).containsExactly(known);
    assertThat(tablet.getShardForNode(known)).isEqualTo(7);
  }

  @Test
  public void should_list_a_replica_named_twice_only_once() {
    DefaultNode node = newNode(1);
    TupleValue payload = payload(0, 10, replica(node.getHostId(), 1), replica(node.getHostId(), 2));

    Tablet tablet =
        DefaultTabletMap.DefaultTablet.parseTabletPayloadV1(
            payload, ImmutableMap.of(node.getHostId(), node));

    assertThat(tablet.getReplicaNodesList()).containsExactly(node);
    // A node can only be recorded on one shard, so the last entry in the payload wins.
    assertThat(tablet.getShardForNode(node)).isEqualTo(2);
  }

  private static DefaultNode newNode(int lastIpByte) {
    InternalDriverContext context = mock(InternalDriverContext.class);
    when(context.getMetricsFactory()).thenReturn(mock(MetricsFactory.class));
    return TestNodeFactory.newNode(lastIpByte, context);
  }

  /** One entry of the replica list in a tablets-routing-v1 payload. */
  private static TupleValue replica(UUID hostId, int shard) {
    TupleValue tuple = mock(TupleValue.class);
    when(tuple.getUuid(0)).thenReturn(hostId);
    when(tuple.getInt(1)).thenReturn(shard);
    return tuple;
  }

  private static TupleValue payload(long firstToken, long lastToken, TupleValue... replicas) {
    TupleValue payload = mock(TupleValue.class);
    when(payload.getLong(0)).thenReturn(firstToken);
    when(payload.getLong(1)).thenReturn(lastToken);
    when(payload.getList(2, TupleValue.class)).thenReturn(ImmutableList.copyOf(replicas));
    return payload;
  }

  private static boolean contains(Tablet tablet, Node node) {
    return tablet.getReplicaNodesList().contains(node);
  }

  private static KeyspaceTableNamePair key(CqlIdentifier keyspace, CqlIdentifier table) {
    return new KeyspaceTableNamePair(keyspace, table);
  }

  /** Variant of {@link #tablet} that pins the shard of each replica explicitly. */
  private static Tablet tablet(
      long firstToken, long lastToken, Map<Node, Integer> replicaShards, Node... replicas) {
    return new DefaultTabletMap.DefaultTablet(
        firstToken, lastToken, ImmutableList.copyOf(replicas), replicaShards);
  }

  private static Tablet tablet(long firstToken, long lastToken, Node... replicas) {
    Map<Node, Integer> replicaShards = new HashMap<>();
    int shard = 0;
    for (Node replica : replicas) {
      replicaShards.put(replica, shard++);
    }
    return new DefaultTabletMap.DefaultTablet(
        firstToken, lastToken, ImmutableList.copyOf(replicas), replicaShards);
  }
}
