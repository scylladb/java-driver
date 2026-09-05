package com.datastax.oss.driver.internal.core.metadata;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.testng.Assert;

public class TabletsQueryTest {

  private static final TupleType REPLICA_TYPE = DataTypes.tupleOf(DataTypes.UUID, DataTypes.INT);

  @Test
  public void should_build_ordered_tablets() {
    UUID firstHostId = UUID.randomUUID();
    UUID secondHostId = UUID.randomUUID();
    Node firstNode = mock(Node.class);
    Node secondNode = mock(Node.class);
    Map<UUID, Node> nodes = ImmutableMap.of(firstHostId, firstNode, secondHostId, secondNode);
    TabletsQuery.TabletRows rows = new TabletsQuery.TabletRows();
    rows.tabletCount = 2;
    rows.tablets.add(
        new TabletsQuery.TabletRow(
            Long.MAX_VALUE, ImmutableList.of(replica(firstHostId, 1), replica(secondHostId, 2))));
    rows.tablets.add(
        new TabletsQuery.TabletRow(
            -1, ImmutableList.of(replica(firstHostId, 3), replica(secondHostId, 4))));

    List<Tablet> tablets = TabletsQuery.buildTablets(rows, nodes);

    Assert.assertEquals(tablets.size(), 2);
    Assert.assertEquals(tablets.get(0).getFirstToken(), Long.MIN_VALUE);
    Assert.assertEquals(tablets.get(0).getLastToken(), -1);
    Assert.assertEquals(tablets.get(0).getShardForNode(firstNode), 3);
    Assert.assertEquals(tablets.get(1).getFirstToken(), -1);
    Assert.assertEquals(tablets.get(1).getLastToken(), Long.MAX_VALUE);
    Assert.assertEquals(
        tablets.get(1).getReplicaNodesList(), ImmutableList.of(firstNode, secondNode));
  }

  @Test(expected = IllegalStateException.class)
  public void should_reject_incomplete_tablet_map() {
    UUID hostId = UUID.randomUUID();
    TabletsQuery.TabletRows rows = new TabletsQuery.TabletRows();
    rows.tabletCount = 2;
    rows.tablets.add(
        new TabletsQuery.TabletRow(Long.MAX_VALUE, ImmutableList.of(replica(hostId, 1))));

    TabletsQuery.buildTablets(rows, ImmutableMap.of(hostId, mock(Node.class)));
  }

  @Test(expected = IllegalStateException.class)
  public void should_reject_unknown_replica() {
    UUID hostId = UUID.randomUUID();
    TabletsQuery.TabletRows rows = new TabletsQuery.TabletRows();
    rows.tabletCount = 1;
    rows.tablets.add(
        new TabletsQuery.TabletRow(Long.MAX_VALUE, ImmutableList.of(replica(hostId, 1))));

    TabletsQuery.buildTablets(rows, ImmutableMap.of());
  }

  private static TupleValue replica(UUID hostId, int shard) {
    return REPLICA_TYPE.newValue().setUuid(0, hostId).setInt(1, shard);
  }
}
