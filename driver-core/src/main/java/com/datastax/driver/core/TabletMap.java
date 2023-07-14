package com.datastax.driver.core;

import com.google.common.annotations.Beta;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds currently known tablet mappings. Updated lazily through received custom payloads described
 * in Scylla's CQL protocol extensions (tablets-routing-v1).
 */
@Beta
public class TabletMap {
  private static final Logger logger = LoggerFactory.getLogger(TabletMap.class);

  private final ConcurrentMap<KeyspaceTableNamePair, NavigableSet<Tablet>> mapping;

  private final Cluster.Manager cluster;

  private TupleType payloadOuterTuple = null;
  private TupleType payloadInnerTuple = null;
  private TypeCodec<TupleValue> tabletPayloadCodec = null;

  public TabletMap(
      Cluster.Manager cluster, ConcurrentMap<KeyspaceTableNamePair, NavigableSet<Tablet>> mapping) {
    this.cluster = cluster;
    this.mapping = mapping;
  }

  public static TabletMap emptyMap(Cluster.Manager cluster) {
    return new TabletMap(cluster, new ConcurrentHashMap<>());
  }

  /**
   * Returns the mapping of tables to their tablets.
   *
   * @return the Map keyed by (keyspace,table) pairs with Set of tablets as value type.
   */
  public Map<KeyspaceTableNamePair, NavigableSet<Tablet>> getMapping() {
    return mapping;
  }

  /**
   * Finds hosts that have replicas for a given table and token combination
   *
   * @param keyspace the keyspace that table is in
   * @param table the table name
   * @param token the token to look for
   * @return Set of host UUIDS that do have a tablet for given token for a given table.
   */
  public Set<UUID> getReplicas(String keyspace, String table, long token) {
    TabletMap.KeyspaceTableNamePair key = new TabletMap.KeyspaceTableNamePair(keyspace, table);

    if (mapping == null) {
      logger.trace("This tablets map is null. Returning empty set.");
      return Collections.emptySet();
    }

    NavigableSet<Tablet> set = mapping.get(key);
    if (set == null) {
      logger.trace(
          "There is no tablets for {}.{} in this mapping. Returning empty set.", keyspace, table);
      return Collections.emptySet();
    }
    Tablet row = mapping.get(key).ceiling(Tablet.malformedTablet(token));
    if (row == null || row.firstToken >= token) {
      logger.trace(
          "Could not find tablet for {}.{} that owns token {}. Returning empty set.",
          keyspace,
          table,
          token);
      return Collections.emptySet();
    }

    HashSet<UUID> uuidSet = new HashSet<>();
    for (HostShardPair hostShardPair : row.replicas) {
      if (cluster.metadata.getHost(hostShardPair.getHost()) != null)
        uuidSet.add(hostShardPair.getHost());
    }
    return uuidSet;
  }

  /**
   * Processes tablets-routing-v1 custom payload. Expects the payload's underlying structure to
   * correspond to {@code TupleType(LongType, LongType, ListType(TupleType(UUIDType, Int32Type)))}.
   * Handles removing outdated tables that intersect with the one about to be added.
   *
   * @param keyspace the keyspace of the table
   * @param table the table name
   * @param payload the payload to be deserialized and processed
   */
  void processTabletsRoutingV1Payload(String keyspace, String table, ByteBuffer payload) {
    TupleValue tupleValue = getTabletPayloadCodec().deserialize(payload, cluster.protocolVersion());
    KeyspaceTableNamePair ktPair = new KeyspaceTableNamePair(keyspace, table);

    long firstToken = tupleValue.getLong(0);
    long lastToken = tupleValue.getLong(1);

    logger.trace(
        "Received tablets routing V1 payload: {}.{} range {}-{}",
        keyspace,
        table,
        firstToken,
        lastToken);

    Set<HostShardPair> replicas = new HashSet<>();
    List<TupleValue> list = tupleValue.getList(2, TupleValue.class);
    for (TupleValue tuple : list) {
      HostShardPair hostShardPair = new HostShardPair(tuple.getUUID(0), tuple.getInt(1));
      replicas.add(hostShardPair);
    }

    // Working on a copy to avoid concurrent modification of the same set
    NavigableSet<Tablet> existingTablets =
        new TreeSet<>(mapping.computeIfAbsent(ktPair, k -> new TreeSet<>()));

    // Single tablet token range is represented by (firstToken, lastToken] interval
    // We need to do two sweeps: remove overlapping tablets by looking at lastToken of existing
    // tablets
    // and then by looking at firstToken of existing tablets. Currently, the tablets are sorted
    // according
    // to their lastTokens.

    // First sweep: remove all tablets whose lastToken is inside this interval
    Iterator<Tablet> it =
        existingTablets.headSet(Tablet.malformedTablet(lastToken), true).descendingIterator();
    while (it.hasNext()) {
      Tablet tablet = it.next();
      if (tablet.lastToken <= firstToken) {
        break;
      }
      it.remove();
    }

    // Second sweep: remove all tablets whose firstToken is inside this tuple's (firstToken,
    // lastToken]
    // After the first sweep, this theoretically should remove at most 1 tablet
    it = existingTablets.tailSet(Tablet.malformedTablet(lastToken), true).iterator();
    while (it.hasNext()) {
      Tablet tablet = it.next();
      if (tablet.firstToken >= lastToken) {
        break;
      }
      it.remove();
    }

    // Add new (now) non-overlapping tablet
    existingTablets.add(new Tablet(keyspace, null, table, firstToken, lastToken, replicas));

    // Set the updated result in the main map
    mapping.put(ktPair, existingTablets);
  }

  public TupleType getPayloadOuterTuple() {
    if (this.payloadOuterTuple == null) {
      this.payloadOuterTuple =
          cluster.metadata.newTupleType(
              DataType.bigint(), DataType.bigint(), DataType.list(getPayloadInnerTuple()));
    }
    return payloadOuterTuple;
  }

  public TupleType getPayloadInnerTuple() {
    if (this.payloadInnerTuple == null) {
      this.payloadInnerTuple = cluster.metadata.newTupleType(DataType.uuid(), DataType.cint());
    }
    return payloadInnerTuple;
  }

  public TypeCodec<TupleValue> getTabletPayloadCodec() {
    if (tabletPayloadCodec == null) {
      this.tabletPayloadCodec =
          cluster.configuration.getCodecRegistry().codecFor(getPayloadOuterTuple());
    }
    return tabletPayloadCodec;
  }

  /**
   * Simple class to hold UUID of a host and a shard number. Class itself makes no checks or
   * guarantees about existence of a shard on corresponding host.
   */
  public static class HostShardPair {
    private final UUID host;
    private final int shard;

    public HostShardPair(UUID host, int shard) {
      this.host = host;
      this.shard = shard;
    }

    public UUID getHost() {
      return host;
    }

    public int getShard() {
      return shard;
    }

    @Override
    public String toString() {
      return "HostShardPair{" + "host=" + host + ", shard=" + shard + '}';
    }
  }

  /** Simple keyspace name and table name pair. */
  public static class KeyspaceTableNamePair {
    private final String keyspace;
    private final String tableName;

    public KeyspaceTableNamePair(String keyspace, String tableName) {
      this.keyspace = keyspace;
      this.tableName = tableName;
    }

    public String getKeyspace() {
      return keyspace;
    }

    public String getTableName() {
      return tableName;
    }

    @Override
    public String toString() {
      return "KeyspaceTableNamePair{"
          + "keyspace='"
          + keyspace
          + '\''
          + ", tableName='"
          + tableName
          + '\''
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      KeyspaceTableNamePair that = (KeyspaceTableNamePair) o;
      return keyspace.equals(that.keyspace) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyspace, tableName);
    }
  }

  /**
   * Represents a single tablet created from tablets-routing-v1 custom payload. Its {@code
   * compareTo} implementation intentionally relies solely on {@code lastToken} in order to allow
   * for quick lookup on sorted Collections based just on the token value.
   */
  public static class Tablet implements Comparable<Tablet> {
    private final String keyspaceName;
    private final UUID tableId; // currently null almost everywhere
    private final String tableName;
    private final long firstToken;
    private final long lastToken;
    private final Set<HostShardPair> replicas;

    private Tablet(
        String keyspaceName,
        UUID tableId,
        String tableName,
        long firstToken,
        long lastToken,
        Set<HostShardPair> replicas) {
      this.keyspaceName = keyspaceName;
      this.tableId = tableId;
      this.tableName = tableName;
      this.firstToken = firstToken;
      this.lastToken = lastToken;
      this.replicas = replicas;
    }

    /**
     * Creates a {@link Tablet} instance with given {@code lastToken}, identical {@code firstToken}
     * and unspecified other fields. Used for lookup of sorted collections of proper {@link Tablet}.
     *
     * @param lastToken
     * @return New {@link Tablet} object
     */
    public static Tablet malformedTablet(long lastToken) {
      return new Tablet(null, null, null, lastToken, lastToken, null);
    }

    public String getKeyspaceName() {
      return keyspaceName;
    }

    public UUID getTableId() {
      return tableId;
    }

    public String getTableName() {
      return tableName;
    }

    public long getFirstToken() {
      return firstToken;
    }

    public long getLastToken() {
      return lastToken;
    }

    public Set<HostShardPair> getReplicas() {
      return replicas;
    }

    @Override
    public String toString() {
      return "LazyTablet{"
          + "keyspaceName='"
          + keyspaceName
          + '\''
          + ", tableId="
          + tableId
          + ", tableName='"
          + tableName
          + '\''
          + ", firstToken="
          + firstToken
          + ", lastToken="
          + lastToken
          + ", replicas="
          + replicas
          + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Tablet that = (Tablet) o;
      return firstToken == that.firstToken
          && lastToken == that.lastToken
          && keyspaceName.equals(that.keyspaceName)
          && Objects.equals(tableId, that.tableId)
          && tableName.equals(that.tableName)
          && Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyspaceName, tableId, tableName, firstToken, lastToken, replicas);
    }

    @Override
    public int compareTo(Tablet tablet) {
      return Long.compare(this.lastToken, tablet.lastToken);
    }
  }
}
