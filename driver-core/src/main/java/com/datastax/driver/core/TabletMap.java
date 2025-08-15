package com.datastax.driver.core;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds currently known tablet mappings. Updated lazily through received custom payloads described
 * in Scylla's CQL protocol extensions (tablets-routing-v1).
 */
@Beta
public class TabletMap {
  private static final Logger logger = LoggerFactory.getLogger(TabletMap.class);
  private static final ImmutableList<Host> EMPTY_LIST = ImmutableList.of();

  // There are no additional locking mechanisms for the mapping field itself, however each TabletSet
  // inside has its own ReadWriteLock that should be used when dealing with its internals.
  private final ConcurrentMap<KeyspaceTableNamePair, TabletSet> mapping;

  private final Cluster.Manager cluster;

  private TupleType payloadOuterTuple = null;
  private TupleType payloadInnerTuple = null;
  private TypeCodec<TupleValue> tabletPayloadCodec = null;

  public TabletMap(
      Cluster.Manager cluster, ConcurrentMap<KeyspaceTableNamePair, TabletSet> mapping) {
    this.cluster = cluster;
    this.mapping = mapping;
  }

  public static TabletMap emptyMap(Cluster.Manager cluster) {
    return new TabletMap(cluster, new ConcurrentHashMap<>());
  }

  /**
   * Returns the mapping of tables to their tablets.
   *
   * @return the Map keyed by (keyspace,table) pairs with {@link TabletSet} as value type.
   */
  public Map<KeyspaceTableNamePair, TabletSet> getMapping() {
    return mapping;
  }

  /**
   * Finds hosts that have replicas for a given table and token combination. Meant for use in query
   * planning. Can return empty collection if internal replica list information is determined not up
   * to date.
   *
   * @param keyspace the keyspace that table is in
   * @param table the table name
   * @param token the token to look for
   * @return List(immutable) of Host instances that do have a tablet for the given token and table
   *     combination.
   */
  public List<Host> getReplicas(String keyspace, String table, long token) {
    TabletMap.KeyspaceTableNamePair key = new TabletMap.KeyspaceTableNamePair(keyspace, table);

    if (mapping == null) {
      logger.trace("This tablets map is null. Returning empty set.");
      return EMPTY_LIST;
    }

    TabletSet tabletSet = mapping.get(key);
    if (tabletSet == null) {
      logger.trace(
          "There is no tablets for {}.{} in this mapping. Returning empty set.", keyspace, table);
      return EMPTY_LIST;
    }
    Lock readLock = tabletSet.lock.readLock();
    try {
      readLock.lock();
      Tablet row = mapping.get(key).tablets.ceiling(Tablet.malformedTablet(token));
      if (row == null || row.firstToken >= token) {
        logger.trace(
            "Could not find tablet for {}.{} that owns token {}. Returning empty set.",
            keyspace,
            table,
            token);
        return EMPTY_LIST;
      }

      ImmutableList.Builder<Host> replicas = new ImmutableList.Builder();
      for (HostShardPair hostShardPair : row.replicas) {
        Host replica = cluster.metadata.getHost(hostShardPair.getHost());
        if (replica == null) {
          // We've encountered a stale host. Return an empty set to
          // misroute the request. If misrouted then response will
          // contain up to date tablet information that will be processed.
          return EMPTY_LIST;
        } else {
          replicas.add(replica);
        }
      }
      return replicas.build();
    } finally {
      readLock.unlock();
    }
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

    List<HostShardPair> replicas = new ArrayList<>();
    List<TupleValue> list = tupleValue.getList(2, TupleValue.class);
    for (TupleValue tuple : list) {
      HostShardPair hostShardPair = new HostShardPair(tuple.getUUID(0), tuple.getInt(1));
      replicas.add(hostShardPair);
    }
    Tablet newTablet = new Tablet(firstToken, lastToken, replicas);

    TabletSet tabletSet = mapping.computeIfAbsent(ktPair, k -> new TabletSet());
    Lock writeLock = tabletSet.lock.writeLock();
    try {
      writeLock.lock();
      NavigableSet<Tablet> currentTablets = tabletSet.tablets;
      // Single tablet token range is represented by (firstToken, lastToken] interval
      // We need to do two sweeps: remove overlapping tablets by looking at lastToken of existing
      // tablets
      // and then by looking at firstToken of existing tablets. Currently, the tablets are sorted
      // according
      // to their lastTokens.

      // First sweep: remove all tablets whose lastToken is inside this interval
      Iterator<Tablet> it = currentTablets.headSet(newTablet, true).descendingIterator();
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
      it = currentTablets.tailSet(newTablet, true).iterator();
      while (it.hasNext()) {
        Tablet tablet = it.next();
        if (tablet.firstToken >= lastToken) {
          break;
        }
        it.remove();
      }

      // Add new (now) non-overlapping tablet
      currentTablets.add(newTablet);
    } finally {
      writeLock.unlock();
    }
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

  public void removeTableMappings(KeyspaceTableNamePair key) {
    this.mapping.remove(key);
  }

  public void removeTableMappings(String keyspace, String table) {
    removeTableMappings(new KeyspaceTableNamePair(keyspace, table));
  }

  public void removeTableMappings(String keyspace) {
    Iterator<TabletMap.KeyspaceTableNamePair> it = getMapping().keySet().iterator();
    while (it.hasNext()) {
      KeyspaceTableNamePair key = it.next();
      if (key.getKeyspace().equals(keyspace)) {
        it.remove();
      }
    }
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
   * Set of tablets bundled with ReadWriteLock to allow concurrent modification for different sets.
   */
  public static class TabletSet {
    final NavigableSet<Tablet> tablets;
    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public TabletSet() {
      this.tablets = new TreeSet<>();
    }
  }

  /**
   * Represents a single tablet created from tablets-routing-v1 custom payload. Its {@code
   * compareTo} implementation intentionally relies solely on {@code lastToken} in order to allow
   * for quick lookup on sorted Collections based just on the token value.
   */
  public static class Tablet implements Comparable<Tablet> {
    private final long firstToken;
    private final long lastToken;
    private final List<HostShardPair> replicas;

    private Tablet(long firstToken, long lastToken, List<HostShardPair> replicas) {
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
      return new Tablet(lastToken, lastToken, null);
    }

    public long getFirstToken() {
      return firstToken;
    }

    public long getLastToken() {
      return lastToken;
    }

    public List<HostShardPair> getReplicas() {
      return replicas;
    }

    @Override
    public String toString() {
      return "LazyTablet{"
          + "firstToken="
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
          && Objects.equals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstToken, lastToken, replicas);
    }

    @Override
    public int compareTo(Tablet tablet) {
      return Long.compare(this.lastToken, tablet.lastToken);
    }
  }
}
