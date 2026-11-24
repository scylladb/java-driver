package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.annotations.Beta;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

/** Holds all currently known tablet metadata. */
@Beta
public interface TabletMap {
  /**
   * Returns mapping from tables to the sets of their tablets.
   *
   * @return the Map keyed by (keyspace,table) pairs with Set of tablets as value type.
   */
  public ConcurrentMap<KeyspaceTableNamePair, ConcurrentSkipListSet<Tablet>> getMapping();

  /**
   * Adds a single tablet to the map. Handles removal of overlapping tablets.
   *
   * @param keyspace target keyspace
   * @param table target table
   * @param tablet tablet instance to add
   */
  public void addTablet(CqlIdentifier keyspace, CqlIdentifier table, Tablet tablet);

  /**
   * Returns {@link Tablet} instance
   *
   * @param keyspace tablet's keyspace
   * @param table tablet's table
   * @param token target token
   * @return {@link Tablet} responsible for provided token or {@code null} if no such tablet is
   *     present.
   */
  public Tablet getTablet(CqlIdentifier keyspace, CqlIdentifier table, long token);

  /**
   * Removes all tablets that contain given node in its replica list.
   *
   * @param node node serving as filter criterion
   */
  public void removeByNode(Node node);

  /**
   * Removes all mappings for a given keyspace.
   *
   * @param keyspace keyspace to remove
   */
  public void removeByKeyspace(CqlIdentifier keyspace);

  /**
   * Removes all mappings for tables with the given name, in every keyspace.
   *
   * @param table table to remove
   * @deprecated this matches on the table name alone, so it also evicts same-named tables in other
   *     keyspaces. Use {@link #removeByTable(CqlIdentifier, CqlIdentifier)} instead.
   */
  @Deprecated
  public void removeByTable(CqlIdentifier table);

  /**
   * Removes all mappings for a given table. Tables with the same name in other keyspaces are left
   * untouched.
   *
   * @param keyspace keyspace of the table to remove
   * @param table table to remove
   */
  public default void removeByTable(CqlIdentifier keyspace, CqlIdentifier table) {
    getMapping().remove(new KeyspaceTableNamePair(keyspace, table));
  }
}
