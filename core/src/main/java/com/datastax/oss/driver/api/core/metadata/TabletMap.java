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
   * Replaces all tablets of a single table, leaving other tables untouched.
   *
   * @param keyspace target keyspace
   * @param table target table
   * @param tablets the complete set of tablets for that table
   */
  public void replaceTablets(
      CqlIdentifier keyspace, CqlIdentifier table, Iterable<? extends Tablet> tablets);

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
   * Removes all mappings for a given table.
   *
   * @param table table to remove
   */
  public void removeByTable(CqlIdentifier table);
}
