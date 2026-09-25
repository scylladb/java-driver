/*
 * Copyright DataStax, Inc.
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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Encapsulates the column-projection state and logic for {@link ControlConnection}'s system table
 * queries (DRIVER-368).
 *
 * <p>On the first query to each system table ({@code system.local}, {@code system.peers}, {@code
 * system.peers_v2}) the driver sends {@code SELECT *} to discover which columns the server exposes.
 * The result is intersected with the appropriate {@code *_COLUMNS_OF_INTEREST} set and cached here.
 * Subsequent queries project only the cached columns, reducing bytes on the wire and
 * deserialization work.
 *
 * <p>All cache fields are {@code volatile} because they are written from the control-connection I/O
 * thread and read from other threads.
 */
class SystemColumnProjection {

  /**
   * Identifies one of the three system tables the driver queries. Used as a parameter to {@link
   * #query}, {@link #populate}, and {@link #hook} so callers dispatch by value rather than by
   * choosing among three separate methods.
   */
  enum SystemTable {
    LOCAL("system.local", "key='local'"),
    PEERS("system.peers", null),
    PEERS_V2("system.peers_v2", null);

    /** Fully-qualified table name used in query strings. */
    final String tableName;

    /**
     * WHERE clause appended to full-scan queries, or {@code null} for tables that are always
     * scanned in full.
     */
    final String defaultWhereClause;

    SystemTable(String tableName, String defaultWhereClause) {
      this.tableName = tableName;
      this.defaultWhereClause = defaultWhereClause;
    }
  }

  // IMPORTANT: Every column read from system.local rows — in updateInfo(),
  // refreshNodeListAndTokenMap(), isValidPeer(), and DefaultEndPointFactory — MUST be listed here.
  // If a new column read is added anywhere that consumes a system table row, add it to the
  // appropriate set below, otherwise it will be silently excluded from projected queries.
  @VisibleForTesting
  static final ImmutableSet<String> LOCAL_COLUMNS_OF_INTEREST =
      ImmutableSet.of(
          "cluster_name",
          "partitioner",
          "data_center",
          "rack",
          "release_version",
          "native_address",
          "native_port",
          "native_transport_address",
          "native_transport_port",
          "native_transport_port_ssl",
          "rpc_address",
          "broadcast_address",
          "broadcast_port",
          "listen_address",
          "listen_port",
          "tokens",
          "host_id",
          "schema_version",
          "workload",
          "graph",
          "dse_version");

  // IMPORTANT: see LOCAL_COLUMNS_OF_INTEREST note above.
  // Includes all columns consumed by updateInfo(), refreshNodeListAndTokenMap(),
  // isValidPeer(), and DefaultEndPointFactory.create() from system.peers rows.
  // Columns that are absent from the actual server schema are silently excluded by
  // intersectWithNeeded(), so listing extra columns here is safe.
  @VisibleForTesting
  static final ImmutableSet<String> PEERS_COLUMNS_OF_INTEREST =
      ImmutableSet.of(
          "peer",
          "peer_port", // peers_v2 column; harmless to list here — absent on peers, excluded safely
          "rpc_address",
          "data_center",
          "rack",
          "release_version",
          "tokens",
          "listen_address",
          "listen_port",
          "host_id",
          "schema_version",
          "native_address", // may appear on some server variants; guarded by contains() in code
          "native_port", // same
          "native_transport_address",
          "native_transport_port",
          "native_transport_port_ssl",
          "workload",
          "graph",
          "dse_version");

  // IMPORTANT: see LOCAL_COLUMNS_OF_INTEREST note above.
  // Includes all columns consumed by updateInfo(), refreshNodeListAndTokenMap(),
  // isValidPeer(), and DefaultEndPointFactory.create() from system.peers_v2 rows.
  // Columns that are absent from the actual server schema are silently excluded by
  // intersectWithNeeded(), so listing extra columns here is safe.
  @VisibleForTesting
  static final ImmutableSet<String> PEERS_V2_COLUMNS_OF_INTEREST =
      ImmutableSet.of(
          "peer",
          "peer_port",
          "native_address",
          "native_port",
          "data_center",
          "rack",
          "release_version",
          "tokens",
          "host_id",
          "schema_version",
          "workload",
          "graph",
          "dse_version",
          "listen_address",
          "listen_port",
          "rpc_address", // legacy; guarded by contains() in code — harmless if absent
          "native_transport_address", // same
          "native_transport_port", // same
          "native_transport_port_ssl"); // same

  private volatile Set<String> localColumns = null;
  private volatile Set<String> peersColumns = null;
  private volatile Set<String> peersV2Columns = null;

  /**
   * Returns the full-scan query string for {@code table}: a projected {@code SELECT} if the cache
   * is warm, otherwise {@code SELECT * FROM <table> [WHERE <defaultWhereClause>]}.
   */
  String query(SystemTable table) {
    Set<String> cached = cachedColumns(table);
    if (cached == null) {
      String base = "SELECT * FROM " + table.tableName;
      return table.defaultWhereClause != null ? base + " WHERE " + table.defaultWhereClause : base;
    }
    return buildProjectedQuery(table.tableName, cached, table.defaultWhereClause);
  }

  /**
   * Populates the column cache for {@code table} from the given result set, if not already
   * populated.
   */
  void populate(SystemTable table, ResultSet rs) {
    if (cachedColumns(table) != null) return;
    ImmutableSet<String> needed = columnsOfInterest(table);
    Set<String> computed = intersectWithNeeded(rs, needed);
    switch (table) {
      case LOCAL:
        if (localColumns == null) localColumns = computed;
        break;
      case PEERS:
        if (peersColumns == null) peersColumns = computed;
        break;
      case PEERS_V2:
        if (peersV2Columns == null) peersV2Columns = computed;
        break;
    }
  }

  /**
   * Attaches a callback to {@code future} that populates the column cache for {@code table} on
   * success and resets all caches on {@link InvalidQueryException} failure. Returns the future
   * unchanged so callers can chain it directly.
   *
   * <p><b>Use only for full-table scans.</b> For single-row {@code WHERE} lookups, the result set
   * may have zero rows while still carrying valid {@code ColumnDefinitions}; the callback would
   * fire and warm the cache from an empty result, which is incorrect. Use {@link #populate} inside
   * an {@code if (row != null)} guard for that path instead.
   */
  ListenableFuture<ResultSet> hook(final SystemTable table, DefaultResultSetFuture future) {
    Futures.addCallback(
        future,
        new FutureCallback<ResultSet>() {
          @Override
          public void onSuccess(ResultSet result) {
            populate(table, result);
          }

          @Override
          public void onFailure(Throwable t) {
            if (t instanceof InvalidQueryException) reset();
          }
        },
        MoreExecutors.directExecutor());
    return future;
  }

  /**
   * Resets all column caches so that the next query to each system table sends {@code SELECT *} and
   * re-discovers available columns. Called on reconnection and on schema errors.
   */
  void reset() {
    localColumns = null;
    peersColumns = null;
    peersV2Columns = null;
  }

  /** Returns the cached column set for {@code table}, or {@code null} if not yet populated. */
  private Set<String> cachedColumns(SystemTable table) {
    switch (table) {
      case LOCAL:
        return localColumns;
      case PEERS:
        return peersColumns;
      case PEERS_V2:
        return peersV2Columns;
      default:
        throw new AssertionError("Unknown SystemTable: " + table);
    }
  }

  /** Returns the set of columns of interest for {@code table}. */
  private static ImmutableSet<String> columnsOfInterest(SystemTable table) {
    switch (table) {
      case LOCAL:
        return LOCAL_COLUMNS_OF_INTEREST;
      case PEERS:
        return PEERS_COLUMNS_OF_INTEREST;
      case PEERS_V2:
        return PEERS_V2_COLUMNS_OF_INTEREST;
      default:
        throw new AssertionError("Unknown SystemTable: " + table);
    }
  }

  /**
   * Returns the intersection of the columns returned by the server (from {@code rs}) with the given
   * {@code needed} set, or {@code null} if the intersection is empty. The result is used to cache
   * projected column lists so subsequent queries fetch only what the driver actually reads. A
   * {@code null} return keeps the cache in the "uninitialized" sentinel state, ensuring the driver
   * continues issuing {@code SELECT *} rather than generating an invalid empty-column projection.
   */
  @VisibleForTesting
  static Set<String> intersectWithNeeded(ResultSet rs, ImmutableSet<String> needed) {
    ImmutableSet.Builder<String> result = ImmutableSet.builder();
    for (ColumnDefinitions.Definition def : rs.getColumnDefinitions()) {
      if (needed.contains(def.getName())) {
        result.add(def.getName());
      }
    }
    ImmutableSet<String> built = result.build();
    return built.isEmpty() ? null : built;
  }

  /**
   * Builds a {@code SELECT col1, col2, ... FROM table [WHERE whereClause]} query string from the
   * given projected column set. Columns are sorted alphabetically so that the generated query
   * string is deterministic regardless of the iteration order of {@code columns}. {@code
   * whereClause} may be {@code null} for table-wide scans.
   */
  @VisibleForTesting
  static String buildProjectedQuery(String table, Set<String> columns, String whereClause) {
    List<String> sorted = new ArrayList<>(columns);
    Collections.sort(sorted);
    String query = "SELECT " + String.join(", ", sorted) + " FROM " + table;
    return whereClause != null ? query + " WHERE " + whereClause : query;
  }
}
