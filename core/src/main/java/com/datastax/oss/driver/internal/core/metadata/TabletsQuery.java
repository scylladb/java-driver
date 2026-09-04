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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/** Loads a complete tablet map from {@code system.tablets}. */
class TabletsQuery {

  private static final String SELECT_TABLETS =
      "SELECT * FROM system.tablets WHERE table_id = :table_id";
  private static final TupleType REPLICA_TYPE = DataTypes.tupleOf(DataTypes.UUID, DataTypes.INT);
  private static final TypeCodec<List<TupleValue>> REPLICAS_CODEC =
      TypeCodecs.listOf(TypeCodecs.tupleOf(REPLICA_TYPE));

  private final DriverChannel channel;
  private final Supplier<Map<UUID, Node>> nodesSupplier;
  private final Duration timeout;
  private final int pageSize;
  private final String logPrefix;
  private final String usingTimeoutClause;

  TabletsQuery(
      DriverChannel channel,
      Supplier<Map<UUID, Node>> nodesSupplier,
      DriverExecutionProfile config,
      String logPrefix) {
    this.channel = channel;
    this.nodesSupplier = nodesSupplier;
    this.timeout = config.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT);
    this.pageSize = config.getInt(DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE);
    this.logPrefix = logPrefix;
    this.usingTimeoutClause = " USING TIMEOUT " + timeout.toMillis() + "ms";
  }

  CompletionStage<List<Tablet>> execute(UUID tableId) {
    return execute(tableId, new HashSet<>());
  }

  private CompletionStage<List<Tablet>> execute(UUID tableId, Set<UUID> visitedTableIds) {
    if (!visitedTableIds.add(tableId)) {
      return CompletableFutures.failedFuture(
          new IllegalStateException("Circular tablet base table reference for " + tableId));
    }
    return loadRows(tableId)
        .thenCompose(
            rows -> {
              if (rows.baseTableId != null) {
                return execute(rows.baseTableId, visitedTableIds);
              }
              return CompletableFuture.completedFuture(buildTablets(rows, nodesSupplier.get()));
            });
  }

  private CompletionStage<TabletRows> loadRows(UUID tableId) {
    TabletRows rows = new TabletRows();
    CompletableFuture<TabletRows> future = new CompletableFuture<>();
    String query = SELECT_TABLETS + (channel.getShardingInfo() == null ? "" : usingTimeoutClause);
    Map<String, Object> parameters = Collections.singletonMap("table_id", tableId);
    CompletionStage<AdminResult> firstPage =
        AdminRequestHandler.query(channel, query, parameters, timeout, pageSize, logPrefix).start();
    loadPage(firstPage, rows, future);
    return future;
  }

  private void loadPage(
      CompletionStage<AdminResult> page, TabletRows rows, CompletableFuture<TabletRows> future) {
    page.whenComplete(
        (result, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
            return;
          }
          for (AdminRow row : result) {
            UUID baseTableId = row.getUuid("base_table");
            if (baseTableId != null) {
              rows.baseTableId = baseTableId;
            }
            Integer tabletCount = row.getInteger("tablet_count");
            if (tabletCount != null) {
              rows.tabletCount = tabletCount;
            }
            Long lastToken = row.get("last_token", TypeCodecs.BIGINT);
            List<TupleValue> replicas = row.get("replicas", REPLICAS_CODEC);
            if (lastToken != null && replicas != null) {
              rows.tablets.add(new TabletRow(lastToken, replicas));
            }
          }
          if (result.hasNextPage()) {
            loadPage(result.nextPage(), rows, future);
          } else {
            future.complete(rows);
          }
        });
  }

  static List<Tablet> buildTablets(TabletRows rows, Map<UUID, Node> nodes) {
    rows.tablets.sort(Comparator.comparingLong(row -> row.lastToken));
    if (rows.tablets.isEmpty()) {
      throw new IllegalStateException("No tablet metadata returned from system.tablets");
    }
    if (rows.tabletCount != null && rows.tabletCount != rows.tablets.size()) {
      throw new IllegalStateException(
          String.format(
              "Expected %d tablets but received %d", rows.tabletCount, rows.tablets.size()));
    }

    ImmutableList.Builder<Tablet> tablets = ImmutableList.builder();
    long firstToken = Long.MIN_VALUE;
    for (TabletRow row : rows.tablets) {
      if (row.lastToken <= firstToken) {
        throw new IllegalStateException("Invalid tablet token range returned from system.tablets");
      }
      tablets.add(createTablet(firstToken, row, nodes));
      firstToken = row.lastToken;
    }
    if (firstToken != Long.MAX_VALUE) {
      throw new IllegalStateException("Tablet metadata does not cover the complete token range");
    }
    return tablets.build();
  }

  private static Tablet createTablet(long firstToken, TabletRow row, Map<UUID, Node> nodes) {
    if (row.replicas.isEmpty()) {
      throw new IllegalStateException("Tablet has no replicas");
    }
    List<Node> replicaNodes = new ArrayList<>();
    Map<Node, Integer> replicaShards = new HashMap<>();
    for (TupleValue replica : row.replicas) {
      UUID hostId = replica.getUuid(0);
      Node node = nodes.get(hostId);
      if (node == null) {
        throw new IllegalStateException("Unknown tablet replica host ID " + hostId);
      }
      if (!replicaNodes.contains(node)) {
        replicaNodes.add(node);
      }
      replicaShards.put(node, replica.getInt(1));
    }
    return new DefaultTabletMap.DefaultTablet(
        firstToken, row.lastToken, replicaNodes, replicaShards);
  }

  static class TabletRows {
    final List<TabletRow> tablets = new ArrayList<>();
    UUID baseTableId;
    Integer tabletCount;
  }

  static class TabletRow {
    final long lastToken;
    final List<TupleValue> replicas;

    TabletRow(long lastToken, List<TupleValue> replicas) {
      this.lastToken = lastToken;
      this.replicas = replicas;
    }
  }
}
