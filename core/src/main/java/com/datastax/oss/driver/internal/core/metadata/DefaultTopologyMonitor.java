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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default topology monitor, based on {@link ControlConnection}.
 *
 * <p>Note that event processing is implemented directly in the control connection, not here.
 */
@ThreadSafe
public class DefaultTopologyMonitor implements TopologyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyMonitor.class);

  // Assume topology queries never need paging
  private static final int INFINITE_PAGE_SIZE = -1;

  // A few system.peers columns which get special handling below
  private static final String NATIVE_PORT = "native_port";
  private static final String NATIVE_TRANSPORT_PORT = "native_transport_port";

  /**
   * The columns we actually read from {@code system.local}. Used to intersect with the full column
   * list returned by the first {@code SELECT *} response, so that subsequent projected queries only
   * fetch columns the driver uses.
   *
   * <p>Includes DSE-specific columns; absent columns are silently ignored by the intersection step.
   */
  @VisibleForTesting
  static final ImmutableSet<String> LOCAL_COLUMNS_OF_INTEREST =
      ImmutableSet.of(
          // Topology / addressing
          "broadcast_address",
          "broadcast_port",
          "listen_address",
          "listen_port",
          "rpc_address",
          "rpc_port",
          "native_address",
          "native_transport_address",
          "native_transport_port",
          "native_transport_port_ssl",
          // Node metadata
          "data_center",
          "rack",
          "release_version",
          "tokens",
          "partitioner",
          "host_id",
          "schema_version",
          // DSE-specific
          "dse_version",
          "graph",
          "workload",
          "workloads",
          "server_id",
          "storage_port",
          "storage_port_ssl",
          "jmx_port");

  /**
   * The columns we actually read from {@code system.peers}. Mirrors {@link
   * #LOCAL_COLUMNS_OF_INTEREST} but replaces {@code listen_address}/{@code listen_port} with the
   * {@code peer} column used as a broadcast-address fallback and peer-row identifier.
   */
  @VisibleForTesting
  static final ImmutableSet<String> PEERS_COLUMNS_OF_INTEREST =
      ImmutableSet.of(
          // Peer identifier / broadcast address fallback
          "peer",
          // Topology / addressing
          "broadcast_address",
          "broadcast_port",
          "rpc_address",
          "rpc_port",
          "native_address",
          "native_transport_address",
          "native_transport_port",
          "native_transport_port_ssl",
          // Node metadata
          "data_center",
          "rack",
          "release_version",
          "tokens",
          "partitioner",
          "host_id",
          "schema_version",
          // DSE-specific
          "dse_version",
          "graph",
          "workload",
          "workloads",
          "server_id",
          "storage_port",
          "storage_port_ssl",
          "jmx_port");

  /**
   * The columns we actually read from {@code system.peers_v2} (Cassandra ≥ 4.0). Replaces {@code
   * rpc_address} with {@code native_address}/{@code native_port} as the primary RPC endpoint
   * columns, and adds {@code peer_port}.
   */
  @VisibleForTesting
  static final ImmutableSet<String> PEERS_V2_COLUMNS_OF_INTEREST =
      ImmutableSet.of(
          // Peer identifier
          "peer",
          "peer_port",
          // Primary RPC endpoint (peers_v2-specific)
          "native_address",
          "native_port",
          // Topology / addressing
          "broadcast_address",
          "broadcast_port",
          "rpc_address",
          "native_transport_address",
          "native_transport_port",
          "native_transport_port_ssl",
          // Node metadata
          "data_center",
          "rack",
          "release_version",
          "tokens",
          "partitioner",
          "host_id",
          "schema_version",
          // DSE-specific
          "dse_version",
          "graph",
          "workload",
          "workloads",
          "server_id",
          "storage_port",
          "storage_port_ssl",
          "jmx_port");

  private final String logPrefix;
  protected final InternalDriverContext context;
  private final ControlConnection controlConnection;
  private final Duration timeout;
  private final boolean reconnectOnInit;
  private final CompletableFuture<Void> closeFuture;

  @VisibleForTesting volatile boolean isSchemaV2;
  @VisibleForTesting volatile int port = -1;

  // Column name caches: null means "not yet learned — use SELECT *".
  // Populated on the first successful response as the intersection of the server's column list
  // and the *_COLUMNS_OF_INTEREST set, so subsequent queries project only columns the driver reads.
  // Reset to null on reconnect.
  private volatile List<String> localColumns = null;
  private volatile List<String> peersColumns = null;
  private volatile List<String> peersV2Columns = null;

  public DefaultTopologyMonitor(InternalDriverContext context) {
    this.logPrefix = context.getSessionName();
    this.context = context;
    this.controlConnection = context.getControlConnection();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    this.timeout = config.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);
    this.reconnectOnInit = config.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT);
    this.closeFuture = new CompletableFuture<>();
    // Set this to true initially, after the first refreshNodes is called this will either stay true
    // or be set to false;
    this.isSchemaV2 = true;
  }

  /**
   * Resets all column name caches to null, causing the next query to use {@code SELECT *} and
   * re-learn the available columns from the response. Should be called on reconnect.
   */
  @Override
  public void resetColumnCaches() {
    localColumns = null;
    peersColumns = null;
    peersV2Columns = null;
  }

  @Override
  public void resetLocalColumnCache() {
    localColumns = null;
  }

  /**
   * Returns a new list containing only the elements of {@code serverColumns} that are present in
   * {@code needed}, preserving the server-response order. Returns an empty list (never {@code
   * null}) if no columns match.
   *
   * <p>This is used when populating the column caches from a {@code SELECT *} response: rather than
   * caching all server columns, we cache only the subset the driver actually reads, so that
   * subsequent projected queries skip unused columns (e.g. large collection columns the driver
   * never inspects).
   */
  private static List<String> intersectWithNeeded(
      List<String> serverColumns, ImmutableSet<String> needed) {
    return serverColumns.stream().filter(needed::contains).collect(ImmutableList.toImmutableList());
  }

  /**
   * Builds a {@code SELECT} query string.
   *
   * @param columns the column names to project, in the order they will appear in the query, or
   *     {@code null} to use {@code SELECT *}
   * @param table the table name (e.g. {@code "system.local"})
   * @return the query string without a trailing WHERE clause
   */
  private String buildQuery(List<String> columns, String table) {
    String projection = (columns == null) ? "*" : String.join(", ", columns);
    return "SELECT " + projection + " FROM " + table;
  }

  /**
   * Builds a {@code SELECT} query string with a WHERE clause.
   *
   * @param columns the column names to project, in the order they will appear in the query, or
   *     {@code null} to use {@code SELECT *}
   * @param table the table name
   * @param where the WHERE clause (without the {@code WHERE} keyword)
   * @return the full query string
   */
  private String buildQuery(List<String> columns, String table, String where) {
    return buildQuery(columns, table) + " WHERE " + where;
  }

  /** Returns the peers column cache appropriate for the current schema version. */
  private List<String> getPeerColumnsCache() {
    return isSchemaV2 ? peersV2Columns : peersColumns;
  }

  @Override
  public CompletionStage<Void> init() {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    return controlConnection.init(true, reconnectOnInit, true);
  }

  @Override
  public CompletionStage<Void> initFuture() {
    return controlConnection.initFuture();
  }

  @Override
  public CompletionStage<Optional<NodeInfo>> refreshNode(Node node) {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    LOG.debug("[{}] Refreshing info for {}", logPrefix, node);
    DriverChannel channel = controlConnection.channel();
    EndPoint localEndPoint = channel.getEndPoint();
    if (node.getEndPoint().equals(channel.getEndPoint())) {
      // refreshNode is called for nodes that just came up. If the control node just came up, it
      // means the control connection just reconnected, which means we did a full node refresh. So
      // we don't need to process this call.
      LOG.debug("[{}] Ignoring refresh of control node", logPrefix);
      return CompletableFuture.completedFuture(Optional.empty());
    } else if (node.getBroadcastAddress().isPresent()) {
      CompletionStage<AdminResult> query;
      if (isSchemaV2) {
        // Use SELECT * for narrow WHERE-clause queries: projecting a single-row result gives
        // negligible benefit, and the fixed WHERE form is easier to prime in test infrastructure.
        query =
            query(
                channel,
                buildQuery(null, getPeerTableName(), "peer = :address and peer_port = :port"),
                ImmutableMap.of(
                    "address",
                    node.getBroadcastAddress().get().getAddress(),
                    "port",
                    node.getBroadcastAddress().get().getPort()));
      } else {
        query =
            query(
                channel,
                buildQuery(null, getPeerTableName(), "peer = :address"),
                ImmutableMap.of("address", node.getBroadcastAddress().get().getAddress()));
      }
      return query.thenApply(result -> firstPeerRowAsNodeInfo(result, localEndPoint));
    } else {
      return query(channel, buildQuery(getPeerColumnsCache(), getPeerTableName()))
          .thenApply(result -> findInPeers(result, node.getHostId(), localEndPoint));
    }
  }

  @Override
  public CompletionStage<Optional<NodeInfo>> getNewNodeInfo(InetSocketAddress broadcastRpcAddress) {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    LOG.debug("[{}] Fetching info for new node {}", logPrefix, broadcastRpcAddress);
    DriverChannel channel = controlConnection.channel();
    EndPoint localEndPoint = channel.getEndPoint();
    return query(channel, buildQuery(getPeerColumnsCache(), getPeerTableName()))
        .thenApply(result -> findInPeers(result, broadcastRpcAddress, localEndPoint));
  }

  @Override
  public CompletionStage<NodeInfo> getChannelNodeInfo(DriverChannel channel) {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    EndPoint localEndPoint = channel.getEndPoint();
    return query(channel, buildQuery(localColumns, "system.local", "key='local'"))
        .thenApply(result -> toLocalNodeInfo(result, localEndPoint));
  }

  /**
   * Decodes the single {@code system.local} row of {@code result} into a {@link NodeInfo}, warming
   * the local column cache from it on the way.
   *
   * <p>The warming is only sound because the caller clears this cache <b>before</b> each read.
   * {@link #getChannelNodeInfo} is no longer reached only for the control channel the driver keeps:
   * {@code ControlConnection#readChannelNodeInfo} runs it from a connect hook, once per candidate
   * address of a contact point, and whether that candidate is the one kept is not known when the
   * read returns -- the hook can refuse it, {@code ChannelFactory} can abandon it afterwards (a
   * REGISTER rejection, or a hook the timeout gave up on whose response arrives anyway), and {@code
   * ControlConnection} re-asks about the node once the channel is open. The intersection can only
   * ever shrink, so a refused candidate would otherwise narrow the projection for every {@code
   * system.local} read of the session, costing the accepted node's extra columns for as long as the
   * cache lives -- and on the first connection, the one round where the hook is guaranteed to run,
   * {@code #onSuccessfulReconnect} returns before it would reset them.
   *
   * <p>Clearing first rather than undoing afterwards is what makes that hold: an undo on the
   * rejection paths cannot see the projection a <i>previous</i> candidate left behind, and two of
   * those paths are {@code ChannelFactory}'s and invisible to the hook. With the cache cleared
   * first, every read is a {@code SELECT *} that re-learns from whoever answered it. See {@link
   * #resetLocalColumnCache()} and {@code ControlConnection#readChannelNodeInfo}.
   *
   * <p>The write below is unconditional, not guarded on the cache still being null, because
   * "cleared before each read" orders the reads and not the <i>responses</i>. A candidate abandoned
   * on the connect-hook timeout is abandoned rather than cancelled, so its {@code system.local}
   * answer can arrive after the next candidate cleared the cache -- and a first-writer-wins guard
   * would then install the refused candidate's intersection and decline to overwrite it with the
   * accepted one's. Overwriting costs nothing on any other path: a projected read returns exactly
   * the projection it asked for, and intersecting that with {@code LOCAL_COLUMNS_OF_INTEREST} again
   * yields the same list.
   *
   * <p>That covers two of the three orders in which a stray answer, the kept candidate's answer,
   * and {@code ControlConnection}'s read of the capture can land. A stray answering <b>before</b>
   * the kept candidate is overwritten here. A stray answering <b>between</b> the kept candidate and
   * that read replaces the capture, so {@code NodeInfoHolder#getFor} misses and {@code
   * ControlConnection#resolveChannelNodeIfNeeded} goes back for a fresh read -- cleared first, on
   * the channel that is actually open.
   *
   * <p>What neither end catches is a stray answering <b>after</b> that read: the capture was still
   * the kept candidate's when it was consulted, so nothing falls back, and the stray's warming is
   * then the last one to land. The window is the REGISTER round trip plus a hop to the admin
   * thread, and a reconnect self-corrects at {@code #resetColumnCaches()}; the first connection
   * does not, because {@code ControlConnection#onSuccessfulReconnect} returns at its {@code
   * isFirstConnection} check before reaching it. Closing it means the projection carrying the
   * channel it was learned from, so that a write from any other channel is dropped outright rather
   * than two guards agreeing by construction -- deferred, and named here rather than claimed away.
   */
  private NodeInfo toLocalNodeInfo(AdminResult result, EndPoint localEndPoint) {
    if (!result.getColumnNames().isEmpty()) {
      localColumns = intersectWithNeeded(result.getColumnNames(), LOCAL_COLUMNS_OF_INTEREST);
    }
    Iterator<AdminRow> iterator = result.iterator();
    if (!iterator.hasNext()) {
      throw new IllegalStateException(
          "Expected a row in system.local for node info resolution, got empty result");
    }
    AdminRow localRow = iterator.next();
    InetSocketAddress broadcastRpcAddress = getBroadcastRpcAddress(localRow, localEndPoint);
    return nodeInfoBuilder(localRow, broadcastRpcAddress, localEndPoint).build();
  }

  @Override
  public CompletionStage<Iterable<NodeInfo>> refreshNodeList() {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    LOG.debug("[{}] Refreshing node list", logPrefix);
    DriverChannel channel = controlConnection.channel();
    EndPoint localEndPoint = channel.getEndPoint();

    savePort(channel);

    CompletionStage<AdminResult> localQuery =
        query(channel, buildQuery(localColumns, "system.local", "key='local'"));
    CompletionStage<AdminResult> peersV2Query =
        query(channel, buildQuery(peersV2Columns, "system.peers_v2"));
    CompletableFuture<AdminResult> peersQuery = new CompletableFuture<>();

    peersV2Query.whenComplete(
        (r, t) -> {
          if (t != null) {
            // If system.peers_v2 does not exist, downgrade to system.peers
            if (t instanceof UnexpectedResponseException
                && ((UnexpectedResponseException) t).message instanceof Error) {
              Error error = (Error) ((UnexpectedResponseException) t).message;
              if (error.code == ProtocolConstants.ErrorCode.INVALID
                  // Also downgrade on server error with a specific error message (DSE 6.0.0 to
                  // 6.0.2 with search enabled)
                  || (error.code == ProtocolConstants.ErrorCode.SERVER_ERROR
                      && error.message.contains("Unknown keyspace/cf pair (system.peers_v2)"))) {
                this.isSchemaV2 = false; // We should not attempt this query in the future.
                CompletableFutures.completeFrom(
                    query(channel, buildQuery(peersColumns, "system.peers")), peersQuery);
                return;
              }
            }
            peersQuery.completeExceptionally(t);
          } else {
            if (peersV2Columns == null && !r.getColumnNames().isEmpty()) {
              peersV2Columns =
                  intersectWithNeeded(r.getColumnNames(), PEERS_V2_COLUMNS_OF_INTEREST);
            }
            peersQuery.complete(r);
          }
        });

    return localQuery.thenCombine(
        peersQuery,
        (controlNodeResult, peersResult) -> {
          if (localColumns == null && !controlNodeResult.getColumnNames().isEmpty()) {
            localColumns =
                intersectWithNeeded(controlNodeResult.getColumnNames(), LOCAL_COLUMNS_OF_INTEREST);
          }
          if (!isSchemaV2 && peersColumns == null && !peersResult.getColumnNames().isEmpty()) {
            peersColumns =
                intersectWithNeeded(peersResult.getColumnNames(), PEERS_COLUMNS_OF_INTEREST);
          }
          List<NodeInfo> nodeInfos = new ArrayList<>();
          AdminRow localRow = controlNodeResult.iterator().next();
          InetSocketAddress localBroadcastRpcAddress =
              getBroadcastRpcAddress(localRow, localEndPoint);
          nodeInfos.add(nodeInfoBuilder(localRow, localBroadcastRpcAddress, localEndPoint).build());
          for (AdminRow peerRow : peersResult) {
            if (isPeerValid(peerRow)) {
              InetSocketAddress peerBroadcastRpcAddress =
                  getBroadcastRpcAddress(peerRow, localEndPoint);
              if (peerBroadcastRpcAddress != null) {
                NodeInfo nodeInfo =
                    nodeInfoBuilder(peerRow, peerBroadcastRpcAddress, localEndPoint).build();
                nodeInfos.add(nodeInfo);
              }
            }
          }
          return nodeInfos;
        });
  }

  @Override
  public CompletionStage<Boolean> checkSchemaAgreement() {
    if (closeFuture.isDone()) {
      return CompletableFuture.completedFuture(true);
    }
    DriverChannel channel = controlConnection.channel();
    return new SchemaAgreementChecker(channel, context, logPrefix).run();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    closeFuture.complete(null);
    return closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return closeAsync();
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(
      DriverChannel channel, String queryString, Map<String, Object> parameters) {
    AdminRequestHandler<AdminResult> handler;
    try {
      handler =
          AdminRequestHandler.query(
              channel, queryString, parameters, timeout, INFINITE_PAGE_SIZE, logPrefix);
    } catch (Exception e) {
      return CompletableFutures.failedFuture(e);
    }
    return handler.start();
  }

  private CompletionStage<AdminResult> query(DriverChannel channel, String queryString) {
    return query(channel, queryString, Collections.emptyMap());
  }

  private String getPeerTableName() {
    return isSchemaV2 ? "system.peers_v2" : "system.peers";
  }

  private Optional<NodeInfo> firstPeerRowAsNodeInfo(AdminResult result, EndPoint localEndPoint) {
    Iterator<AdminRow> iterator = result.iterator();
    if (iterator.hasNext()) {
      AdminRow row = iterator.next();
      if (isPeerValid(row)) {
        return Optional.ofNullable(getBroadcastRpcAddress(row, localEndPoint))
            .map(
                broadcastRpcAddress ->
                    nodeInfoBuilder(row, broadcastRpcAddress, localEndPoint).build());
      }
    }
    return Optional.empty();
  }

  /**
   * Creates a {@link DefaultNodeInfo.Builder} instance from the given row.
   *
   * @param broadcastRpcAddress this is a parameter only because we already have it when we come
   *     from {@link #findInPeers(AdminResult, InetSocketAddress, EndPoint)}. Callers that don't
   *     already have it can use {@link #getBroadcastRpcAddress}. For the control host, this can be
   *     null; if this node is a peer however, this cannot be null, since we use that address to
   *     create the node's endpoint. Callers can use {@link #isPeerValid(AdminRow)} to check that
   *     before calling this method.
   * @param localEndPoint the control node endpoint that was used to query the node's system tables.
   *     This is a parameter because it would be racy to call {@code
   *     controlConnection.channel().getEndPoint()} from within this method, as the control
   *     connection may have changed its channel since. So this parameter must be provided by the
   *     caller.
   */
  @NonNull
  protected DefaultNodeInfo.Builder nodeInfoBuilder(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {

    EndPoint endPoint = buildNodeEndPoint(row, broadcastRpcAddress, localEndPoint);

    // in system.local
    InetAddress broadcastInetAddress = row.getInetAddress("broadcast_address");
    if (broadcastInetAddress == null) {
      // in system.peers or system.peers_v2
      broadcastInetAddress = row.getInetAddress("peer");
    }

    Integer broadcastPort = 0;
    if (row.contains("broadcast_port")) {
      // system.local for Cassandra >= 4.0
      broadcastPort = row.getInteger("broadcast_port");
    } else if (row.contains("peer_port")) {
      // system.peers_v2
      broadcastPort = row.getInteger("peer_port");
    }

    InetSocketAddress broadcastAddress = null;
    if (broadcastInetAddress != null && broadcastPort != null) {
      broadcastAddress = new InetSocketAddress(broadcastInetAddress, broadcastPort);
    }

    // in system.local only, and only for Cassandra versions >= 2.0.17, 2.1.8, 2.2.0 rc2;
    // not present in system.peers nor system.peers_v2
    InetAddress listenInetAddress = row.getInetAddress("listen_address");

    // in system.local only, and only for Cassandra >= 4.0
    Integer listenPort = 0;
    if (row.contains("listen_port")) {
      listenPort = row.getInteger("listen_port");
    }

    InetSocketAddress listenAddress = null;
    if (listenInetAddress != null && listenPort != null) {
      listenAddress = new InetSocketAddress(listenInetAddress, listenPort);
    }

    DefaultNodeInfo.Builder builder =
        DefaultNodeInfo.builder()
            .withEndPoint(endPoint)
            .withBroadcastRpcAddress(broadcastRpcAddress)
            .withBroadcastAddress(broadcastAddress)
            .withListenAddress(listenAddress)
            .withDatacenter(row.getString("data_center"))
            .withRack(row.getString("rack"))
            .withCassandraVersion(row.getString("release_version"))
            .withTokens(row.getSetOfString("tokens"))
            .withPartitioner(row.getString("partitioner"))
            .withHostId(
                Objects.requireNonNull(
                    row.getUuid("host_id"),
                    "host_id is null in system.local, node may still be bootstrapping"))
            .withSchemaVersion(row.getUuid("schema_version"));

    // Handle DSE-specific columns, if present
    String rawVersion = row.getString("dse_version");
    if (rawVersion != null) {
      builder.withExtra(DseNodeProperties.DSE_VERSION, Version.parse(rawVersion));
    }

    ImmutableSet.Builder<String> workloadsBuilder = ImmutableSet.builder();
    Boolean legacyGraph = row.getBoolean("graph"); // DSE 5.0
    if (legacyGraph != null && legacyGraph) {
      workloadsBuilder.add("Graph");
    }
    String legacyWorkload = row.getString("workload"); // DSE 5.0 (other than graph)
    if (legacyWorkload != null) {
      workloadsBuilder.add(legacyWorkload);
    }
    Set<String> modernWorkloads = row.getSetOfString("workloads"); // DSE 5.1+
    if (modernWorkloads != null) {
      workloadsBuilder.addAll(modernWorkloads);
    }
    ImmutableSet<String> workloads = workloadsBuilder.build();
    if (!workloads.isEmpty()) {
      builder.withExtra(DseNodeProperties.DSE_WORKLOADS, workloads);
    }

    // Note: withExtra discards null values
    builder
        .withExtra(DseNodeProperties.SERVER_ID, row.getString("server_id"))
        .withExtra(DseNodeProperties.NATIVE_TRANSPORT_PORT, row.getInteger("native_transport_port"))
        .withExtra(
            DseNodeProperties.NATIVE_TRANSPORT_PORT_SSL,
            row.getInteger("native_transport_port_ssl"))
        .withExtra(DseNodeProperties.STORAGE_PORT, row.getInteger("storage_port"))
        .withExtra(DseNodeProperties.STORAGE_PORT_SSL, row.getInteger("storage_port_ssl"))
        .withExtra(DseNodeProperties.JMX_PORT, row.getInteger("jmx_port"));

    return builder;
  }

  /**
   * Builds the node's endpoint from the given row.
   *
   * @param broadcastRpcAddress this is a parameter only because we already have it when we come
   *     from {@link #findInPeers(AdminResult, InetSocketAddress, EndPoint)}. Callers that don't
   *     already have it can use {@link #getBroadcastRpcAddress}. For the control host, this can be
   *     null; if this node is a peer however, this cannot be null, since we use that address to
   *     create the node's endpoint. Callers can use {@link #isPeerValid(AdminRow)} to check that
   *     before calling this method.
   * @param localEndPoint the control node endpoint that was used to query the node's system tables.
   *     This is a parameter because it would be racy to call {@code
   *     controlConnection.channel().getEndPoint()} from within this method, as the control
   *     connection may have changed its channel since. So this parameter must be provided by the
   *     caller.
   */
  @NonNull
  protected EndPoint buildNodeEndPoint(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    boolean peer = row.contains("peer");
    if (peer) {
      // If this node is a peer, its broadcast RPC address must be present.
      Objects.requireNonNull(
          broadcastRpcAddress, "broadcastRpcAddress cannot be null for a peer row");
      // Deployments that use a custom EndPoint implementation will need their own TopologyMonitor.
      // One simple approach is to extend this class and override this method.

      InetSocketAddress translatedAddress =
          context.getAddressTranslator().translate(broadcastRpcAddress);
      return new DefaultEndPoint(translatedAddress);
    } else {
      // Don't rely on system.local.rpc_address for the control node, because it mistakenly
      // reports the normal RPC address instead of the broadcast one (CASSANDRA-11181). We
      // already know the endpoint anyway since we've just used it to query.
      return connectedNodeEndPoint(localEndPoint);
    }
  }

  /**
   * The endpoint to register the connected node under: the address the control channel actually
   * reached, rather than the contact point it was reached through.
   *
   * <p>They differ when the control connection came up through a contact point, because a contact
   * point is kept unresolved and {@code ChannelFactory} binds a {@linkplain PinnableEndPoint
   * pinned} copy of it to the one address the channel reached -- a copy that, by that interface's
   * contract, is identified exactly like the unpinned original. Registering the node under it would
   * give it a <i>hostname</i> identity: metric names and tags derived from a name that denotes the
   * whole cluster rather than this node. That is bad enough on its own, but the real damage is that
   * the identity is not the node's: the reconnection fallback hands the contact points back on
   * every reconnection round, so each successive control node acquires the same one. Two live nodes
   * then report under a single metric prefix, sharing get-or-create metric objects, until the next
   * refresh moves the older one back to its own address -- and {@code clearMetrics()} recomputes
   * the names to delete from the prefix the node still holds, taking the newcomer's freshly
   * registered series with it (see {@code DefaultNode#setEndPoint}).
   *
   * <p>Deriving the identity from the address actually connected to fixes all of that at once: it
   * is this node's own address, so it is unique to it, and re-registering an unchanged control node
   * becomes a no-op instead of an identity change.
   *
   * <p>The identity comes from the connected address's <b>bytes</b>, not from its host string. That
   * string is not the node's either -- for a hostname contact point it is the queried name, which
   * every resolver attaches to what it returns and {@code ChannelFactory#reattachHostname} restores
   * when a custom one does not, so reading it back here would produce the contact point's prefix
   * again and this method would do nothing at all. It is also not stable: for an IP-literal contact
   * point it starts out as the literal and begins reporting a reverse-DNS name as soon as {@code
   * DefaultSslEngineFactory} calls {@code getHostName()} on the shared {@code InetAddress}, so an
   * identity keyed off it would depend on whether TLS is enabled and whether a PTR record exists.
   * Stripping the label (see {@link AddressUtils#stripHostName}) settles both.
   *
   * <p>What the node connects to is unaffected: the rebuilt endpoint still {@linkplain
   * EndPoint#resolve() resolves} to the labelled address the channel reached, so the TLS peer host
   * and the Kerberos service name stay the name the operator configured, with no reverse lookup --
   * see {@link DefaultEndPoint#identifiedBy}.
   *
   * <p>Two costs come with it, both narrower than the damage above and both named here rather than
   * argued away. The rewrite is an identity change, so {@code DefaultNode#setEndPoint} clears the
   * previous updater's metrics before the swap, and Dropwizard and MicroProfile recompute the names
   * to delete from the prefix the node still holds -- which under a translator that hands back one
   * name for the whole cluster is a prefix every node shares, so promoting such a peer to control
   * node takes the cluster's node-metric series with it (the root of that is {@code clearMetrics()}
   * not remembering what it registered; see https://github.com/scylladb/java-driver/issues/1010).
   * And the endpoint this hands back resolves to one address, so the control node is the single
   * node in such a deployment that does <b>not</b> get its name re-expanded per connection attempt:
   * its pool cannot fail over to a sibling record and will not pick up a DNS change until the
   * control connection moves and the node is re-derived from {@code system.peers}. That is the
   * trade {@code TopologyMonitor#reresolvesNodeAddresses()} describes for this node, and it is why
   * the contact-point reconnection fallback is what recovers it.
   *
   * <p>Endpoints this cannot rebuild are returned untouched -- a third-party {@link EndPoint}, or
   * one whose {@code resolve()} is not a resolved {@code InetSocketAddress} (the user disabled
   * Netty's resolver, or a custom one declined the address, so nothing was pinned). So is one that
   * already carries the connected address, which is every reconnection to an identified node and
   * every refresh after the first: the existing instance is kept so that the control node's
   * endpoint stays {@code ==} to the channel's, which is what lets {@link #refreshNode}'s
   * control-node check settle on the identity short-circuit in {@code equals()}. Where that does
   * not hold -- the channel kept an unresolved endpoint because adoption was skipped -- the check
   * falls through to a full address comparison, which for a <i>name</i> costs a lookup on the admin
   * thread and only answers "equal" if the resolver lists the reached address first. A miss there
   * is not fatal, but it does send refreshNode on to query the peers table for the control node's
   * own address, which by definition has no row; see
   * https://github.com/scylladb/java-driver/issues/1006.
   */
  private static EndPoint connectedNodeEndPoint(EndPoint localEndPoint) {
    if (!(localEndPoint instanceof DefaultEndPoint)) {
      return localEndPoint;
    }
    SocketAddress connected = localEndPoint.resolve();
    if (!(connected instanceof InetSocketAddress)
        || ((InetSocketAddress) connected).isUnresolved()) {
      return localEndPoint;
    }
    InetSocketAddress reached = (InetSocketAddress) connected;
    InetSocketAddress identity = AddressUtils.stripHostName(reached);
    if (identity == null) {
      return localEndPoint;
    }
    DefaultEndPoint asConnected = DefaultEndPoint.identifiedBy(identity, reached);
    return asConnected.asMetricPrefix().equals(localEndPoint.asMetricPrefix())
        ? localEndPoint
        : asConnected;
  }

  // Called when a new node is being added; the peers table is keyed by broadcast_address,
  // but the received event only contains broadcast_rpc_address, so
  // we have to traverse the whole table and check the rows one by one.
  private Optional<NodeInfo> findInPeers(
      AdminResult result, InetSocketAddress broadcastRpcAddressToFind, EndPoint localEndPoint) {
    for (AdminRow row : result) {
      InetSocketAddress broadcastRpcAddress = getBroadcastRpcAddress(row, localEndPoint);
      if (broadcastRpcAddress != null
          && broadcastRpcAddress.equals(broadcastRpcAddressToFind)
          && isPeerValid(row)) {
        return Optional.of(nodeInfoBuilder(row, broadcastRpcAddress, localEndPoint).build());
      }
    }
    LOG.debug("[{}] Could not find any peer row matching {}", logPrefix, broadcastRpcAddressToFind);
    return Optional.empty();
  }

  // Called when refreshing an existing node, and we don't know its broadcast address; in this
  // case we attempt a search by host id and have to traverse the whole table and check the rows one
  // by one.
  private Optional<NodeInfo> findInPeers(
      AdminResult result, UUID hostIdToFind, EndPoint localEndPoint) {
    for (AdminRow row : result) {
      UUID hostId = row.getUuid("host_id");
      if (hostId != null && hostId.equals(hostIdToFind) && isPeerValid(row)) {
        return Optional.ofNullable(getBroadcastRpcAddress(row, localEndPoint))
            .map(
                broadcastRpcAddress ->
                    nodeInfoBuilder(row, broadcastRpcAddress, localEndPoint).build());
      }
    }
    LOG.debug("[{}] Could not find any peer row matching {}", logPrefix, hostIdToFind);
    return Optional.empty();
  }

  // Current versions of Cassandra (3.11 at the time of writing), require the same port for all
  // nodes. As a consequence, the port is not stored in system tables.
  // We save it the first time we get a control connection channel.
  protected void savePort(DriverChannel channel) {
    if (port < 0) {
      SocketAddress address = channel.getEndPoint().resolve();
      if (address instanceof InetSocketAddress) {
        port = ((InetSocketAddress) address).getPort();
      }
    }
  }

  /**
   * Determines the broadcast RPC address of the node represented by the given row.
   *
   * @param row The row to inspect; can represent either a local (control) node or a peer node.
   * @param localEndPoint the control node endpoint that was used to query the node's system tables.
   *     This is a parameter because it would be racy to call {@code
   *     controlConnection.channel().getEndPoint()} from within this method, as the control
   *     connection may have changed its channel since. So this parameter must be provided by the
   *     caller.
   * @return the broadcast RPC address of the node, if it could be determined; or {@code null}
   *     otherwise.
   */
  @Nullable
  protected InetSocketAddress getBroadcastRpcAddress(
      @NonNull AdminRow row, @NonNull EndPoint localEndPoint) {

    InetAddress broadcastRpcInetAddress = null;
    Iterator<String> addrCandidates =
        Iterators.forArray(
            // in system.peers_v2 (Cassandra >= 4.0)
            "native_address",
            // DSE 6.8 introduced native_transport_address and native_transport_port for the
            // listen address.
            "native_transport_address",
            // in system.peers or system.local
            "rpc_address");

    while (broadcastRpcInetAddress == null && addrCandidates.hasNext())
      broadcastRpcInetAddress = row.getInetAddress(addrCandidates.next());
    // This could only happen if system tables are corrupted, but handle gracefully
    if (broadcastRpcInetAddress == null) {
      LOG.warn(
          "[{}] Unable to determine broadcast RPC IP address, returning null.  "
              + "This is likely due to a misconfiguration or invalid system tables.  "
              + "Please validate the contents of system.local and/or {}.",
          logPrefix,
          getPeerTableName());
      return null;
    }

    Integer broadcastRpcPort = null;
    Iterator<String> portCandidates =
        Iterators.forArray(
            // in system.peers_v2 (Cassandra >= 4.0)
            NATIVE_PORT,
            // DSE 6.8 introduced native_transport_address and native_transport_port for the
            // listen address.
            NATIVE_TRANSPORT_PORT,
            // system.local for Cassandra >= 4.0
            "rpc_port");

    while ((broadcastRpcPort == null || broadcastRpcPort == 0) && portCandidates.hasNext()) {

      String colName = portCandidates.next();
      broadcastRpcPort = row.getInteger(colName);
      // Support override for SSL port (if enabled) in DSE
      if (NATIVE_TRANSPORT_PORT.equals(colName) && context.getSslEngineFactory().isPresent()) {

        String sslColName = colName + "_ssl";
        broadcastRpcPort = row.getInteger(sslColName);
      }
    }
    // use the default port if no port information was found in the row;
    // note that in rare situations, the default port might not be known, in which case we
    // report zero, as advertised in the javadocs of Node and NodeInfo.
    if (broadcastRpcPort == null || broadcastRpcPort == 0) {

      LOG.warn(
          "[{}] Unable to determine broadcast RPC port.  "
              + "Trying to fall back to port used by the control connection.",
          logPrefix);
      broadcastRpcPort = port == -1 ? 0 : port;
    }

    InetSocketAddress broadcastRpcAddress =
        new InetSocketAddress(broadcastRpcInetAddress, broadcastRpcPort);
    if (row.contains("peer") && broadcastRpcAddress.equals(localEndPoint.resolve())) {
      // JAVA-2303: if the peer is actually the control node, ignore that peer as it is likely
      // a misconfiguration problem.
      LOG.warn(
          "[{}] Control node {} has an entry for itself in {}: this entry will be ignored. "
              + "This is likely due to a misconfiguration; please verify your rpc_address "
              + "configuration in cassandra.yaml on all nodes in your cluster.",
          logPrefix,
          localEndPoint,
          getPeerTableName());
      return null;
    }

    return broadcastRpcAddress;
  }

  /**
   * Returns {@code true} if the given peer row is valid, and {@code false} otherwise.
   *
   * <p>This method must at least ensure that the row contains enough information to extract the
   * node's broadcast RPC address and host ID; otherwise the driver may not work properly.
   */
  protected boolean isPeerValid(AdminRow peerRow) {
    if (PeerRowValidator.isValid(
        peerRow,
        context
            .getConfig()
            .getDefaultProfile()
            .getBoolean(DefaultDriverOption.METADATA_ALLOW_ZERO_TOKEN_PEERS))) {
      return true;
    } else {
      LOG.warn(
          "[{}] Found invalid row in {} for peer: {}. "
              + "This is likely a gossip or snitch issue, this node will be ignored.",
          logPrefix,
          getPeerTableName(),
          peerRow.getInetAddress("peer"));
      return false;
    }
  }
}
