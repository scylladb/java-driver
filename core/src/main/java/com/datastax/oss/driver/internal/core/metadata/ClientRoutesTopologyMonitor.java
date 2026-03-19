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

import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.clientroutes.ClientRouteRecord;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ClientRoutesTopologyMonitor extends DefaultTopologyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesTopologyMonitor.class);

  private static final String SELECT_ROUTES_COLUMNS =
      "SELECT host_id, address, port, tls_port, connection_id FROM %s";

  /** Disables result-set paging, matching the convention used by {@link DefaultTopologyMonitor}. */
  private static final int NO_PAGING = -1;

  /** Maximum number of CAS attempts when acquiring the in-flight refresh slot. */
  private static final int MAX_CAS_ATTEMPTS = 30;

  /** Initial backoff delay (in milliseconds) between CAS retry attempts. */
  private static final long BACKOFF_START_MS = 100;

  /** Maximum backoff delay (in milliseconds) between CAS retry attempts. */
  private static final long BACKOFF_MAX_MS = 60_000;

  /**
   * Number of consecutive empty full-refresh results before clearing the cache. This guards against
   * stale routes being served indefinitely after legitimate route removal.
   */
  private static final int MAX_CONSECUTIVE_EMPTY_RESULTS = 3;

  private final ClientRoutesConfig config;
  private final List<String> configuredConnectionIds;
  private final Map<String, String> connectionAddrOverrides;
  private final String logPrefix;
  private final AtomicReference<Map<UUID, ClientRouteRecord>> resolvedRoutesCache;
  private final boolean useSSL;
  private volatile boolean closed = false;
  private final AtomicInteger consecutiveEmptyResults = new AtomicInteger(0);

  private final AtomicReference<CompletionStage<Void>> inFlightRefresh = new AtomicReference<>();
  private final AtomicReference<RefreshRequest> queuedRefresh = new AtomicReference<>();

  /**
   * Holds coalesced parameters for a queued refresh. At most one request is queued; new arrivals
   * are merged into the existing queued request via {@link #coalesce}.
   */
  static class RefreshRequest {
    @Nullable final List<String> connectionIds;
    @Nullable final List<String> hostIds;

    RefreshRequest(@Nullable List<String> connectionIds, @Nullable List<String> hostIds) {
      this.connectionIds = connectionIds;
      this.hostIds = hostIds;
    }

    boolean isFull() {
      return hostIds == null || hostIds.isEmpty();
    }

    /** Merges two requests. Full refresh subsumes targeted; two targeted merge their IDs. */
    RefreshRequest coalesce(RefreshRequest other) {
      if (this.isFull() || other.isFull()) {
        return new RefreshRequest(null, null);
      }
      LinkedHashSet<String> mergedHosts = new LinkedHashSet<>(this.hostIds);
      mergedHosts.addAll(other.hostIds);
      LinkedHashSet<String> mergedConns = new LinkedHashSet<>();
      if (this.connectionIds != null) {
        mergedConns.addAll(this.connectionIds);
      }
      if (other.connectionIds != null) {
        mergedConns.addAll(other.connectionIds);
      }
      List<String> connIds = mergedConns.isEmpty() ? null : new ArrayList<>(mergedConns);
      return new RefreshRequest(connIds, new ArrayList<>(mergedHosts));
    }
  }

  private volatile Object clientRoutesUpdateKey;

  public ClientRoutesTopologyMonitor(
      @NonNull InternalDriverContext context, @NonNull ClientRoutesConfig config) {
    super(context);
    this.config = config;
    this.configuredConnectionIds =
        Collections.unmodifiableList(
            config.getEndpoints().stream()
                .map(ClientRouteProxy::getConnectionId)
                .collect(Collectors.toList()));
    this.connectionAddrOverrides =
        Collections.unmodifiableMap(
            config.getEndpoints().stream()
                .filter(ep -> ep.getConnectionAddr() != null)
                .collect(
                    Collectors.toMap(
                        ClientRouteProxy::getConnectionId, ClientRouteProxy::getConnectionAddr)));
    this.logPrefix = context.getSessionName();
    this.resolvedRoutesCache = new AtomicReference<>(Collections.emptyMap());
    this.useSSL = context.getSslEngineFactory().isPresent();
  }

  @Override
  public CompletionStage<Void> init() {
    if (closed) {
      return CompletableFuture.completedFuture(null);
    }
    this.clientRoutesUpdateKey =
        context
            .getEventBus()
            .register(ClientRoutesUpdateEvent.class, this::onClientRoutesUpdateEvent);
    if (closed) {
      // init() raced with closeAsync(): unregister listeners we just added
      context.getEventBus().unregister(clientRoutesUpdateKey, ClientRoutesUpdateEvent.class);
      return CompletableFuture.completedFuture(null);
    }
    // First establish the control connection (super.init()), then pre-load client routes so that
    // buildNodeEndPoint (called during the subsequent refreshNodes) can use ClientRoutesEndPoint.
    // If the server does not support CLIENT_ROUTES_CHANGE, the control connection will fail with
    // a ConnectionInitException and the error propagates naturally.
    // Note: the control node's host_id is resolved by ControlConnection.connect() (via
    // fetchControlNodeHostId) so that InitialNodeListRefresh can match the contact point.
    return super.init()
        .thenCompose(ignored -> queryClientRoutesAndCache(null, null))
        .whenComplete(
            (result, error) -> {
              if (error != null) {
                context
                    .getEventBus()
                    .unregister(clientRoutesUpdateKey, ClientRoutesUpdateEvent.class);
              }
            });
  }

  /** Returns the {@link ClientRoutesConfig} this handler was built from. */
  @NonNull
  public ClientRoutesConfig getClientRoutesConfig() {
    return config;
  }

  @VisibleForTesting
  Map<UUID, ClientRouteRecord> getResolvedRoutes() {
    return resolvedRoutesCache.get();
  }

  @VisibleForTesting
  void setResolvedRoutes(Map<UUID, ClientRouteRecord> routes) {
    resolvedRoutesCache.set(Collections.unmodifiableMap(new HashMap<>(routes)));
  }

  @Nullable
  public InetSocketAddress resolve(@NonNull UUID hostId)
      throws IllegalStateException, UnknownHostException {
    if (closed) {
      throw new IllegalStateException("Topology monitor is closed");
    }
    ClientRouteRecord route = resolvedRoutesCache.get().get(hostId);
    if (route == null) {
      return null; // no client route for this node — caller falls back to default
    }

    return new InetSocketAddress(resolveAddress(route.getHostname()), route.getPort());
  }

  /**
   * Refreshes the client routes cache by querying the configured client routes table. Errors are
   * logged but do not propagate, so that periodic refreshes don't interrupt driver operation.
   *
   * <p>If another refresh is already in-flight, this request is queued and coalesced. The returned
   * future completes when <em>a</em> refresh finishes — not necessarily the one triggered by this
   * call. Callers must not assume the cache reflects their specific request upon completion.
   */
  public CompletionStage<Void> refresh() {
    if (closed) {
      return CompletableFuture.completedFuture(null);
    }
    return queryClientRoutesAndCache(null, null);
  }

  private CompletionStage<Void> queryClientRoutesAndCache(
      @Nullable List<String> eventConnectionIds, @Nullable List<String> eventHostIds) {
    CompletableFuture<Void> sentinel = new CompletableFuture<>();

    // Try to acquire the in-flight slot via CAS with exponential backoff.
    // The contention window is tiny (nanoseconds between CAS and get()), so this loop
    // almost never exceeds a single attempt. The backoff is a safety net to avoid
    // spinning under pathological contention.
    long backoffMs = BACKOFF_START_MS;
    for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
      if (inFlightRefresh.compareAndSet(null, sentinel)) {
        return executeRefresh(sentinel, eventConnectionIds, eventHostIds);
      }
      CompletionStage<Void> existing = inFlightRefresh.get();
      if (existing != null) {
        // Another refresh is in-flight — queue this request (coalescing with any pending one).
        RefreshRequest incoming = new RefreshRequest(eventConnectionIds, eventHostIds);
        queuedRefresh.getAndUpdate(q -> q == null ? incoming : q.coalesce(incoming));
        LOG.debug("[{}] Client routes refresh in progress, request queued", logPrefix);
        return existing;
      }
      // Slot was cleared between CAS and get(); back off before retrying.
      try {
        Thread.sleep(backoffMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.debug("[{}] Client routes refresh interrupted during backoff", logPrefix);
        return CompletableFuture.completedFuture(null);
      }
      backoffMs = Math.min(backoffMs * 2, BACKOFF_MAX_MS);
    }
    LOG.warn(
        "[{}] Client routes refresh dropped: CAS contention after {} attempts",
        logPrefix,
        MAX_CAS_ATTEMPTS);
    CompletionStage<Void> existing = inFlightRefresh.get();
    if (existing != null) {
      return existing;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Executes the actual CQL query and updates the routes cache. Called only after successfully
   * acquiring the in-flight slot.
   */
  private CompletionStage<Void> executeRefresh(
      CompletableFuture<Void> sentinel,
      @Nullable List<String> eventConnectionIds,
      @Nullable List<String> eventHostIds) {

    DriverChannel channel = context.getControlConnection().channel();
    if (channel == null) {
      LOG.warn("[{}] Control connection channel is null, cannot query client routes", logPrefix);
      completeAndDrain(sentinel);
      return sentinel;
    }

    String query = buildQuery(config, configuredConnectionIds, eventConnectionIds, eventHostIds);
    // A targeted refresh (host IDs known) merges into the existing cache rather than replacing it
    boolean isTargetedRefresh = eventHostIds != null && !eventHostIds.isEmpty();

    Duration timeout =
        context
            .getConfig()
            .getDefaultProfile()
            .getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);

    try {
      runAdminQuery(channel, query, timeout)
          .thenAccept(
              adminResult -> {
                Map<UUID, ClientRouteRecord> newRoutes = new HashMap<>();
                for (AdminRow row : adminResult) {
                  if (row.isNull("host_id") || row.isNull("address")) {
                    LOG.warn("[{}] Skipping incomplete client_routes row: {}", logPrefix, row);
                    continue;
                  }
                  UUID hostId = Objects.requireNonNull(row.getUuid("host_id"));
                  String address = Objects.requireNonNull(row.getString("address"));

                  // Select port based on SSL configuration at record creation time.
                  // Skip the record if the required port column is absent.
                  Integer effectivePort;
                  if (useSSL) {
                    effectivePort = row.isNull("tls_port") ? null : row.getInteger("tls_port");
                  } else {
                    effectivePort = row.isNull("port") ? null : row.getInteger("port");
                  }
                  if (effectivePort == null) {
                    LOG.error(
                        "[{}] Skipping client route for host_id={} ({}): "
                            + "required port column ({}) is not set in client routes table",
                        logPrefix,
                        hostId,
                        address,
                        useSSL ? "tls_port" : "port");
                    continue;
                  }

                  // Apply connectionAddr override if configured for this connection_id
                  String connId =
                      row.contains("connection_id") && !row.isNull("connection_id")
                          ? row.getString("connection_id")
                          : null;
                  if (connId != null) {
                    String override = connectionAddrOverrides.get(connId);
                    if (override != null) {
                      address = override;
                    }
                  }

                  newRoutes.put(hostId, new ClientRouteRecord(hostId, address, effectivePort));
                }

                if (isTargetedRefresh) {
                  // Merge: update only the returned host IDs, keep all others unchanged
                  mergeRoutes(newRoutes);
                  // Remove stale routes: host IDs present in the event but absent from query
                  // results have been deleted server-side (e.g. node decommission).
                  for (String hostIdStr : eventHostIds) {
                    UUID hostId;
                    try {
                      hostId = UUID.fromString(hostIdStr);
                    } catch (IllegalArgumentException e) {
                      LOG.warn(
                          "[{}] Skipping route with malformed host_id: {}",
                          logPrefix,
                          hostIdStr,
                          e);
                      continue;
                    }
                    if (!newRoutes.containsKey(hostId)) {
                      removeRoute(hostId);
                    }
                  }
                  LOG.debug(
                      "[{}] Merged {} client routes (targeted refresh)",
                      logPrefix,
                      newRoutes.size());
                } else if (newRoutes.isEmpty() && !resolvedRoutesCache.get().isEmpty()) {
                  int emptyCount = consecutiveEmptyResults.incrementAndGet();
                  if (emptyCount >= MAX_CONSECUTIVE_EMPTY_RESULTS) {
                    // Too many consecutive empties -- routes were likely removed server-side.
                    int staleSize = resolvedRoutesCache.get().size();
                    resolvedRoutesCache.set(Collections.emptyMap());
                    consecutiveEmptyResults.set(0);
                    LOG.warn(
                        "[{}] Client routes query returned 0 rows {} consecutive times; "
                            + "clearing {} stale cached routes",
                        logPrefix,
                        emptyCount,
                        staleSize);
                  } else {
                    // Guard against replacing a valid cache with empty results.
                    // This can happen when an async reconnect-triggered refresh queries a node
                    // that hasn't received the latest routes yet (eventual consistency).
                    LOG.debug(
                        "[{}] Client routes query returned 0 rows ({}/{}); "
                            + "keeping {} existing cached routes to avoid race condition",
                        logPrefix,
                        emptyCount,
                        MAX_CONSECUTIVE_EMPTY_RESULTS,
                        resolvedRoutesCache.get().size());
                  }
                } else {
                  consecutiveEmptyResults.set(0);
                  resolvedRoutesCache.set(Collections.unmodifiableMap(newRoutes));
                  LOG.debug(
                      "[{}] Updated client routes: {} routes loaded", logPrefix, newRoutes.size());
                }
              })
          .exceptionally(
              e -> {
                LOG.error("[{}] Failed to query client routes: {}", logPrefix, e.getMessage(), e);
                return null;
              })
          .whenComplete((v, t) -> completeAndDrain(sentinel));
      return sentinel;
    } catch (Exception e) {
      LOG.warn("[{}] Exception while querying client routes: {}", logPrefix, e.getMessage(), e);
      completeAndDrain(sentinel);
      return sentinel;
    }
  }

  /**
   * Completes the in-flight sentinel and drains any queued refresh request. The drain re-arms the
   * slot directly (no CAS needed — we still hold it), avoiding contention with new callers. Must be
   * called exactly once per acquired slot.
   */
  private void completeAndDrain(CompletableFuture<Void> sentinel) {
    RefreshRequest next = queuedRefresh.getAndSet(null);
    if (next != null && !closed) {
      // Re-arm the slot with a new sentinel before releasing the old one,
      // so no window exists where the slot is empty while a drain is pending.
      CompletableFuture<Void> nextSentinel = new CompletableFuture<>();
      inFlightRefresh.set(nextSentinel);
      sentinel.complete(null);
      LOG.debug("[{}] Draining queued client routes refresh", logPrefix);
      executeRefresh(nextSentinel, next.connectionIds, next.hostIds);
    } else {
      inFlightRefresh.set(null);
      sentinel.complete(null);
    }
  }

  private void onClientRoutesUpdateEvent(ClientRoutesUpdateEvent event) {
    if (closed) {
      return;
    }
    LOG.debug("[{}] Received {}, refreshing routes", logPrefix, event);
    queryClientRoutesAndCache(event.getConnectionIds(), event.getHostIds());
  }

  /**
   * Overrides the default port discovery to use the port from the client routes cache instead of
   * the control connection channel. When connecting through an NLB proxy, the channel port is the
   * proxy port, not the real server port. The {@code system.client_routes} table has the correct
   * port.
   *
   * <p>All nodes in a Scylla/Cassandra cluster use the same native transport port, so any route's
   * port is correct. We use the minimum host_id for deterministic selection.
   */
  @Override
  protected void savePort(DriverChannel channel) {
    if (port < 0) {
      Map<UUID, ClientRouteRecord> routes = resolvedRoutesCache.get();
      if (!routes.isEmpty()) {
        // Pick the route with the smallest host_id for deterministic behavior.
        UUID minId = null;
        ClientRouteRecord chosen = null;
        for (Map.Entry<UUID, ClientRouteRecord> entry : routes.entrySet()) {
          if (minId == null || entry.getKey().compareTo(minId) < 0) {
            minId = entry.getKey();
            chosen = entry.getValue();
          }
        }
        if (chosen != null && chosen.getPort() > 0) {
          port = chosen.getPort();
          return;
        }
      }
    }
    super.savePort(channel);
  }

  @NonNull
  @Override
  protected EndPoint buildNodeEndPoint(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    UUID hostId = row.getUuid("host_id");
    if (hostId == null) {
      LOG.warn(
          "[{}] host_id is null in system row for address {} — cannot assign a client route. "
              + "This may indicate corrupted system tables. "
              + "Falling back to default endpoint resolution.",
          logPrefix,
          broadcastRpcAddress);
      return super.buildNodeEndPoint(row, broadcastRpcAddress, localEndPoint);
    }
    EndPoint fallback = super.buildNodeEndPoint(row, broadcastRpcAddress, localEndPoint);
    InetAddress broadcastInetAddress = null;
    if (broadcastRpcAddress != null) {
      broadcastInetAddress = broadcastRpcAddress.getAddress();
    }
    if (broadcastInetAddress == null) {
      broadcastInetAddress = row.getInetAddress("broadcast_address");
    }
    if (broadcastInetAddress == null) {
      broadcastInetAddress = row.getInetAddress("peer");
    }
    return new ClientRoutesEndPoint(this, hostId, broadcastInetAddress, fallback);
  }

  /**
   * Builds the CQL query to fetch client routes.
   *
   * <ul>
   *   <li>Both connection IDs and host IDs present → {@code WHERE connection_id IN (...) AND
   *       host_id IN (...)} — no {@code ALLOW FILTERING} needed (both partition key components
   *       provided)
   *   <li>Connection IDs only → {@code WHERE connection_id IN (...) ALLOW FILTERING}; uses event
   *       connection IDs when present, otherwise falls back to all configured connection IDs
   *   <li>Neither → full scan with {@code ALLOW FILTERING} (should not occur in practice)
   * </ul>
   */
  @NonNull
  private static String buildQuery(
      @NonNull ClientRoutesConfig config,
      @NonNull List<String> configuredConnectionIds,
      @Nullable List<String> eventConnectionIds,
      @Nullable List<String> eventHostIds) {

    // Use event connection IDs when present, otherwise fall back to all configured IDs
    List<String> connectionIds =
        (eventConnectionIds != null && !eventConnectionIds.isEmpty())
            ? eventConnectionIds
            : configuredConnectionIds;

    boolean hasConnectionIds = !connectionIds.isEmpty();
    boolean hasHostIds = eventHostIds != null && !eventHostIds.isEmpty();

    StringBuilder stmt =
        new StringBuilder(String.format(SELECT_ROUTES_COLUMNS, config.getTableName()));

    if (hasConnectionIds) {
      // Prepared statements cannot be used here because AdminRequestHandler only supports
      // plain-text queries (it is designed for system-level queries that bypass the normal
      // request pipeline). Connection IDs are escaped by doubling single quotes.
      String quoted =
          connectionIds.stream()
              .map(ClientRoutesTopologyMonitor::cqlQuoteLiteral)
              .collect(Collectors.joining(", "));
      stmt.append(" WHERE connection_id IN (").append(quoted).append(")");
    }

    if (hasHostIds) {
      String validatedHostIds =
          eventHostIds.stream()
              .map(
                  id -> {
                    try {
                      return UUID.fromString(id).toString();
                    } catch (IllegalArgumentException e) {
                      throw new IllegalArgumentException(
                          "Invalid host ID (expected UUID format): " + id, e);
                    }
                  })
              .collect(Collectors.joining(", "));
      stmt.append(hasConnectionIds ? " AND" : " WHERE");
      stmt.append(" host_id IN (").append(validatedHostIds).append(")");
    }

    // ALLOW FILTERING is required unless both connection_id and host_id are provided
    // (matching gocql: isFullScan = len(hostIDs) == 0 || len(connectionIDs) == 0)
    boolean isFullScan = !hasHostIds || !hasConnectionIds;
    if (isFullScan) {
      stmt.append(" ALLOW FILTERING");
    }

    return stmt.toString();
  }

  /** Escapes a CQL string literal by doubling single quotes. */
  @NonNull
  private static String cqlQuoteLiteral(@NonNull String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  /**
   * Merges freshly-queried routes into the cache:
   *
   * <ul>
   *   <li>Route unchanged → keep existing entry as-is (no unnecessary churn)
   *   <li>Route changed (hostname or ports differ) → replace
   *   <li>Route not yet in cache → append
   * </ul>
   *
   * <p>The update is applied via a CAS loop. On collision the candidate is merged with the
   * concurrent winner before retrying, matching gocql's {@code MergeWithResolved} retry loop.
   */
  void mergeRoutes(Map<UUID, ClientRouteRecord> incoming) {
    Map<UUID, ClientRouteRecord> current;
    Map<UUID, ClientRouteRecord> candidate;
    // 10 retries is more than enough (matches gocql's ceiling)
    for (int attempt = 0; attempt < 10; attempt++) {
      current = resolvedRoutesCache.get();
      candidate = new HashMap<>(current);

      for (Map.Entry<UUID, ClientRouteRecord> entry : incoming.entrySet()) {
        UUID hostId = entry.getKey();
        ClientRouteRecord next = entry.getValue();
        ClientRouteRecord existing = candidate.get(hostId);

        if (existing == null) {
          // New route — append it
          candidate.put(hostId, next);
        } else if (!existing.equals(next)) {
          candidate.put(hostId, next);
        }
      }

      if (candidate.equals(current)) {
        return; // no changes — skip CAS
      }
      if (resolvedRoutesCache.compareAndSet(current, Collections.unmodifiableMap(candidate))) {
        return;
      }
      // CAS collision: another thread updated the cache concurrently.
      // Treat our candidate as the "incoming" side and merge it into whatever won,
      // then retry — matches gocql's MergeWithResolved loop.
      incoming = candidate;
    }
    LOG.warn("[{}] Failed to update client routes cache after 10 CAS attempts", logPrefix);
  }

  /**
   * Removes a single route from the cache (e.g. after a node decommission). Uses a CAS loop
   * identical to {@link #mergeRoutes}.
   */
  void removeRoute(UUID hostId) {
    for (int attempt = 0; attempt < 10; attempt++) {
      Map<UUID, ClientRouteRecord> current = resolvedRoutesCache.get();
      if (!current.containsKey(hostId)) {
        return; // already absent
      }
      Map<UUID, ClientRouteRecord> candidate = new HashMap<>(current);
      candidate.remove(hostId);
      if (resolvedRoutesCache.compareAndSet(current, Collections.unmodifiableMap(candidate))) {
        LOG.debug("[{}] Removed stale client route for host_id={}", logPrefix, hostId);
        return;
      }
    }
    LOG.warn(
        "[{}] Failed to remove client route for host_id={} after 10 CAS attempts",
        logPrefix,
        hostId);
  }

  /**
   * Executes a CQL admin query on the given channel. Extracted as a protected method so that unit
   * tests can override it to capture the query string and return stubbed results without touching
   * the network.
   */
  @NonNull
  protected CompletionStage<AdminResult> runAdminQuery(
      @NonNull DriverChannel channel, @NonNull String queryString, @NonNull Duration timeout) {
    return AdminRequestHandler.query(channel, queryString, timeout, NO_PAGING, logPrefix).start();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    closed = true;
    if (clientRoutesUpdateKey != null) {
      context.getEventBus().unregister(clientRoutesUpdateKey, ClientRoutesUpdateEvent.class);
    }
    LOG.debug("[{}] ClientRoutesTopologyMonitor closed", logPrefix);
    return super.closeAsync();
  }

  /**
   * Resolves a hostname to an {@link InetAddress}. Extracted as a protected method so that unit
   * tests can override it to return stubbed addresses without hitting the network.
   */
  @NonNull
  protected InetAddress resolveAddress(@NonNull String hostname) throws UnknownHostException {
    return InetAddress.getByName(hostname);
  }
}
