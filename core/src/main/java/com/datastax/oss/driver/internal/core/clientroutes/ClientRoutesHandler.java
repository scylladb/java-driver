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
package com.datastax.oss.driver.internal.core.clientroutes;

import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ClientRoutesHandler implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesHandler.class);

  private static final String SELECT_ROUTES_TEMPLATE =
      "SELECT connection_id, host_id, address, port, tls_port FROM %s"
          + " WHERE connection_id IN (%s) ALLOW FILTERING";

  final InternalDriverContext context;

  private final ClientRoutesConfig config;
  private final String logPrefix;
  final AtomicReference<Map<UUID, ResolvedClientRoute>> resolvedRoutesRef;
  private final DnsResolver dnsResolver;
  private volatile boolean closed = false;

  public ClientRoutesHandler(
      @NonNull InternalDriverContext context, @NonNull ClientRoutesConfig config) {
    this.context = context;
    this.config = config;
    this.logPrefix = context.getSessionName();
    this.resolvedRoutesRef = new AtomicReference<>(new ConcurrentHashMap<>());
    this.dnsResolver = createDnsResolver(config.getDnsCacheDurationMillis());
  }

  /** Creates a DNS resolver with the specified cache duration. */
  private DnsResolver createDnsResolver(long dnsCacheDurationMillis) {
    return new CachingDnsResolver(dnsCacheDurationMillis);
  }

  public CompletionStage<Void> init() {
    LOG.debug(
        "[{}] Initializing ClientRoutesHandler with {} endpoints",
        logPrefix,
        config.getEndpoints().size());
    // Propagate failures so callers can detect unsupported servers or configuration problems.
    return queryAndResolveRoutes(/* propagateErrors= */ true);
  }

  public CompletionStage<Void> refresh() {
    LOG.debug("[{}] Refreshing client routes", logPrefix);
    // Refresh failures are non-fatal: log a warning and keep the previous route map.
    return queryAndResolveRoutes(/* propagateErrors= */ false);
  }

  /**
   * Queries the configured client-routes table and updates the in-memory route map.
   *
   * @param propagateErrors {@code true} to let query errors propagate to the caller (used during
   *     {@link #init()} so session startup can detect missing tables); {@code false} to catch all
   *     errors and log a warning (used during {@link #refresh()} where continuity matters more).
   */
  private CompletionStage<Void> queryAndResolveRoutes(boolean propagateErrors) {
    DriverChannel channel = context.getControlConnection().channel();
    if (channel == null) {
      LOG.warn("[{}] Control connection channel is null, cannot query client routes", logPrefix);
      return CompletableFuture.completedFuture(null);
    }

    List<ClientRoutesEndpoint> endpoints = config.getEndpoints();
    if (endpoints.isEmpty()) {
      LOG.warn("[{}] No endpoints configured for client routes", logPrefix);
      return CompletableFuture.completedFuture(null);
    }

    // Build the IN clause with literal UUID values — AdminRequestHandler does not support
    // List<UUID> as a named parameter, so we inline the values directly.
    String connectionIdsCsv =
        endpoints.stream()
            .map(ep -> ep.getConnectionId().toString())
            .collect(Collectors.joining(", "));
    String query = String.format(SELECT_ROUTES_TEMPLATE, config.getTableName(), connectionIdsCsv);

    Duration timeout =
        context
            .getConfig()
            .getDefaultProfile()
            .getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);

    try {
      CompletionStage<Void> future =
          AdminRequestHandler.query(channel, query, timeout, Integer.MAX_VALUE, logPrefix)
              .start()
              .thenAccept(
                  adminResult -> {
                    Map<UUID, ResolvedClientRoute> newRoutes = new ConcurrentHashMap<>();
                    for (AdminRow row : adminResult) {
                      if (row.isNull("host_id") || row.isNull("address") || row.isNull("port")) {
                        LOG.warn("[{}] Skipping incomplete client_routes row: {}", logPrefix, row);
                        continue;
                      }
                      UUID hostId = row.getUuid("host_id");
                      String address = row.getString("address");
                      Integer port = row.getInteger("port");
                      Integer tlsPort =
                          row.contains("tls_port") && !row.isNull("tls_port")
                              ? row.getInteger("tls_port")
                              : null;
                      //noinspection DataFlowIssue
                      newRoutes.put(
                          hostId, new ResolvedClientRoute(hostId, address, port, tlsPort));
                    }
                    resolvedRoutesRef.set(newRoutes);
                    LOG.debug(
                        "[{}] Updated client routes: {} routes loaded",
                        logPrefix,
                        newRoutes.size());
                  });

      if (propagateErrors) {
        // Let failures propagate so that init() callers (e.g. session startup) can detect
        // missing tables or configuration problems rather than silently succeeding.
        return future;
      } else {
        return future.exceptionally(
            e -> {
              LOG.warn("[{}] Failed to query client routes: {}", logPrefix, e.getMessage(), e);
              return null;
            });
      }
    } catch (Exception e) {
      LOG.warn("[{}] Exception while querying client routes: {}", logPrefix, e.getMessage(), e);
      if (propagateErrors) {
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(e);
        return failed;
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  @Nullable
  public InetSocketAddress translate(@NonNull UUID hostId, boolean useSsl) {
    if (closed) {
      return null;
    }
    Map<UUID, ResolvedClientRoute> routes = resolvedRoutesRef.get();
    ResolvedClientRoute route = routes.get(hostId);
    if (route == null) {
      LOG.debug("[{}] No client route found for host_id={}", logPrefix, hostId);
      return null;
    }

    try {
      // DNS resolution happens here through the cached resolver
      return route.toSocketAddress(useSsl, dnsResolver);
    } catch (UnknownHostException e) {
      LOG.warn(
          "[{}] Failed to resolve hostname {} for host_id={}",
          logPrefix,
          route.getHostname(),
          hostId,
          e);
      return null;
    } catch (IllegalStateException e) {
      LOG.warn(
          "[{}] Invalid route configuration for host_id={}: {}", logPrefix, hostId, e.getMessage());
      return null;
    }
  }

  @Override
  public void close() {
    closed = true;
    dnsResolver.clearCache();
    LOG.debug("[{}] ClientRoutesHandler closed", logPrefix);
  }
}
