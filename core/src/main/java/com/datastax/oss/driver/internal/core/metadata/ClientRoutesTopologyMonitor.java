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

import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.clientroutes.CachingDnsResolver;
import com.datastax.oss.driver.internal.core.clientroutes.DnsResolver;
import com.datastax.oss.driver.internal.core.clientroutes.ResolvedClientRoute;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnectionReconnectEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ClientRoutesTopologyMonitor extends DefaultTopologyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesTopologyMonitor.class);

  private static final String SELECT_ROUTES_TEMPLATE =
      "SELECT connection_id, host_id, address, port, tls_port FROM %s"
          + " WHERE connection_id IN (%s) ALLOW FILTERING";

  final InternalDriverContext context;

  private final ClientRoutesConfig config;
  private final String logPrefix;
  final AtomicReference<Map<UUID, ResolvedClientRoute>> resolvedRoutesCache;
  private final DnsResolver dnsResolver;
  private final boolean useSSL;
  private volatile boolean closed = false;

  private volatile Object clientRoutesChangeKey;
  private volatile Object reconnectKey;

  public ClientRoutesTopologyMonitor(
      @NonNull InternalDriverContext context, @NonNull ClientRoutesConfig config) {
    super(context);
    this.context = context;
    this.config = config;
    this.logPrefix = context.getSessionName();
    this.resolvedRoutesCache = new AtomicReference<>(new HashMap<>());
    this.dnsResolver = new CachingDnsResolver(config.getDnsCacheDurationMillis());
    this.useSSL = context.getSslEngineFactory().isPresent();
  }

  @Override
  public CompletionStage<Void> init() {
    this.clientRoutesChangeKey =
        context
            .getEventBus()
            .register(ClientRoutesChangeEvent.class, this::onClientRoutesChangeEvent);
    this.reconnectKey =
        context
            .getEventBus()
            .register(ControlConnectionReconnectEvent.class, this::onReconnectEvent);
    return super.init();
  }

  /** Returns the {@link ClientRoutesConfig} this handler was built from. */
  @NonNull
  public ClientRoutesConfig getClientRoutesConfig() {
    return config;
  }

  @NonNull
  public InetSocketAddress resolve(@NonNull UUID hostId)
      throws IllegalStateException, UnknownHostException {
    if (closed) {
      throw new IllegalStateException("Topology monitor is closed");
    }
    ResolvedClientRoute route = resolvedRoutesCache.get().get(hostId);
    if (route == null) {
      throw new IllegalStateException(
          String.format("No client route found for host_id=%s", hostId));
    }

    // Select port based on SSL configuration
    Integer port;
    if (useSSL) {
      port = route.getNativeTransportPortSsl();
      if (port == null) {
        port = route.getNativeTransportPort();
        LOG.warn(
            "SSL requested for host_id={} ({}) but tls_port is not configured in client routes. "
                + "Falling back to non-SSL port {}. This may indicate a configuration issue.",
            hostId,
            route.getHostname(),
            port);
      }
    } else {
      port = route.getNativeTransportPort();
    }

    if (port == null) {
      throw new IllegalStateException(
          String.format(
              "No port configured for host_id=%s, hostname=%s. "
                  + "The system.client_routes table may be incomplete.",
              hostId, route.getHostname()));
    }

    return new InetSocketAddress(dnsResolver.resolve(route.getHostname()), port);
  }

  /**
   * Refreshes the client routes cache by querying the configured client routes table. Errors are
   * logged but do not propagate, so that periodic refreshes don't interrupt driver operation.
   */
  public CompletionStage<Void> refresh() {
    return queryAndResolveRoutes(false);
  }

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

    String connectionIdsCsv =
        endpoints.stream()
            .map(ClientRoutesEndpoint::getConnectionId)
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
                    Map<UUID, ResolvedClientRoute> newRoutes = new HashMap<>();
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
                      newRoutes.put(
                          hostId, new ResolvedClientRoute(hostId, address, port, tlsPort));
                    }
                    resolvedRoutesCache.set(newRoutes);
                    LOG.debug(
                        "[{}] Updated client routes: {} routes loaded",
                        logPrefix,
                        newRoutes.size());
                  });

      if (propagateErrors) {
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

  private void onClientRoutesChangeEvent(ClientRoutesChangeEvent event) {
    if (closed) {
      return;
    }
    LOG.debug("[{}] Received {}, refreshing routes", logPrefix, event);
    refresh();
  }

  private void onReconnectEvent(@SuppressWarnings("unused") ControlConnectionReconnectEvent event) {
    if (closed) {
      return;
    }
    LOG.debug("[{}] Control connection reconnected, refreshing routes", logPrefix);
    refresh();
  }

  @NonNull
  @Override
  protected EndPoint buildNodeEndPoint(
      @NonNull AdminRow row,
      @Nullable InetSocketAddress broadcastRpcAddress,
      @NonNull EndPoint localEndPoint) {
    UUID hostId = Objects.requireNonNull(row.getUuid("host_id"));
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
    return new ClientRoutesEndPoint(this, hostId, broadcastInetAddress);
  }

  @Override
  public void close() {
    closed = true;
    if (clientRoutesChangeKey != null) {
      context.getEventBus().unregister(clientRoutesChangeKey, ClientRoutesChangeEvent.class);
    }
    if (reconnectKey != null) {
      context.getEventBus().unregister(reconnectKey, ControlConnectionReconnectEvent.class);
    }
    dnsResolver.clearCache();
    LOG.debug("[{}] ClientRoutesTopologyMonitor closed", logPrefix);
  }
}
