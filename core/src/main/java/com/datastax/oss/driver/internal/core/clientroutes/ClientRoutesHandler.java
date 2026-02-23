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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ClientRoutesHandler implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesHandler.class);

  @SuppressWarnings("UnusedVariable") // Will be used for querying system.client_routes
  private final InternalDriverContext context;

  private final ClientRoutesConfig config;
  private final String logPrefix;
  private final AtomicReference<Map<UUID, ResolvedClientRoute>> resolvedRoutesRef;
  private final Map<String, DnsCacheEntry> dnsCache;
  private final long dnsCacheDurationNanos;
  private volatile boolean closed = false;

  public ClientRoutesHandler(
      @NonNull InternalDriverContext context, @NonNull ClientRoutesConfig config) {
    this.context = context;
    this.config = config;
    this.logPrefix = context.getSessionName();
    this.resolvedRoutesRef = new AtomicReference<>(new ConcurrentHashMap<>());
    this.dnsCache = new ConcurrentHashMap<>();
    this.dnsCacheDurationNanos = config.getDnsCacheDurationMillis() * 1_000_000L;
  }

  public CompletionStage<Void> init() {
    LOG.debug(
        "[{}] Initializing ClientRoutesHandler with {} endpoints",
        logPrefix,
        config.getEndpoints().size());
    return queryAndResolveRoutes();
  }

  public CompletionStage<Void> refresh() {
    LOG.debug("[{}] Refreshing client routes", logPrefix);
    return queryAndResolveRoutes();
  }

  private CompletionStage<Void> queryAndResolveRoutes() {
    // TODO: Query system.client_routes table
    // For now, return completed future
    return CompletableFuture.completedFuture(null);
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
    return route.toSocketAddress(useSsl);
  }

  @SuppressWarnings("UnusedMethod") // Will be used when implementing system.client_routes query
  private InetAddress resolveDns(String hostname) throws UnknownHostException {
    DnsCacheEntry cached = dnsCache.get(hostname);
    long now = System.nanoTime();
    if (cached != null && (now - cached.resolvedAtNanos) < dnsCacheDurationNanos) {
      return cached.address;
    }
    InetAddress resolved = InetAddress.getByName(hostname);
    dnsCache.put(hostname, new DnsCacheEntry(resolved, now));
    return resolved;
  }

  @Override
  public void close() {
    closed = true;
    dnsCache.clear();
    LOG.debug("[{}] ClientRoutesHandler closed", logPrefix);
  }

  private static class DnsCacheEntry {
    final InetAddress address;
    final long resolvedAtNanos;

    DnsCacheEntry(InetAddress address, long resolvedAtNanos) {
      this.address = address;
      this.resolvedAtNanos = resolvedAtNanos;
    }
  }
}
