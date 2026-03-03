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

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Interface for DNS resolution with caching support.
 *
 * <p>Implementations should provide caching to avoid DNS storms during application startup and
 * should handle resolution failures gracefully (e.g., by returning the last known good IP).
 *
 * <p>This interface is part of the internal API and is not intended for public use.
 */
public interface DnsResolver {

  /**
   * Resolves a hostname to an IP address, potentially using a cache.
   *
   * <p>Implementations should:
   *
   * <ul>
   *   <li>Cache successful resolutions for a configurable duration (default: 500ms per
   *       Description.md)
   *   <li>Return cached results when available and not expired
   *   <li>On failure, return the last known good IP if available
   *   <li>Limit concurrency to avoid DNS storms (default: 1 concurrent resolution per hostname)
   * </ul>
   *
   * @param hostname the hostname to resolve (must not be null)
   * @return the resolved IP address
   * @throws UnknownHostException if the hostname cannot be resolved and no cached value is
   *     available
   */
  @NonNull
  InetAddress resolve(@NonNull String hostname) throws UnknownHostException;

  /**
   * Clears all cached DNS entries, but intentionally retains last-known-good addresses so that the
   * fallback mechanism continues to work after a cache flush.
   *
   * <p>This method is called exactly once, by {@code ClientRoutesHandler.close()}, when the session
   * is shut down. It is <strong>not</strong> called during route-map refreshes (triggered by {@code
   * CLIENT_ROUTES_CHANGE} events or control-connection reconnects); those refreshes re-query the
   * routes table but let the TTL-based DNS cache expire naturally.
   *
   * <p>This method is not intended for manual or administrative use outside of session close.
   */
  void clearCache();
}
