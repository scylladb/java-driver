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
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CachingDnsResolver implements DnsResolver {
  private static final Logger LOG = LoggerFactory.getLogger(CachingDnsResolver.class);

  private final long cacheDurationNanos;
  private final ConcurrentHashMap<String, Semaphore> semaphores = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, InetAddress> lastKnownGood = new ConcurrentHashMap<>();
  private final ThrowingFunction<String, InetAddress> resolverFn;

  public CachingDnsResolver(long cacheDurationMillis) {
    this(cacheDurationMillis, InetAddress::getByName);
  }

  CachingDnsResolver(long cacheDurationMillis, ThrowingFunction<String, InetAddress> resolverFn) {
    this.cacheDurationNanos = cacheDurationMillis * 1_000_000L;
    this.resolverFn = resolverFn;
  }

  @Override
  @NonNull
  public InetAddress resolve(@NonNull String hostname) throws UnknownHostException {
    // Fast path: unlocked read — avoids semaphore overhead on a warm cache.
    CacheEntry entry = cachedEntry(hostname);
    if (entry != null) {
      return entry.address;
    }

    Semaphore semaphore = semaphores.computeIfAbsent(hostname, h -> new Semaphore(1));
    if (!semaphore.tryAcquire()) {
      // Another thread is already resolving this hostname. Block until it finishes,
      // then re-check the cache (the other thread will have populated it).
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UnknownHostException(
            "Interrupted while waiting for DNS resolution of " + hostname);
      }
      try {
        // Contended path: the resolver that held the semaphore just finished — cache hit expected.
        entry = cachedEntry(hostname);
        if (entry != null) {
          return entry.address;
        }
        // Cache still empty (e.g. the other thread failed); fall through to resolve ourselves.
        return doResolve(hostname);
      } finally {
        semaphore.release();
      }
    } else {
      try {
        // Double-checked locking: a concurrent thread may have resolved while we were acquiring.
        entry = cachedEntry(hostname);
        if (entry != null) {
          return entry.address;
        }
        return doResolve(hostname);
      } finally {
        semaphore.release();
      }
    }
  }

  /** Returns a non-expired {@link CacheEntry} for {@code hostname}, or {@code null}. */
  @Nullable
  private CacheEntry cachedEntry(String hostname) {
    CacheEntry entry = cache.get(hostname);
    return (entry != null && System.nanoTime() < entry.expiryNanos) ? entry : null;
  }

  /**
   * Performs a real DNS lookup, stores the result in the cache and {@code lastKnownGood}, and
   * returns the resolved address. Falls back to the last known good address on failure.
   */
  private InetAddress doResolve(String hostname) throws UnknownHostException {
    InetAddress address;
    try {
      address = resolverFn.apply(hostname);
    } catch (UnknownHostException e) {
      InetAddress fallback = lastKnownGood.get(hostname);
      if (fallback != null) {
        LOG.warn(
            "DNS resolution failed for {}, using last known good address {}", hostname, fallback);
        return fallback;
      }
      throw e;
    }
    cache.put(hostname, new CacheEntry(address, System.nanoTime() + cacheDurationNanos));
    lastKnownGood.put(hostname, address);
    return address;
  }

  @Override
  public void clearCache() {
    cache.clear();
    semaphores.clear();
    // lastKnownGood is retained for fallback
  }

  static class CacheEntry {
    final InetAddress address;
    final long expiryNanos;

    CacheEntry(InetAddress address, long expiryNanos) {
      this.address = address;
      this.expiryNanos = expiryNanos;
    }
  }

  @FunctionalInterface
  interface ThrowingFunction<T, R> {
    R apply(T t) throws UnknownHostException;
  }
}
