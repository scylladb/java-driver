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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CachingDnsResolver implements DnsResolver {
  private static final Logger LOG = LoggerFactory.getLogger(CachingDnsResolver.class);

  private final long cacheDurationNanos;
  /**
   * Tracks one {@link Semaphore} per hostname that is currently being resolved, together with a
   * reference count of how many threads are using it. The waiter count is incremented atomically
   * inside {@link ConcurrentHashMap#compute} before any thread blocks, and decremented (also inside
   * {@code compute}) after {@link Semaphore#release()}. Because {@code compute} holds the map's
   * internal bucket lock for that key, the decrement-and-conditional-remove is one indivisible
   * operation — closing the race that a plain {@code remove(key, value)} leaves open.
   */
  private final ConcurrentHashMap<String, SemaphoreEntry> semaphores = new ConcurrentHashMap<>();

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

    // Atomically get-or-create the entry and increment the waiter count in one compute() call.
    // This ensures the count is already > 0 before any other thread can observe the entry,
    // preventing a premature removal by a concurrently finishing thread.
    SemaphoreEntry semEntry =
        semaphores.compute(
            hostname,
            (k, existing) -> {
              if (existing == null) {
                existing = new SemaphoreEntry();
              }
              existing.waiters.incrementAndGet();
              return existing;
            });

    Semaphore semaphore = semEntry.semaphore;
    if (!semaphore.tryAcquire()) {
      // Another thread is already resolving this hostname. Block until it finishes,
      // then re-check the cache (the other thread will have populated it).
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Decrement the waiter count we incremented above before bailing out.
        semaphores.compute(
            hostname, (k, v) -> (v != null && v.waiters.decrementAndGet() == 0) ? null : v);
        throw new UnknownHostException(
            "Interrupted while waiting for DNS resolution of " + hostname);
      }
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
      // Atomically decrement the waiter count and remove the map entry when it reaches zero.
      // Using compute() here is essential: it holds the bucket lock while decrementing *and*
      // deciding whether to remove, so no other thread can slip in between the two operations.
      semaphores.compute(
          hostname, (k, v) -> (v != null && v.waiters.decrementAndGet() == 0) ? null : v);
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
    // Semaphore entries are self-cleaning: the waiter count reaches zero and the entry is removed
    // atomically inside compute() at the end of every resolution. Any entry that remains here
    // belongs to a resolution currently in flight and must not be discarded.
    //
    // lastKnownGood is intentionally NOT cleared here. clearCache() is only called at session
    // close; it is never called during a route-map refresh (CLIENT_ROUTES_CHANGE events or
    // control-connection reconnects). Retaining last-known-good means the fallback address
    // survives a close/reopen cycle and remains available if the first re-resolution fails.
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

  /**
   * Wraps a {@link Semaphore} with a reference count ({@code waiters}) that tracks how many threads
   * are currently using this entry. The count is managed exclusively through {@link
   * ConcurrentHashMap#compute}, which serialises increments and decrements on the same key.
   */
  static class SemaphoreEntry {
    final Semaphore semaphore = new Semaphore(1);
    final AtomicInteger waiters = new AtomicInteger(0);
  }

  /**
   * Returns the number of hostnames that currently have an active semaphore entry (i.e. have at
   * least one resolution in flight). Visible for testing only — callers must not depend on the
   * internal synchronisation mechanism or the map type.
   */
  @VisibleForTesting
  int semaphoreCount() {
    return semaphores.size();
  }
}
