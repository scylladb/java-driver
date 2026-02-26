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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class CachingDnsResolverTest {

  private static final InetAddress ADDR_1;
  private static final InetAddress ADDR_2;

  static {
    try {
      ADDR_1 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
      ADDR_2 = InetAddress.getByAddress(new byte[] {127, 0, 0, 2});
    } catch (UnknownHostException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @Test
  public void should_return_cached_address_within_ttl() throws UnknownHostException {
    AtomicInteger callCount = new AtomicInteger(0);
    CachingDnsResolver resolver =
        new CachingDnsResolver(
            10_000,
            hostname -> {
              callCount.incrementAndGet();
              return ADDR_1;
            });

    InetAddress first = resolver.resolve("host1");
    InetAddress second = resolver.resolve("host1");

    assertThat(first).isEqualTo(ADDR_1);
    assertThat(second).isEqualTo(ADDR_1);
    assertThat(callCount.get()).isEqualTo(1);
  }

  @Test
  public void should_re_resolve_after_ttl_expires() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);
    // 0ms TTL means cache entries are immediately stale
    CachingDnsResolver resolver =
        new CachingDnsResolver(
            0,
            hostname -> {
              callCount.incrementAndGet();
              return ADDR_1;
            });

    resolver.resolve("host1");
    Thread.sleep(5); // ensure expiry
    resolver.resolve("host1");

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  public void should_resolve_different_hostnames_independently() throws UnknownHostException {
    CachingDnsResolver resolver =
        new CachingDnsResolver(10_000, hostname -> hostname.equals("host1") ? ADDR_1 : ADDR_2);

    assertThat(resolver.resolve("host1")).isEqualTo(ADDR_1);
    assertThat(resolver.resolve("host2")).isEqualTo(ADDR_2);
    assertThat(resolver.resolve("host1")).isEqualTo(ADDR_1);
  }

  @Test
  public void should_fall_back_to_last_known_good_on_failure() throws Exception {
    AtomicBoolean shouldFail = new AtomicBoolean(false);
    CachingDnsResolver resolver =
        new CachingDnsResolver(
            0,
            hostname -> {
              if (shouldFail.get()) {
                throw new UnknownHostException("simulated failure");
              }
              return ADDR_1;
            });

    // Populate last-known-good
    assertThat(resolver.resolve("host1")).isEqualTo(ADDR_1);

    // Expire cache and make resolution fail
    Thread.sleep(5);
    shouldFail.set(true);

    // Should return last-known-good instead of throwing
    assertThat(resolver.resolve("host1")).isEqualTo(ADDR_1);
  }

  @Test
  public void should_throw_when_no_fallback_available() {
    CachingDnsResolver resolver =
        new CachingDnsResolver(
            10_000,
            hostname -> {
              throw new UnknownHostException("always fails");
            });

    assertThatThrownBy(() -> resolver.resolve("unknown-host"))
        .isInstanceOf(UnknownHostException.class);
  }

  @Test
  public void should_clear_cache_on_clearCache() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);
    CachingDnsResolver resolver =
        new CachingDnsResolver(
            10_000,
            hostname -> {
              callCount.incrementAndGet();
              return ADDR_1;
            });

    resolver.resolve("host1");
    assertThat(callCount.get()).isEqualTo(1);

    resolver.clearCache();
    resolver.resolve("host1");

    assertThat(callCount.get()).isEqualTo(2);
  }

  @Test
  public void should_retain_last_known_good_after_clearCache() throws Exception {
    AtomicBoolean shouldFail = new AtomicBoolean(false);
    CachingDnsResolver resolver =
        new CachingDnsResolver(
            10_000,
            hostname -> {
              if (shouldFail.get()) {
                throw new UnknownHostException("simulated failure");
              }
              return ADDR_1;
            });

    resolver.resolve("host1"); // populate last-known-good
    resolver.clearCache(); // clear cache but not last-known-good
    shouldFail.set(true);

    // Should still fall back to last-known-good after cache clear
    assertThat(resolver.resolve("host1")).isEqualTo(ADDR_1);
  }
}
