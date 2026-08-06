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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import io.netty.channel.local.LocalAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

public class DefaultEndPointTest {

  @Test
  public void should_create_from_host_name() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));
    assertThat(endPoint.asMetricPrefix()).isEqualTo("localhost:9042");
  }

  @Test
  public void should_create_from_literal_ipv4_address() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    assertThat(endPoint.asMetricPrefix()).isEqualTo("127_0_0_1:9042");
  }

  @Test
  public void should_create_from_literal_ipv6_address() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("::1", 9042));
    assertThat(endPoint.asMetricPrefix()).isEqualTo("0:0:0:0:0:0:0:1:9042");
  }

  @Test
  public void should_create_from_unresolved_address() {
    InetSocketAddress address = InetSocketAddress.createUnresolved("test.com", 9042);
    DefaultEndPoint endPoint = new DefaultEndPoint(address);
    assertThat(endPoint.asMetricPrefix()).isEqualTo("test_com:9042");
    assertThat(address.isUnresolved()).isTrue();
  }

  @Test
  public void should_reject_null_address() {
    assertThatThrownBy(() -> new DefaultEndPoint(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("address can't be null");
  }

  @Test
  public void resolve_returns_already_resolved_address_as_is() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    InetSocketAddress resolved = endPoint.resolve();
    assertThat(resolved.isUnresolved()).isFalse();
    assertThat(resolved.getHostString()).isEqualTo("127.0.0.1");
  }

  @Test
  public void resolve_passes_unresolved_hostname_through_without_looking_it_up() {
    // This endpoint does NOT resolve hostnames itself. It hands the unresolved address to
    // ChannelFactory, which expands it through Netty's AddressResolverGroup so that a custom
    // resolver installed via NettyOptions#afterBootstrapInitialized still applies -- a direct
    // InetAddress.getAllByName() call here would bypass it, and would block the admin event loop
    // that connect() runs on. "localhost" would resolve fine, so this assertion is only meaningful
    // because we check the address comes back *unresolved*.
    DefaultEndPoint endPoint =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 9042));

    InetSocketAddress resolved = endPoint.resolve();

    assertThat(resolved.isUnresolved()).isTrue();
    assertThat(resolved.getHostString()).isEqualTo("localhost");
    assertThat(resolved.getPort()).isEqualTo(9042);
  }

  @Test
  public void resolve_does_not_throw_for_unresolvable_hostname() {
    // No lookup happens, so an unresolvable name is not an error at this level: the connect attempt
    // fails later with a descriptive error instead.
    DefaultEndPoint endPoint =
        new DefaultEndPoint(
            InetSocketAddress.createUnresolved("this-host-does-not-exist.invalid", 9042));

    assertThat(endPoint.resolve().getHostString()).isEqualTo("this-host-does-not-exist.invalid");
  }

  @Test
  public void pin_to_should_override_resolution_but_preserve_identity() {
    InetSocketAddress hostname = InetSocketAddress.createUnresolved("test.com", 9042);
    DefaultEndPoint original = new DefaultEndPoint(hostname);
    InetSocketAddress pinnedTo = new InetSocketAddress("127.0.0.1", 9042);

    EndPoint pinned = original.pinTo(pinnedTo);

    // Resolution now yields exactly the pinned address...
    assertThat(pinned.resolve()).isEqualTo(pinnedTo);
    // ...but the copy still denotes the same node, and metric names must not change depending on
    // which IP a connection happened to land on -- including through toString(), which is what
    // TaggingMetricIdGenerator tags node metrics with, and nodes do adopt pinned copies.
    assertThat(pinned.asMetricPrefix()).isEqualTo(original.asMetricPrefix());
    assertThat(pinned.toString()).isEqualTo(original.toString());
    assertThat(pinned).isEqualTo(original);
    assertThat(pinned.hashCode()).isEqualTo(original.hashCode());
    // Equality has to hold in both directions: endpoints are used as set and map keys.
    assertThat(original).isEqualTo(pinned);
    // The original is untouched.
    assertThat(original.resolve()).isEqualTo(hostname);
  }

  @Test
  public void pin_to_should_return_same_instance_when_already_pinned_to_that_address() {
    DefaultEndPoint original =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("test.com", 9042));
    InetSocketAddress pinnedTo = new InetSocketAddress("127.0.0.1", 9042);

    EndPoint pinned = original.pinTo(pinnedTo);

    assertThat(((DefaultEndPoint) pinned).pinTo(pinnedTo)).isSameAs(pinned);
  }

  @Test
  public void pin_to_should_return_same_instance_when_address_is_already_the_endpoints_own() {
    // An already-resolved endpoint expands to exactly one candidate -- itself -- so ChannelFactory
    // pins it to the address it already holds. That copy would be indistinguishable from the
    // original in every respect, so there is no point allocating it. Every node discovered from the
    // peers rows takes this path.
    InetSocketAddress resolved = new InetSocketAddress("127.0.0.1", 9042);
    DefaultEndPoint endPoint = new DefaultEndPoint(resolved);

    assertThat(endPoint.pinTo(new InetSocketAddress("127.0.0.1", 9042))).isSameAs(endPoint);
    assertThat(endPoint.toString()).isEqualTo(resolved.toString());
  }

  @Test
  public void pin_to_should_reject_null_address() {
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    assertThatThrownBy(() -> endPoint.pinTo(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("resolvedAddress can't be null");
  }

  @Test
  public void pin_to_should_be_a_no_op_for_a_non_inet_address() {
    // ChannelFactory pins whatever address it connected to; a non-Inet one (e.g. the local
    // transport
    // used by unit tests) cannot be held in an InetSocketAddress field, so pinning is skipped
    // rather
    // than failing the connection.
    DefaultEndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    assertThat(endPoint.pinTo(new LocalAddress("some-id"))).isSameAs(endPoint);
  }

  @Test
  public void pin_to_should_be_a_no_op_for_an_unresolved_address() {
    // resolveCandidates() hands the original address straight through when the user disabled
    // Netty's
    // resolver, when the resolver does not support the address, or when it reports it as already
    // resolved. Pinning a name would freeze resolve() on something that must re-expand on every
    // connect -- no address stability gained, and the endpoint's own source silenced. The same
    // guard
    // is in SniEndPoint and ClientRoutesEndPoint.
    DefaultEndPoint endPoint =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("db.example.com", 9042));

    assertThat(endPoint.pinTo(InetSocketAddress.createUnresolved("db.example.com", 9042)))
        .isSameAs(endPoint);
  }

  @Test
  public void should_still_compare_an_unresolved_address_against_a_resolved_one() {
    // Kept working for Metadata#findNode, whose caller may hold either form -- but it costs a DNS
    // lookup on the calling thread, only compares the first address the name maps to, and does not
    // agree with hashCode(). equals() warns once per JVM about it; that latch is static, so whether
    // this test is the one that trips it depends on test ordering and is deliberately not asserted.
    // See https://github.com/scylladb/java-driver/issues/1006.
    DefaultEndPoint unresolved =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 9042));
    DefaultEndPoint resolved = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));

    assertThat(unresolved).isEqualTo(resolved);
    assertThat(resolved).isEqualTo(unresolved);
    // ... while hashing differently, which is exactly the inconsistency the issue tracks.
    assertThat(unresolved.hashCode()).isNotEqualTo(resolved.hashCode());
  }

  @Test
  public void should_not_warn_when_comparing_an_ip_literal_against_a_resolved_address() {
    // Contact points are stored unresolved whatever form they were written in -- AddressUtils
    // .extract is always called with resolve = false -- so a plain "1.2.3.4:9042" meets a resolved
    // peer endpoint on every comparison. That is the most ordinary deployment there is, and none of
    // the three hazards the warning names applies to it: re-building an IP literal parses it with
    // no resolver call, keeps the only address it can denote, and agrees with hashCode().
    DefaultEndPoint.LOGGED_MIXED_COMPARISON_WARNING.set(false);
    LoggerTest.LoggerSetup logger = LoggerTest.setupTestLogger(DefaultEndPoint.class, Level.WARN);
    try {
      DefaultEndPoint literal =
          new DefaultEndPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
      DefaultEndPoint resolved = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));

      assertThat(literal).isEqualTo(resolved);
      assertThat(resolved).isEqualTo(literal);

      verify(logger.appender, never()).doAppend(any());
    } finally {
      logger.close();
    }
  }

  @Test
  public void should_warn_when_comparing_a_host_name_against_a_resolved_address() {
    // The counterpart: a *name* on the unresolved side does cost a lookup on the calling thread and
    // does only compare the first address it maps to, so the warning still has to fire there.
    DefaultEndPoint.LOGGED_MIXED_COMPARISON_WARNING.set(false);
    LoggerTest.LoggerSetup logger = LoggerTest.setupTestLogger(DefaultEndPoint.class, Level.WARN);
    try {
      DefaultEndPoint name =
          new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 9042));
      DefaultEndPoint resolved = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));

      assertThat(name).isEqualTo(resolved);

      verify(logger.appender, times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains("Compared an unresolved host name against a resolved endpoint address");
    } finally {
      logger.close();
    }
  }
}
