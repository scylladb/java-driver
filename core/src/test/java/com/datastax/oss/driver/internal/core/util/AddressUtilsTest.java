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
package com.datastax.oss.driver.internal.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

public class AddressUtilsTest {

  @Test
  public void should_recognize_a_hostname_whether_resolved_or_not() {
    assertThat(
            AddressUtils.carriesName(InetSocketAddress.createUnresolved("host.example.com", 9042)))
        .isTrue();
    // Eagerly resolved by the constructor, but still a name.
    assertThat(AddressUtils.carriesName(new InetSocketAddress("localhost", 9042))).isTrue();
  }

  @Test
  public void should_not_mistake_an_ip_literal_for_a_hostname() throws Exception {
    assertThat(AddressUtils.carriesName(new InetSocketAddress("127.0.0.1", 9042))).isFalse();
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("127.0.0.1", 9042)))
        .isFalse();
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("::1", 9042))).isFalse();
    // Built from raw bytes, so it carries no name at all and getHostString() falls back to the
    // literal -- without triggering the reverse lookup that getHostName() would.
    assertThat(
            AddressUtils.carriesName(
                new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042)))
        .isFalse();
  }

  @Test
  public void should_report_a_bracketed_ipv6_literal_as_a_literal() {
    // The spelling extract() preserves: it splits a contact point on its *last* colon, so
    // "[2001:db8::5]:9042" yields the host string "[2001:db8::5]", brackets and all -- and
    // InetAddress.getAllByName() accepts that form, so the configuration works end to end.
    // Guava's isInetAddress() rejects brackets (only forUriString/isUriInetAddress take them), so
    // testing the bare form alone would call this a host name. That is not cosmetic: it would fire
    // DefaultEndPoint#equals's mixed unresolved/resolved warning and burn its once-per-JVM canary
    // on a message whose stated hazards are all false for a literal, and it would send
    // ChannelFactory#reattachHostname down the name-wins branch, relabelling even a
    // resolver-redirected candidate.
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("[2001:db8::5]", 9042)))
        .isFalse();
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("[::1]", 9042)))
        .isFalse();
    // Brackets and a zone together, which is what a link-local contact point looks like.
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("[fe80::1%eth0]", 9042)))
        .isFalse();
    // Still a name when what is inside the brackets is not an address -- the check must not
    // degenerate into "starts with a bracket".
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("[not.an.ip]", 9042)))
        .isTrue();
  }

  @Test
  public void should_report_a_scoped_ipv6_literal_as_a_literal() {
    // Guava's isInetAddress() accepts a zone suffix, so a scoped link-local literal is correctly
    // classified as a literal rather than as a name. Pinned because the opposite was once assumed:
    // ChannelFactory#reattachHostname takes its IP-literal branch on the strength of this, and that
    // branch has to cope with a zone it cannot parse.
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("fe80::1%eth0", 9042)))
        .isFalse();
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("fe80::1%1", 9042)))
        .isFalse();
    // A name that merely contains a '%' is still a name.
    assertThat(
            AddressUtils.carriesName(InetSocketAddress.createUnresolved("od%d.example.com", 9042)))
        .isTrue();
  }

  @Test
  public void should_report_an_explicitly_named_address_as_a_name() throws Exception {
    // A resolver may label its results with a name of its own; that is still a name.
    assertThat(
            AddressUtils.carriesName(
                new InetSocketAddress(
                    InetAddress.getByAddress("cname.example.com", new byte[] {10, 0, 0, 1}), 9042)))
        .isTrue();
  }

  @Test
  public void should_flip_for_a_resolved_literal_once_its_name_is_cached() throws Exception {
    // The documented instability, pinned so it is not rediscovered as a surprise: a *resolved*
    // address has no host string of its own. getHostString() renders InetAddress's cached hostName
    // field, which getHostName() fills in with a reverse-DNS name -- and DefaultSslEngineFactory
    // calls getHostName() while building an engine, on the very instance an endpoint holds. So the
    // same instance answers differently before and after the first TLS handshake to that node.
    InetSocketAddress resolvedLiteral = new InetSocketAddress("127.0.0.1", 9042);
    assertThat(AddressUtils.carriesName(resolvedLiteral)).isFalse();

    String reverseName = resolvedLiteral.getAddress().getHostName();
    assumeThat(reverseName)
        .isNotEqualTo("127.0.0.1"); // no PTR record for loopback: nothing to show

    assertThat(AddressUtils.carriesName(resolvedLiteral)).isTrue();
    // An unresolved address has no such field, which is why callers that need a stable answer
    // restrict themselves to those.
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("127.0.0.1", 9042)))
        .isFalse();
  }

  @Test
  public void should_strip_a_host_name_without_looking_anything_up() throws Exception {
    InetSocketAddress labelled =
        new InetSocketAddress(
            InetAddress.getByAddress("db.example.com", new byte[] {10, 0, 0, 2}), 9042);

    InetSocketAddress stripped = AddressUtils.stripHostName(labelled);

    assertThat(stripped).isNotNull();
    assertThat(stripped.getHostString()).isEqualTo("10.0.0.2");
    assertThat(stripped.getPort()).isEqualTo(9042);
    assertThat(stripped.getAddress()).isEqualTo(labelled.getAddress()); // same bytes
    assertThat(AddressUtils.carriesName(stripped)).isFalse();
  }

  @Test
  public void should_not_strip_an_unresolved_address() {
    // Nothing to strip: there are no bytes to rebuild from.
    assertThat(AddressUtils.stripHostName(InetSocketAddress.createUnresolved("host", 9042)))
        .isNull();
  }

  @Test
  public void should_not_invent_a_scope_for_an_unscoped_ipv6_address() throws Exception {
    // Inet6AddressHolder.init treats any scope_id >= 0 as zone-present, so handing it the 0 that an
    // unscoped address reports would render "...%0" -- which would reach node metric tags through
    // the endpoint's toString(), and would break carriesName's resolved branch.
    Inet6Address unscoped = (Inet6Address) InetAddress.getByName("2001:db8::5");
    assertThat(unscoped.getScopeId()).isZero();

    InetAddress relabelled = AddressUtils.withHostName("db.example.com", unscoped);

    assertThat(relabelled.getHostAddress()).isEqualTo("2001:db8:0:0:0:0:0:5");
    assertThat(relabelled.getHostName()).isEqualTo("db.example.com");
  }

  @Test
  public void should_keep_a_real_ipv6_scope_when_relabelling() throws Exception {
    byte[] linkLocal = new byte[16];
    linkLocal[0] = (byte) 0xfe;
    linkLocal[1] = (byte) 0x80;
    linkLocal[15] = 1;
    Inet6Address scoped = Inet6Address.getByAddress(null, linkLocal, 7);
    assertThat(scoped.getScopeId()).isEqualTo(7);

    InetAddress relabelled = AddressUtils.withHostName("db.example.com", scoped);

    assertThat(((Inet6Address) relabelled).getScopeId()).isEqualTo(7);
    assertThat(relabelled.getHostAddress()).endsWith("%7");
  }
}
