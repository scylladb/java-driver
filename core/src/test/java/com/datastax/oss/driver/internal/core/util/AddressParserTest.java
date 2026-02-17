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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.UUID;
import org.junit.Test;

/** Tests for address parsing logic used in contact points and client routes configuration. */
public class AddressParserTest {

  private final UUID connectionId = UUID.randomUUID();

  @Test
  public void should_reject_null_address() {
    assertThatThrownBy(() -> AddressParser.parseContactPoint(null, connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address null")
        .hasMessageContaining("Address must not be null");
  }

  @Test
  public void should_reject_empty_address() {
    assertThatThrownBy(() -> AddressParser.parseContactPoint("", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address ''")
        .hasMessageContaining("Address must not be empty");
  }

  @Test
  public void should_reject_invalid_port_not_a_number() {
    assertThatThrownBy(() -> AddressParser.parseContactPoint("host:abc", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address 'host:abc'")
        .hasMessageContaining(connectionId.toString());
  }

  @Test
  public void should_reject_port_out_of_range_too_high() {
    assertThatThrownBy(() -> AddressParser.parseContactPoint("host:99999", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid port 99999")
        .hasMessageContaining("must be between 1 and 65535");
  }

  @Test
  public void should_reject_port_out_of_range_zero() {
    assertThatThrownBy(() -> AddressParser.parseContactPoint("host:0", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid port 0")
        .hasMessageContaining("must be between 1 and 65535");
  }

  @Test
  public void should_accept_valid_ipv4_with_port() {
    InetSocketAddress addr1 = AddressParser.parseContactPoint("host:9042", connectionId);
    assertThat(addr1.getHostString()).isEqualTo("host");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = AddressParser.parseContactPoint("192.168.1.1:9042", connectionId);
    assertThat(addr2.getHostString()).isEqualTo("192.168.1.1");
    assertThat(addr2.getPort()).isEqualTo(9042);

    InetSocketAddress addr3 = AddressParser.parseContactPoint("host:1", connectionId);
    assertThat(addr3.getPort()).isEqualTo(1);

    InetSocketAddress addr4 = AddressParser.parseContactPoint("host:65535", connectionId);
    assertThat(addr4.getPort()).isEqualTo(65535);
  }

  @Test
  public void should_accept_valid_ipv6_with_port() {
    InetSocketAddress addr1 = AddressParser.parseContactPoint("[::1]:9042", connectionId);
    // Java expands ::1 to its canonical form
    assertThat(addr1.getHostString()).matches("(\\[::1]|\\[0:0:0:0:0:0:0:1])");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = AddressParser.parseContactPoint("[2001:db8::1]:9042", connectionId);
    assertThat(addr2.getHostString()).contains("2001");
    assertThat(addr2.getHostString()).contains("db8");
    assertThat(addr2.getPort()).isEqualTo(9042);

    InetSocketAddress addr3 = AddressParser.parseContactPoint("[fe80::1]:19042", connectionId);
    assertThat(addr3.getHostString()).contains("fe80");
    assertThat(addr3.getPort()).isEqualTo(19042);
  }

  @Test
  public void should_accept_valid_ipv6_without_port() {
    // Should use default port 9042
    InetSocketAddress addr1 = AddressParser.parseContactPoint("[::1]", connectionId);
    // Java expands ::1 to its canonical form
    assertThat(addr1.getHostString()).matches("(\\[::1]|\\[0:0:0:0:0:0:0:1])");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = AddressParser.parseContactPoint("[2001:db8::1]", connectionId);
    assertThat(addr2.getHostString()).contains("2001");
    assertThat(addr2.getHostString()).contains("db8");
    assertThat(addr2.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_reject_bare_ipv6_without_brackets() {
    // URI parser will reject bare IPv6 addresses
    assertThatThrownBy(() -> AddressParser.parseContactPoint("::1", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address '::1'");

    assertThatThrownBy(() -> AddressParser.parseContactPoint("2001:db8::1", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address");
  }

  @Test
  public void should_handle_address_without_port() {
    // Should use default port 9042
    InetSocketAddress addr1 = AddressParser.parseContactPoint("host", connectionId);
    assertThat(addr1.getHostString()).isEqualTo("host");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 =
        AddressParser.parseContactPoint("my-cluster.scylladb.com", connectionId);
    assertThat(addr2.getHostString()).isEqualTo("my-cluster.scylladb.com");
    assertThat(addr2.getPort()).isEqualTo(9042);

    InetSocketAddress addr3 = AddressParser.parseContactPoint("192.168.1.1", connectionId);
    assertThat(addr3.getHostString()).isEqualTo("192.168.1.1");
    assertThat(addr3.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_handle_null_connection_id() {
    // When connection ID is null, error messages should still be clear
    assertThatThrownBy(() -> AddressParser.parseContactPoint("host:99999", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address 'host:99999'")
        .hasMessageContaining("Invalid port 99999");
  }
}
