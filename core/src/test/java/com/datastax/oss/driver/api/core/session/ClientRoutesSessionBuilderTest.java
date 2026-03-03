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
package com.datastax.oss.driver.api.core.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.UUID;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.Test;

public class ClientRoutesSessionBuilderTest {

  // ---------------------------------------------------------------------------
  // SessionBuilder integration tests
  // ---------------------------------------------------------------------------

  @Test
  public void should_set_client_routes_config_programmatically() {
    UUID connectionId = UUID.randomUUID();
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(connectionId, "host:9042"))
            .build();

    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(config);

    assertThat(builder.clientRoutesConfig).isEqualTo(config);
    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig())
        .isEqualTo(config);
  }

  @Test
  public void should_allow_null_client_routes_config() {
    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(null);

    assertThat(builder.clientRoutesConfig).isNull();
    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig()).isNull();
  }

  // ---------------------------------------------------------------------------
  // parseContactPoint tests (logic moved from AddressParser into SessionBuilder)
  // ---------------------------------------------------------------------------

  @Test
  public void should_reject_null_address() {
    UUID connectionId = UUID.randomUUID();

    assertThatThrownBy(() -> parseContactPoint(null, connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address null")
        .hasMessageContaining("Address must not be null");
  }

  @Test
  public void should_reject_empty_address() {
    UUID connectionId = UUID.randomUUID();

    assertThatThrownBy(() -> parseContactPoint("", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address ''")
        .hasMessageContaining("Address must not be empty");
  }

  @Test
  public void should_reject_invalid_port_not_a_number() {
    UUID connectionId = UUID.randomUUID();

    assertThatThrownBy(() -> parseContactPoint("host:abc", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address 'host:abc'")
        .hasMessageContaining(connectionId.toString());
  }

  @Test
  public void should_reject_port_out_of_range_too_high() {
    UUID connectionId = UUID.randomUUID();

    assertThatThrownBy(() -> parseContactPoint("host:99999", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid port 99999")
        .hasMessageContaining("must be between 1 and 65535");
  }

  @Test
  public void should_reject_port_out_of_range_zero() {
    UUID connectionId = UUID.randomUUID();

    assertThatThrownBy(() -> parseContactPoint("host:0", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid port 0")
        .hasMessageContaining("must be between 1 and 65535");
  }

  @Test
  public void should_accept_valid_ipv4_with_port() throws Exception {
    UUID connectionId = UUID.randomUUID();

    InetSocketAddress addr1 = parseContactPoint("host:9042", connectionId);
    assertThat(addr1.getHostString()).isEqualTo("host");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = parseContactPoint("192.168.1.1:9042", connectionId);
    assertThat(addr2.getHostString()).isEqualTo("192.168.1.1");
    assertThat(addr2.getPort()).isEqualTo(9042);

    InetSocketAddress addr3 = parseContactPoint("host:1", connectionId);
    assertThat(addr3.getPort()).isEqualTo(1);

    InetSocketAddress addr4 = parseContactPoint("host:65535", connectionId);
    assertThat(addr4.getPort()).isEqualTo(65535);
  }

  @Test
  public void should_accept_valid_ipv6_with_port() throws Exception {
    UUID connectionId = UUID.randomUUID();

    InetSocketAddress addr1 = parseContactPoint("[::1]:9042", connectionId);
    // Java expands ::1 to its canonical form
    assertThat(addr1.getHostString()).matches("(\\[::1]|\\[0:0:0:0:0:0:0:1])");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = parseContactPoint("[2001:db8::1]:9042", connectionId);
    assertThat(addr2.getHostString()).contains("2001");
    assertThat(addr2.getHostString()).contains("db8");
    assertThat(addr2.getPort()).isEqualTo(9042);

    InetSocketAddress addr3 = parseContactPoint("[fe80::1]:19042", connectionId);
    assertThat(addr3.getHostString()).contains("fe80");
    assertThat(addr3.getPort()).isEqualTo(19042);
  }

  @Test
  public void should_accept_valid_ipv6_without_port() throws Exception {
    UUID connectionId = UUID.randomUUID();

    // Should use default port 9042
    InetSocketAddress addr1 = parseContactPoint("[::1]", connectionId);
    // Java expands ::1 to its canonical form
    assertThat(addr1.getHostString()).matches("(\\[::1]|\\[0:0:0:0:0:0:0:1])");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = parseContactPoint("[2001:db8::1]", connectionId);
    assertThat(addr2.getHostString()).contains("2001");
    assertThat(addr2.getHostString()).contains("db8");
    assertThat(addr2.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_reject_bare_ipv6_without_brackets() {
    UUID connectionId = UUID.randomUUID();

    // URI parser will reject bare IPv6 addresses
    assertThatThrownBy(() -> parseContactPoint("::1", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address '::1'");

    assertThatThrownBy(() -> parseContactPoint("2001:db8::1", connectionId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address");
  }

  @Test
  public void should_handle_address_without_port() throws Exception {
    UUID connectionId = UUID.randomUUID();

    // Should use default port 9042
    InetSocketAddress addr1 = parseContactPoint("host", connectionId);
    assertThat(addr1.getHostString()).isEqualTo("host");
    assertThat(addr1.getPort()).isEqualTo(9042);

    InetSocketAddress addr2 = parseContactPoint("my-cluster.scylladb.com", connectionId);
    assertThat(addr2.getHostString()).isEqualTo("my-cluster.scylladb.com");
    assertThat(addr2.getPort()).isEqualTo(9042);

    InetSocketAddress addr3 = parseContactPoint("192.168.1.1", connectionId);
    assertThat(addr3.getHostString()).isEqualTo("192.168.1.1");
    assertThat(addr3.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_handle_null_connection_id() {
    // When connection ID is null, error messages should still be clear
    assertThatThrownBy(() -> parseContactPoint("host:99999", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse address 'host:99999'")
        .hasMessageContaining("Invalid port 99999");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Reflectively invokes the private static {@code parseContactPoint} method on {@link
   * SessionBuilder}, unwrapping any {@link InvocationTargetException} so that assertion helpers see
   * the real exception type.
   */
  private static InetSocketAddress parseContactPoint(String address, UUID connectionId)
      throws Exception {
    try {
      Method m =
          SessionBuilder.class.getDeclaredMethod("parseContactPoint", String.class, UUID.class);
      m.setAccessible(true);
      return (InetSocketAddress) m.invoke(null, address, connectionId);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw e;
    }
  }

  /** Test subclass to access protected fields. */
  private static class TestSessionBuilder extends SessionBuilder<TestSessionBuilder, CqlSession> {
    @Override
    protected CqlSession wrap(@NonNull CqlSession defaultSession) {
      return mock(CqlSession.class);
    }
  }
}
