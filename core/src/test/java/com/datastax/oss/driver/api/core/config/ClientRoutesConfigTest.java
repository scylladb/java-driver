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
package com.datastax.oss.driver.api.core.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class ClientRoutesConfigTest {

  @Test
  public void should_build_config_with_single_endpoint() {
    String connectionId = "conn-id-1";
    String connectionAddr = "my-privatelink.us-east-1.aws.scylladb.com";

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, connectionAddr))
            .build();

    assertThat(config.getEndpoints()).hasSize(1);
    assertThat(config.getEndpoints().get(0).getConnectionId()).isEqualTo(connectionId);
    assertThat(config.getEndpoints().get(0).getConnectionAddr()).isEqualTo(connectionAddr);
    assertThat(config.getTableName()).isEqualTo("system.client_routes");
  }

  @Test
  public void should_build_config_with_multiple_endpoints() {
    String connectionId1 = "conn-id-1";
    String connectionId2 = "conn-id-2";

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId1, "host1"))
            .addEndpoint(new ClientRouteProxy(connectionId2, "host2"))
            .build();

    assertThat(config.getEndpoints()).hasSize(2);
    assertThat(config.getEndpoints().get(0).getConnectionId()).isEqualTo(connectionId1);
    assertThat(config.getEndpoints().get(1).getConnectionId()).isEqualTo(connectionId2);
  }

  @Test
  public void should_build_config_with_custom_table_name() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1"))
            .withTableName("custom.client_routes_test")
            .build();

    assertThat(config.getTableName()).isEqualTo("custom.client_routes_test");
  }

  @Test
  public void should_fail_when_no_endpoints_provided() {
    assertThatThrownBy(() -> ClientRoutesConfig.builder().build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("At least one endpoint must be specified");
  }

  @Test
  public void should_create_endpoint_without_connection_address() {
    String connectionId = "conn-id-1";
    ClientRouteProxy endpoint = new ClientRouteProxy(connectionId);

    assertThat(endpoint.getConnectionId()).isEqualTo(connectionId);
    assertThat(endpoint.getConnectionAddr()).isNull();
  }

  @Test
  public void should_create_endpoint_with_connection_address() {
    String connectionId = "conn-id-1";
    String connectionAddr = "host";
    ClientRouteProxy endpoint = new ClientRouteProxy(connectionId, connectionAddr);

    assertThat(endpoint.getConnectionId()).isEqualTo(connectionId);
    assertThat(endpoint.getConnectionAddr()).isEqualTo(connectionAddr);
  }

  @Test
  public void should_fail_when_connection_id_is_null() {
    assertThatThrownBy(() -> new ClientRouteProxy(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("connectionId must not be null");
  }

  @Test
  public void should_reject_empty_connection_id() {
    assertThatThrownBy(() -> new ClientRouteProxy(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionId must not be empty");
  }

  @Test
  public void should_reject_blank_connection_id() {
    assertThatThrownBy(() -> new ClientRouteProxy("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionId must not be empty");
  }

  @Test
  public void should_reject_blank_connection_addr() {
    assertThatThrownBy(() -> new ClientRouteProxy("conn-id-1", "  "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connectionAddr must not be empty");
  }

  @Test
  public void should_reject_duplicate_connection_ids() {
    assertThatThrownBy(
            () ->
                ClientRoutesConfig.builder()
                    .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
                    .addEndpoint(new ClientRouteProxy("conn-id-1", "host2"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate connection ID");
  }

  @Test
  public void should_reject_invalid_table_name() {
    assertThatThrownBy(
            () ->
                ClientRoutesConfig.builder()
                    .addEndpoint(new ClientRouteProxy("conn-id-1"))
                    .withTableName("system.client_routes; DROP TABLE foo")
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid table name");
  }

  @Test
  public void should_accept_valid_table_names() {
    // Qualified name
    ClientRoutesConfig config1 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1"))
            .withTableName("my_keyspace.my_table")
            .build();
    assertThat(config1.getTableName()).isEqualTo("my_keyspace.my_table");

    // Unqualified name
    ClientRoutesConfig config2 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1"))
            .withTableName("client_routes")
            .build();
    assertThat(config2.getTableName()).isEqualTo("client_routes");
  }

  @Test
  public void should_reject_null_table_name() {
    assertThatThrownBy(
            () ->
                ClientRoutesConfig.builder()
                    .addEndpoint(new ClientRouteProxy("conn-id-1"))
                    .withTableName(null)
                    .build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableName must not be null");
  }

  @Test
  public void should_replace_endpoints_with_withEndpoints() {
    String connectionId1 = "conn-id-1";
    String connectionId2 = "conn-id-2";
    String connectionId3 = "conn-id-3";

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId1))
            .withEndpoints(
                java.util.Arrays.asList(
                    new ClientRouteProxy(connectionId2), new ClientRouteProxy(connectionId3)))
            .build();

    assertThat(config.getEndpoints()).hasSize(2);
    assertThat(config.getEndpoints().get(0).getConnectionId()).isEqualTo(connectionId2);
    assertThat(config.getEndpoints().get(1).getConnectionId()).isEqualTo(connectionId3);
  }

  @Test
  public void should_reject_connection_addr_with_port() {
    assertThatThrownBy(() -> new ClientRouteProxy("conn-id-1", "host.example.com:9042"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without a port");
  }

  @Test
  public void should_reject_ipv6_bracketed_connection_addr_with_port() {
    assertThatThrownBy(() -> new ClientRouteProxy("conn-id-1", "[::1]:9042"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without a port");
  }

  @Test
  public void should_reject_connection_addr_with_leading_colon_port() {
    // ":9042" is a port-only string with no hostname — must be rejected
    assertThatThrownBy(() -> new ClientRouteProxy("conn-id-1", ":9042"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without a port");
  }

  @Test
  public void should_accept_plain_hostname_connection_addr() {
    ClientRouteProxy ep = new ClientRouteProxy("conn-id-1", "host.example.com");
    assertThat(ep.getConnectionAddr()).isEqualTo("host.example.com");
  }

  @Test
  public void should_accept_ipv4_connection_addr() {
    ClientRouteProxy ep = new ClientRouteProxy("conn-id-1", "10.0.1.5");
    assertThat(ep.getConnectionAddr()).isEqualTo("10.0.1.5");
  }

  @Test
  public void should_accept_bare_ipv6_connection_addr() {
    // Bare IPv6 contains multiple colons — should NOT be treated as host:port
    ClientRouteProxy ep = new ClientRouteProxy("conn-id-1", "::1");
    assertThat(ep.getConnectionAddr()).isEqualTo("::1");
  }

  @Test
  public void should_reject_connection_addr_with_spaces() {
    assertThatThrownBy(() -> new ClientRouteProxy("conn-id-1", "host name.example.com"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid characters");
  }

  @Test
  public void should_reject_connection_addr_with_special_characters() {
    assertThatThrownBy(() -> new ClientRouteProxy("conn-id-1", "host!@#.example.com"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid characters");
  }

  @Test
  public void should_equal_same_contact_point() {
    ClientRouteProxy ep1 = new ClientRouteProxy("conn-id-1", "host1");
    ClientRouteProxy ep2 = new ClientRouteProxy("conn-id-1", "host1");
    assertThat(ep1).isEqualTo(ep2);
    assertThat(ep1.hashCode()).isEqualTo(ep2.hashCode());
  }

  @Test
  public void should_not_equal_different_connection_id() {
    ClientRouteProxy ep1 = new ClientRouteProxy("conn-id-1", "host1");
    ClientRouteProxy ep2 = new ClientRouteProxy("conn-id-2", "host1");
    assertThat(ep1).isNotEqualTo(ep2);
  }

  @Test
  public void should_not_equal_different_connection_addr() {
    ClientRouteProxy ep1 = new ClientRouteProxy("conn-id-1", "host1");
    ClientRouteProxy ep2 = new ClientRouteProxy("conn-id-1", "host2");
    assertThat(ep1).isNotEqualTo(ep2);
  }

  @Test
  public void should_equal_same_config() {
    ClientRoutesConfig cfg1 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
            .build();
    ClientRoutesConfig cfg2 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
            .build();
    assertThat(cfg1).isEqualTo(cfg2);
    assertThat(cfg1.hashCode()).isEqualTo(cfg2.hashCode());
  }

  @Test
  public void should_not_equal_different_config_endpoints() {
    ClientRoutesConfig cfg1 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
            .build();
    ClientRoutesConfig cfg2 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-2", "host2"))
            .build();
    assertThat(cfg1).isNotEqualTo(cfg2);
  }

  @Test
  public void should_return_unmodifiable_endpoints_list() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
            .build();

    assertThatThrownBy(() -> config.getEndpoints().add(new ClientRouteProxy("conn-id-2")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_not_equal_config_with_different_table_name() {
    ClientRoutesConfig cfg1 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
            .build();
    ClientRoutesConfig cfg2 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy("conn-id-1", "host1"))
            .withTableName("custom.my_routes")
            .build();
    assertThat(cfg1).isNotEqualTo(cfg2);
  }

  @Test
  public void should_not_equal_proxy_with_null_vs_non_null_connection_addr() {
    ClientRouteProxy ep1 = new ClientRouteProxy("conn-id-1");
    ClientRouteProxy ep2 = new ClientRouteProxy("conn-id-1", "host1");
    assertThat(ep1).isNotEqualTo(ep2);
    assertThat(ep2).isNotEqualTo(ep1);
  }
}
