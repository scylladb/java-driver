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
            .addEndpoint(new ClientRoutesEndpoint(connectionId, connectionAddr))
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
            .addEndpoint(new ClientRoutesEndpoint(connectionId1, "host1"))
            .addEndpoint(new ClientRoutesEndpoint(connectionId2, "host2"))
            .build();

    assertThat(config.getEndpoints()).hasSize(2);
    assertThat(config.getEndpoints().get(0).getConnectionId()).isEqualTo(connectionId1);
    assertThat(config.getEndpoints().get(1).getConnectionId()).isEqualTo(connectionId2);
  }

  @Test
  public void should_build_config_with_custom_table_name() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint("conn-id-1"))
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
    ClientRoutesEndpoint endpoint = new ClientRoutesEndpoint(connectionId);

    assertThat(endpoint.getConnectionId()).isEqualTo(connectionId);
    assertThat(endpoint.getConnectionAddr()).isNull();
  }

  @Test
  public void should_create_endpoint_with_connection_address() {
    String connectionId = "conn-id-1";
    String connectionAddr = "host";
    ClientRoutesEndpoint endpoint = new ClientRoutesEndpoint(connectionId, connectionAddr);

    assertThat(endpoint.getConnectionId()).isEqualTo(connectionId);
    assertThat(endpoint.getConnectionAddr()).isEqualTo(connectionAddr);
  }

  @Test
  public void should_fail_when_connection_id_is_null() {
    assertThatThrownBy(() -> new ClientRoutesEndpoint(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("connectionId must not be null");
  }

  @Test
  public void should_replace_endpoints_with_withEndpoints() {
    String connectionId1 = "conn-id-1";
    String connectionId2 = "conn-id-2";
    String connectionId3 = "conn-id-3";

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(connectionId1))
            .withEndpoints(
                java.util.Arrays.asList(
                    new ClientRoutesEndpoint(connectionId2),
                    new ClientRoutesEndpoint(connectionId3)))
            .build();

    assertThat(config.getEndpoints()).hasSize(2);
    assertThat(config.getEndpoints().get(0).getConnectionId()).isEqualTo(connectionId2);
    assertThat(config.getEndpoints().get(1).getConnectionId()).isEqualTo(connectionId3);
  }

  @Test
  public void should_use_default_dns_cache_duration() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder().addEndpoint(new ClientRoutesEndpoint("conn-id-1")).build();

    assertThat(config.getDnsCacheDurationMillis()).isEqualTo(500L);
  }

  @Test
  public void should_build_config_with_custom_dns_cache_duration() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint("conn-id-1"))
            .withDnsCacheDuration(1000L)
            .build();

    assertThat(config.getDnsCacheDurationMillis()).isEqualTo(1000L);
  }

  @Test
  public void should_allow_zero_dns_cache_duration() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint("conn-id-1"))
            .withDnsCacheDuration(0L)
            .build();

    assertThat(config.getDnsCacheDurationMillis()).isEqualTo(0L);
  }

  @Test
  public void should_fail_when_dns_cache_duration_is_negative() {
    assertThatThrownBy(
            () ->
                ClientRoutesConfig.builder()
                    .addEndpoint(new ClientRoutesEndpoint("conn-id-1"))
                    .withDnsCacheDuration(-1L)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DNS cache duration must be non-negative");
  }
}
