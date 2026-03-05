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
package com.datastax.oss.driver.internal.core.context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for config-file-based client routes parsing in {@link DefaultDriverContext}.
 *
 * <p>Each test builds a minimal {@link DefaultDriverContext} from an inline HOCON snippet and
 * verifies that {@link DefaultDriverContext#buildClientRoutesConfigFromFile()} correctly parses (or
 * rejects) the {@code advanced.client-routes} section.
 *
 * <p>Note: {@code connection-addr} is a <em>plain hostname</em> — it must not include a port
 * number. The port is read from the {@code system.client_routes} table at runtime.
 */
public class ClientRoutesConfigFromFileTest {

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /**
   * Builds a {@link DefaultDriverContext} whose configuration is the driver's {@code
   * reference.conf} merged on top of the supplied extra HOCON string.
   */
  private DefaultDriverContext contextFromHocon(String extraHocon) {
    DriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () -> {
              ConfigFactory.invalidateCaches();
              return ConfigFactory.parseString(extraHocon)
                  .withFallback(
                      ConfigFactory.defaultReference()
                          .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH));
            });
    return new DefaultDriverContext(loader, ProgrammaticArguments.builder().build());
  }

  // ---------------------------------------------------------------------------
  // Happy-path: endpoint parsing
  // ---------------------------------------------------------------------------

  @Test
  public void should_parse_single_endpoint_without_addr() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]");

    ClientRoutesConfig cfg = ctx.buildClientRoutesConfigFromFile();

    assertThat(cfg).isNotNull();
    assertThat(cfg.getEndpoints()).hasSize(1);
    ClientRoutesEndpoint ep = cfg.getEndpoints().get(0);
    assertThat(ep.getConnectionId()).isEqualTo("11111111-1111-1111-1111-111111111111");
    assertThat(ep.getConnectionAddr()).isNull();
  }

  @Test
  public void should_parse_single_endpoint_with_addr() {
    // connection-addr is a plain hostname — no port
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\","
                + "    connection-addr = \"cluster.example.com\" }"
                + "]");

    ClientRoutesConfig cfg = ctx.buildClientRoutesConfigFromFile();

    assertThat(cfg).isNotNull();
    List<ClientRoutesEndpoint> eps = cfg.getEndpoints();
    assertThat(eps).hasSize(1);
    assertThat(eps.get(0).getConnectionId()).isEqualTo("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    assertThat(eps.get(0).getConnectionAddr()).isEqualTo("cluster.example.com");
  }

  @Test
  public void should_parse_multiple_endpoints() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\","
                + "    connection-addr = \"node1.example.com\" },"
                + "  { connection-id = \"22222222-2222-2222-2222-222222222222\" }"
                + "]");

    ClientRoutesConfig cfg = ctx.buildClientRoutesConfigFromFile();

    assertThat(cfg).isNotNull();
    assertThat(cfg.getEndpoints()).hasSize(2);
    assertThat(cfg.getEndpoints().get(0).getConnectionId())
        .isEqualTo("11111111-1111-1111-1111-111111111111");
    assertThat(cfg.getEndpoints().get(0).getConnectionAddr()).isEqualTo("node1.example.com");
    assertThat(cfg.getEndpoints().get(1).getConnectionId())
        .isEqualTo("22222222-2222-2222-2222-222222222222");
    assertThat(cfg.getEndpoints().get(1).getConnectionAddr()).isNull();
  }

  // ---------------------------------------------------------------------------
  // Happy-path: scalar options
  // ---------------------------------------------------------------------------

  @Test
  public void should_use_default_table_name() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]");

    assertThat(ctx.buildClientRoutesConfigFromFile().getTableName())
        .isEqualTo("system.client_routes");
  }

  @Test
  public void should_apply_custom_table_name() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes {\n"
                + "  endpoints = [{ connection-id = \"11111111-1111-1111-1111-111111111111\" }]\n"
                + "  table-name = \"test.custom_routes\"\n"
                + "}");

    assertThat(ctx.buildClientRoutesConfigFromFile().getTableName())
        .isEqualTo("test.custom_routes");
  }

  @Test
  public void should_use_default_dns_cache_duration() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]");

    assertThat(ctx.buildClientRoutesConfigFromFile().getDnsCacheDurationMillis()).isEqualTo(500L);
  }

  @Test
  public void should_apply_custom_dns_cache_duration() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes {\n"
                + "  endpoints = [{ connection-id = \"11111111-1111-1111-1111-111111111111\" }]\n"
                + "  dns-cache-duration = 2 seconds\n"
                + "}");

    assertThat(ctx.buildClientRoutesConfigFromFile().getDnsCacheDurationMillis())
        .isEqualTo(Duration.ofSeconds(2).toMillis());
  }

  // ---------------------------------------------------------------------------
  // Absent / disabled
  // ---------------------------------------------------------------------------

  @Test
  public void should_return_null_when_endpoints_not_configured() {
    // No endpoints key — reference.conf comments it out, so client routes are disabled
    DefaultDriverContext ctx = contextFromHocon("");

    assertThat(ctx.buildClientRoutesConfigFromFile()).isNull();
  }

  // ---------------------------------------------------------------------------
  // Error cases
  // ---------------------------------------------------------------------------

  @Test
  public void should_throw_when_endpoints_list_is_empty() {
    DefaultDriverContext ctx =
        contextFromHocon("advanced.client-routes.endpoints = []");

    assertThatThrownBy(ctx::buildClientRoutesConfigFromFile)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no entries");
  }

  @Test
  public void should_throw_when_endpoint_missing_connection_id() {
    // connection-addr without connection-id should fail validation
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = [{ connection-addr = \"host.example.com\" }]");

    assertThatThrownBy(ctx::buildClientRoutesConfigFromFile)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("connection-id");
  }
}
