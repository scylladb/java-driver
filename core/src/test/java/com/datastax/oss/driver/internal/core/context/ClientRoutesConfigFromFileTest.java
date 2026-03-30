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

import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesTopologyMonitor;
import com.typesafe.config.ConfigFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for config-file-based client routes parsing in {@link DefaultDriverContext}.
 *
 * <p>Each test builds a minimal {@link DefaultDriverContext} from an inline HOCON snippet and
 * verifies that {@link DefaultDriverContext#buildClientRoutesConfigFromFile()} correctly parses (or
 * rejects) the {@code advanced.client-routes} section.
 *
 * <p>Note: {@code connection-addr} must be a plain DNS name or IP address (e.g. {@code
 * "cluster.example.com"} or {@code "10.0.1.5"}). It must not include a port.
 */
public class ClientRoutesConfigFromFileTest {

  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesConfigFromFileTest.class);

  private final List<DefaultDriverContext> createdContexts = new ArrayList<>();

  @After
  public void tearDown() {
    for (DefaultDriverContext ctx : createdContexts) {
      try {
        ctx.getNettyOptions().onClose().syncUninterruptibly();
      } catch (Exception e) {
        LOG.warn("Error closing DefaultDriverContext during test tearDown", e);
      }
    }
    createdContexts.clear();
  }

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /**
   * Builds a {@link DefaultDriverContext} whose configuration is the driver's {@code
   * reference.conf} merged on top of the supplied extra HOCON string.
   */
  private DefaultDriverContext contextFromHocon(String extraHocon) {
    return contextFromHocon(extraHocon, ProgrammaticArguments.builder().build());
  }

  private DefaultDriverContext contextFromHocon(
      String extraHocon, ProgrammaticArguments programmaticArguments) {
    DriverConfigLoader loader =
        new DefaultDriverConfigLoader(
            () -> {
              ConfigFactory.invalidateCaches();
              return ConfigFactory.parseString(extraHocon)
                  .withFallback(
                      ConfigFactory.defaultReference()
                          .getConfig(DefaultDriverConfigLoader.DEFAULT_ROOT_PATH));
            });
    DefaultDriverContext ctx = new DefaultDriverContext(loader, programmaticArguments);
    createdContexts.add(ctx);
    return ctx;
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
    ClientRouteProxy ep = cfg.getEndpoints().get(0);
    assertThat(ep.getConnectionId()).isEqualTo("11111111-1111-1111-1111-111111111111");
    assertThat(ep.getConnectionAddrOverride()).isNull();
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
    List<ClientRouteProxy> eps = cfg.getEndpoints();
    assertThat(eps).hasSize(1);
    assertThat(eps.get(0).getConnectionId()).isEqualTo("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    assertThat(eps.get(0).getConnectionAddrOverride()).isEqualTo("cluster.example.com");
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
    assertThat(cfg.getEndpoints().get(0).getConnectionAddrOverride())
        .isEqualTo("node1.example.com");
    assertThat(cfg.getEndpoints().get(1).getConnectionId())
        .isEqualTo("22222222-2222-2222-2222-222222222222");
    assertThat(cfg.getEndpoints().get(1).getConnectionAddrOverride()).isNull();
  }

  // ---------------------------------------------------------------------------
  // Happy-path: scalar options
  // ---------------------------------------------------------------------------

  @Test
  public void should_always_use_default_table_name() {
    // Even if table-name is present in the HOCON (legacy config), it must be ignored —
    // the table name is no longer a file-config property.
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes {\n"
                + "  endpoints = [{ connection-id = \"11111111-1111-1111-1111-111111111111\" }]\n"
                + "  table-name = \"test.should_be_ignored\"\n"
                + "}");

    assertThat(ctx.buildClientRoutesConfigFromFile().getTableName())
        .isEqualTo("system.client_routes");
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
    DefaultDriverContext ctx = contextFromHocon("advanced.client-routes.endpoints = []");

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

  @Test
  public void should_allow_client_routes_with_unqualified_passthrough_address_translator() {
    // Unqualified short name — default package resolution must find PassThroughAddressTranslator
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]\n"
                + "advanced.address-translator.class = PassThroughAddressTranslator");

    assertThat(ctx.getTopologyMonitor()).isInstanceOf(ClientRoutesTopologyMonitor.class);
  }

  @Test
  public void should_allow_client_routes_with_qualified_passthrough_address_translator() {
    // Fully-qualified name — direct class load, no package prefix appended
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]\n"
                + "advanced.address-translator.class ="
                + " com.datastax.oss.driver.internal.core.addresstranslation"
                + ".PassThroughAddressTranslator");

    assertThat(ctx.getTopologyMonitor()).isInstanceOf(ClientRoutesTopologyMonitor.class);
  }

  @Test
  public void should_throw_when_secure_connect_bundle_and_client_routes_both_configured() {
    ProgrammaticArguments args =
        ProgrammaticArguments.builder()
            .withCloudProxyAddress(new InetSocketAddress("127.0.0.1", 9042))
            .build();
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]",
            args);

    assertThatThrownBy(ctx::getTopologyMonitor)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("secure connect bundle")
        .hasMessageContaining("client routes");
  }

  @Test
  public void should_throw_when_client_routes_and_unloadable_address_translator_class() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]\n"
                + "advanced.address-translator.class ="
                + " com.nonexistent.FakeAddressTranslator");

    assertThatThrownBy(ctx::getTopologyMonitor)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Could not load AddressTranslator class")
        .hasMessageContaining("com.nonexistent.FakeAddressTranslator");
  }

  @Test
  public void should_throw_when_client_routes_and_custom_address_translator() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\" }"
                + "]\n"
                + "advanced.address-translator.class = Ec2MultiRegionAddressTranslator");

    assertThatThrownBy(ctx::getTopologyMonitor)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Both client routes configuration and a custom AddressTranslator")
        .hasMessageContaining("Ec2MultiRegionAddressTranslator");
  }

  @Test
  public void should_throw_when_duplicate_connection_ids_in_config() {
    DefaultDriverContext ctx =
        contextFromHocon(
            "advanced.client-routes.endpoints = ["
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\","
                + "    connection-addr = \"host1.example.com\" },"
                + "  { connection-id = \"11111111-1111-1111-1111-111111111111\","
                + "    connection-addr = \"host2.example.com\" }"
                + "]");

    assertThatThrownBy(ctx::buildClientRoutesConfigFromFile)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate connection ID");
  }
}
