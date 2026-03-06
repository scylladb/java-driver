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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.clientroutes.ResolvedClientRoute;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientRoutesTopologyMonitorTest {

  @Mock private InternalDriverContext context;
  @Mock private ControlConnection controlConnection;
  @Mock private DriverConfig driverConfig;
  @Mock private DriverExecutionProfile defaultProfile;

  private TestableClientRoutesTopologyMonitor handler;

  /**
   * Subclass exposing package-private {@code resolvedRoutesCache} so tests can inject test data
   * without actually executing admin queries.
   */
  @SuppressWarnings("NewClassNamingConvention")
  static class TestableClientRoutesTopologyMonitor extends ClientRoutesTopologyMonitor {
    TestableClientRoutesTopologyMonitor(InternalDriverContext ctx, ClientRoutesConfig cfg) {
      super(ctx, cfg);
    }

    void setRoutes(Map<UUID, ResolvedClientRoute> routes) {
      resolvedRoutesCache.set(new HashMap<>(routes));
    }
  }

  @Before
  public void setup() {
    when(context.getSessionName()).thenReturn("test-session");
    when(context.getControlConnection()).thenReturn(controlConnection);
    when(context.getConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(defaultProfile.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT)).thenReturn(false);
    when(context.getSslEngineFactory()).thenReturn(Optional.empty());
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(UUID.randomUUID().toString(), "host1"))
            .build();
    handler = new TestableClientRoutesTopologyMonitor(context, config);
  }

  // ---- resolve() -------------------------------------------------------

  @Test
  public void should_throw_for_unknown_host_id() {
    assertThatThrownBy(() -> handler.resolve(UUID.randomUUID()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No client route found");
  }

  @Test
  public void should_resolve_known_host_id_non_ssl() throws UnknownHostException {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, 9142)));

    InetSocketAddress result = handler.resolve(hostId);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_select_tls_port_when_ssl() throws UnknownHostException {
    // Recreate handler with SSL enabled
    when(context.getSslEngineFactory())
        .thenReturn(
            Optional.of(Mockito.mock(com.datastax.oss.driver.api.core.ssl.SslEngineFactory.class)));
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(UUID.randomUUID().toString(), "host1"))
            .build();
    handler = new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, 9142)));

    InetSocketAddress result = handler.resolve(hostId);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9142);
  }

  @Test
  public void should_fall_back_to_non_ssl_port_when_tls_port_absent() throws UnknownHostException {
    // Recreate handler with SSL enabled
    when(context.getSslEngineFactory())
        .thenReturn(
            Optional.of(Mockito.mock(com.datastax.oss.driver.api.core.ssl.SslEngineFactory.class)));
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(UUID.randomUUID().toString(), "host1"))
            .build();
    handler = new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    // tls_port is null — should warn and fall back to non-SSL port
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, null)));

    InetSocketAddress result = handler.resolve(hostId);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_throw_when_no_port_configured() {
    UUID hostId = UUID.randomUUID();
    // Both ports null → IllegalStateException
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", null, null)));

    assertThatThrownBy(() -> handler.resolve(hostId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No port configured");
  }

  @Test
  public void should_throw_after_close() {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, null)));

    handler.close();

    assertThatThrownBy(() -> handler.resolve(hostId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }

  @Test
  public void should_throw_for_unresolvable_hostname() {
    UUID hostId = UUID.randomUUID();
    // Use a hostname guaranteed not to resolve
    handler.setRoutes(
        ImmutableMap.of(
            hostId,
            new ResolvedClientRoute(hostId, "this.host.does.not.exist.invalid", 9042, null)));

    assertThatThrownBy(() -> handler.resolve(hostId)).isInstanceOf(UnknownHostException.class);
  }

  @Test
  public void should_refresh_updates_routes() throws UnknownHostException {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();

    handler.setRoutes(
        ImmutableMap.of(hostId1, new ResolvedClientRoute(hostId1, "127.0.0.1", 9042, null)));
    assertThat(handler.resolve(hostId1)).isNotNull();
    assertThatThrownBy(() -> handler.resolve(hostId2)).isInstanceOf(IllegalStateException.class);

    // Simulate a refresh that swaps in a different set of routes
    handler.setRoutes(
        ImmutableMap.of(hostId2, new ResolvedClientRoute(hostId2, "127.0.0.2", 9042, null)));

    assertThatThrownBy(() -> handler.resolve(hostId1)).isInstanceOf(IllegalStateException.class);
    assertThat(handler.resolve(hostId2)).isNotNull();
  }
}
