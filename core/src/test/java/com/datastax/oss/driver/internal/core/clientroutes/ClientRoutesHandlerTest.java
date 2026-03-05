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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientRoutesHandlerTest {

  @Mock private InternalDriverContext context;

  private TestableClientRoutesHandler handler;

  /**
   * Subclass exposing package-private {@code resolvedRoutesRef} so tests can inject test data
   * without actually executing admin queries.
   */
  @SuppressWarnings("NewClassNamingConvention")
  static class TestableClientRoutesHandler extends ClientRoutesHandler {
    TestableClientRoutesHandler(InternalDriverContext ctx, ClientRoutesConfig cfg) {
      super(ctx, cfg);
    }

    void setRoutes(Map<UUID, ResolvedClientRoute> routes) {
      resolvedRoutesRef.set(new ConcurrentHashMap<>(routes));
    }
  }

  @Before
  public void setup() {
    when(context.getSessionName()).thenReturn("test-session");
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(UUID.randomUUID().toString(), "host1"))
            .build();
    handler = new TestableClientRoutesHandler(context, config);
  }

  // ---- translate() -------------------------------------------------------

  @Test
  public void should_return_null_for_unknown_host_id() {
    assertThat(handler.translate(UUID.randomUUID(), false)).isNull();
  }

  @Test
  public void should_translate_known_host_id_non_ssl() {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, 9142)));

    InetSocketAddress result = handler.translate(hostId, false);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_select_tls_port_when_ssl() {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, 9142)));

    InetSocketAddress result = handler.translate(hostId, true);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9142);
  }

  @Test
  public void should_fall_back_to_non_ssl_port_when_tls_port_absent() {
    UUID hostId = UUID.randomUUID();
    // tls_port is null — should warn and fall back to non-SSL port
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, null)));

    InetSocketAddress result = handler.translate(hostId, true);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_return_null_when_no_port_configured() {
    UUID hostId = UUID.randomUUID();
    // Both ports null → IllegalStateException → translate() catches it and returns null
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", null, null)));

    assertThat(handler.translate(hostId, false)).isNull();
  }

  @Test
  public void should_return_null_after_close() {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(hostId, new ResolvedClientRoute(hostId, "127.0.0.1", 9042, null)));

    handler.close();

    assertThat(handler.translate(hostId, false)).isNull();
  }

  @Test
  public void should_return_null_for_unresolvable_hostname() {
    UUID hostId = UUID.randomUUID();
    // Use a hostname guaranteed not to resolve
    handler.setRoutes(
        ImmutableMap.of(
            hostId,
            new ResolvedClientRoute(hostId, "this.host.does.not.exist.invalid", 9042, null)));

    // Should not throw; returns null and logs a warning
    assertThat(handler.translate(hostId, false)).isNull();
  }

  @Test
  public void should_refresh_updates_routes() {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();

    handler.setRoutes(
        ImmutableMap.of(hostId1, new ResolvedClientRoute(hostId1, "127.0.0.1", 9042, null)));
    assertThat(handler.translate(hostId1, false)).isNotNull();
    assertThat(handler.translate(hostId2, false)).isNull();

    // Simulate a refresh that swaps in a different set of routes
    handler.setRoutes(
        ImmutableMap.of(hostId2, new ResolvedClientRoute(hostId2, "127.0.0.2", 9042, null)));

    assertThat(handler.translate(hostId1, false)).isNull();
    assertThat(handler.translate(hostId2, false)).isNotNull();
  }
}
