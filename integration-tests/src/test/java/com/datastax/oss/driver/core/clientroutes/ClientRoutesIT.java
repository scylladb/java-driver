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

/*
 * Copyright (C) 2025 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.clientroutes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.ScyllaOnly;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesTopologyMonitor;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for the PrivateLink client routes feature.
 *
 * <p>These tests require a ScyllaDB instance that exposes the {@code system.client_routes} table
 * (introduced in scylladb/scylladb#27323). Each test opens its own CQL sessions to populate the
 * test table and configure {@link ClientRoutesConfig} precisely.
 *
 * <p>The <em>positive</em> tests use a separate keyspace-level table (not the actual {@code
 * system.client_routes}) where write access is available, configured via {@link
 * ClientRoutesConfig.Builder#withTableName}. This allows testing the full query and translation
 * pipeline without requiring privileged write access to the {@code system} keyspace.
 *
 * <p>A dedicated <em>negative</em> test ({@link
 * ClientRoutesUnsupportedVersionIT#should_not_activate_on_unsupported_version()}) is annotated to
 * run only on versions that do <em>not</em> support the feature and verifies that the handler
 * remains empty when {@code system.client_routes} does not exist.
 */
@Category(IsolatedTests.class)
@ScyllaOnly(description = "system.client_routes is a ScyllaDB-only system table")
@ScyllaRequirement(
    minEnterprise = "2026.1",
    description =
        "system.client_routes requires ScyllaDB Enterprise >= 2026.1 (scylladb/scylladb#27323)")
public class ClientRoutesIT {

  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesIT.class);

  /**
   * Name of a test-keyspace table that mirrors the {@code system.client_routes} schema. Tests
   * INSERT into this table via a plain session, then point {@link ClientRoutesConfig} at it.
   */
  private static final String TEST_TABLE = "test_client_routes.client_routes";

  private static final String CREATE_KEYSPACE =
      "CREATE KEYSPACE IF NOT EXISTS test_client_routes"
          + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}";

  /**
   * DDL matching the server-side {@code system.client_routes} schema. Using a user keyspace lets
   * the test insert rows freely without requiring internal permissions.
   */
  private static final String CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS "
          + TEST_TABLE
          + " ("
          + "  connection_id uuid,"
          + "  host_id uuid,"
          + "  address text,"
          + "  port int,"
          + "  tls_port int,"
          + "  PRIMARY KEY (connection_id, host_id)"
          + ")";

  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE).withKeyspace(false).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  // ---- helpers -----------------------------------------------------------

  /** Returns the IP address of CCM node 1 (e.g. {@code "127.0.2.1"}). */
  private String nodeAddress() {
    return CCM_RULE.getCcmBridge().getNodeIpAddress(1);
  }

  /** Opens a plain (no client-routes) session against CCM to set up the test table and rows. */
  private CqlSession openAdminSession() {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withConfigLoader(
                SessionUtils.configLoaderBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                    .build())
            .build();
  }

  /** Ensures the test keyspace and table exist and are empty. */
  private void prepareTable(CqlSession admin) {
    admin.execute(CREATE_KEYSPACE);
    admin.execute(CREATE_TABLE);
    admin.execute("TRUNCATE TABLE " + TEST_TABLE);
  }

  /** Inserts one row into the test client_routes table. */
  private void insertRoute(
      CqlSession admin, String connectionId, UUID hostId, String address, int port) {
    admin.execute(
        "INSERT INTO "
            + TEST_TABLE
            + " (connection_id, host_id, address, port)"
            + " VALUES ("
            + connectionId
            + ", "
            + hostId
            + ", '"
            + address
            + "', "
            + port
            + ")");
  }

  /** Builds a {@link CqlSession} with the given {@link ClientRoutesConfig}. */
  private CqlSession openClientRoutesSession(ClientRoutesConfig config) {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withClientRoutesConfig(config)
            .withConfigLoader(
                SessionUtils.configLoaderBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                    .build())
            .build();
  }

  /** Returns the {@link ClientRoutesTopologyMonitor} from an open session's context. */
  private ClientRoutesTopologyMonitor handlerOf(CqlSession session) {
    ClientRoutesTopologyMonitor handler =
        ((InternalDriverContext) session.getContext()).getClientRoutesHandler();
    assertThat(handler)
        .as("ClientRoutesHandler must be non-null when ClientRoutesConfig is set")
        .isNotNull();
    return handler;
  }

  /**
   * Guards a positive test: confirms that {@code system.client_routes} really exists on the running
   * server. If it doesn't, the test is skipped via an {@link AssumptionViolatedException} rather
   * than failing, which provides a clear signal that the feature is not yet available.
   *
   * <p>This is a defence-in-depth check on top of the class-level {@link ScyllaRequirement} — it
   * catches cases where the version annotation is wrong or the binary predates the feature despite
   * its reported version.
   */
  private void requireSystemClientRoutesTable(CqlSession admin) {
    Row row =
        admin
            .execute(
                "SELECT table_name FROM system_schema.tables"
                    + " WHERE keyspace_name='system' AND table_name='client_routes'")
            .one();
    if (row == null) {
      throw new AssumptionViolatedException(
          "system.client_routes does not exist on this server — "
              + "feature requires ScyllaDB Enterprise >= 2026.1 (not yet available on OSS). "
              + "Skipping positive client-routes test.");
    }
  }

  /**
   * Tries to resolve a host ID and returns the result, or null if no route is found (i.e.
   * IllegalStateException is thrown).
   */
  private InetSocketAddress tryResolve(ClientRoutesTopologyMonitor handler, UUID hostId) {
    try {
      return handler.resolve(hostId);
    } catch (IllegalStateException e) {
      return null;
    } catch (UnknownHostException e) {
      throw new RuntimeException("DNS resolution failed for host_id=" + hostId, e);
    }
  }

  // ---- tests -------------------------------------------------------------

  /**
   * Verifies that {@link ClientRoutesTopologyMonitor#init()} reads rows from the configured table
   * and populates the internal route map on session startup.
   */
  @Test
  public void should_load_routes_from_table_on_init() throws UnknownHostException {
    String connectionId = UUID.randomUUID().toString();
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
      prepareTable(admin);

      // Discover the node host_id from system.local
      Row localRow = admin.execute("SELECT host_id FROM system.local WHERE key='local'").one();
      assertThat(localRow).isNotNull();
      UUID hostId = localRow.getUuid("host_id");
      assertThat(hostId).isNotNull();

      insertRoute(admin, connectionId, hostId, nodeAddr, 9042);

      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRoutesEndpoint(connectionId, nodeAddr + ":9042"))
              .withTableName(TEST_TABLE)
              .build();

      try (CqlSession session = openClientRoutesSession(config)) {
        // Session must be usable
        ResultSet rs =
            session.execute("SELECT release_version FROM system.local WHERE key='local'");
        assertThat(rs.one()).isNotNull();

        // Handler must have loaded the route for the known host_id
        InetSocketAddress translated = handlerOf(session).resolve(hostId);
        assertThat(translated)
            .as("Handler should have a translated address for host_id=%s", hostId)
            .isNotNull();
        assertThat(translated.getPort()).isEqualTo(9042);

        LOG.info("Translated host_id={} -> {}", hostId, translated);
      }
    }
  }

  /**
   * Verifies that calling {@link ClientRoutesTopologyMonitor#refresh()} after inserting new rows
   * picks up the new routes without restarting the session.
   */
  @Test
  public void should_refresh_routes_after_table_update() throws Exception {
    String connectionId = UUID.randomUUID().toString();
    UUID hostId = UUID.randomUUID(); // synthetic — tests the query/map pipeline
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
      prepareTable(admin); // empty initially

      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRoutesEndpoint(connectionId, nodeAddr + ":9042"))
              .withTableName(TEST_TABLE)
              .build();

      try (CqlSession session = openClientRoutesSession(config)) {
        ClientRoutesTopologyMonitor handler = handlerOf(session);

        // No route yet
        assertThatThrownBy(() -> handler.resolve(hostId)).isInstanceOf(IllegalStateException.class);

        // Insert a new route and explicitly trigger a refresh
        insertRoute(admin, connectionId, hostId, nodeAddr, 9042);
        handler.refresh().toCompletableFuture().get(10, TimeUnit.SECONDS);

        InetSocketAddress translated = handler.resolve(hostId);
        assertThat(translated).isNotNull();
        assertThat(translated.getPort()).isEqualTo(9042);
      }
    }
  }

  /**
   * Verifies that a session with client routes configured against an empty table still starts
   * successfully and remains usable (nodes accessible via untranslated addresses).
   */
  @Test
  public void should_start_session_when_table_is_empty() {
    String nodeAddr = nodeAddress();
    String connectionId = UUID.randomUUID().toString();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
      prepareTable(admin); // intentionally empty

      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRoutesEndpoint(connectionId, nodeAddr + ":9042"))
              .withTableName(TEST_TABLE)
              .build();

      try (CqlSession session = openClientRoutesSession(config)) {
        ResultSet rs =
            session.execute("SELECT release_version FROM system.local WHERE key='local'");
        assertThat(rs.one()).isNotNull();

        // No routes loaded — resolve throws for any host_id
        assertThatThrownBy(() -> handlerOf(session).resolve(UUID.randomUUID()))
            .isInstanceOf(IllegalStateException.class);
      }
    }
  }

  /**
   * Verifies that a route row with a {@code tls_port} column is correctly stored and the non-SSL
   * port is used when SSL is not configured.
   */
  @Test
  public void should_select_non_ssl_port_when_ssl_not_configured() throws UnknownHostException {
    String connectionId = UUID.randomUUID().toString();
    UUID hostId = UUID.randomUUID();
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
      prepareTable(admin);

      admin.execute(
          "INSERT INTO "
              + TEST_TABLE
              + " (connection_id, host_id, address, port, tls_port)"
              + " VALUES ("
              + connectionId
              + ", "
              + hostId
              + ", '"
              + nodeAddr
              + "', 9042, 9142)");

      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRoutesEndpoint(connectionId, nodeAddr + ":9042"))
              .withTableName(TEST_TABLE)
              .build();

      try (CqlSession session = openClientRoutesSession(config)) {
        ClientRoutesTopologyMonitor handler = handlerOf(session);

        InetSocketAddress resolved = handler.resolve(hostId);
        assertThat(resolved).isNotNull();
        // Non-SSL session should use the regular port
        assertThat(resolved.getPort()).isEqualTo(9042);
      }
    }
  }

  /**
   * Verifies that after a control connection force-reconnect, {@link
   * ClientRoutesTopologyMonitor#refresh()} is called automatically and picks up new rows inserted
   * while the session was running.
   */
  @Test
  public void should_refresh_routes_after_control_connection_reconnect() {
    String connectionId = UUID.randomUUID().toString();
    UUID hostId = UUID.randomUUID();
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
      prepareTable(admin); // no rows initially

      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRoutesEndpoint(connectionId, nodeAddr + ":9042"))
              .withTableName(TEST_TABLE)
              .build();

      try (CqlSession session = openClientRoutesSession(config)) {
        ClientRoutesTopologyMonitor handler = handlerOf(session);
        assertThat(tryResolve(handler, hostId)).isNull();

        // Insert a new route while the session is running
        insertRoute(admin, connectionId, hostId, nodeAddr, 9042);

        // Force control connection reconnect — onSuccessfulReconnect triggers handler.refresh()
        ((InternalDriverContext) session.getContext()).getControlConnection().reconnectNow();

        // Routes are refreshed asynchronously; wait for the translation to become available
        await().atMost(15, TimeUnit.SECONDS).until(() -> tryResolve(handler, hostId) != null);

        assertThat(tryResolve(handler, hostId))
            .as("Route should be available after reconnect and refresh for host_id=%s", hostId)
            .isNotNull()
            .extracting(InetSocketAddress::getPort)
            .isEqualTo(9042);
      }
    }
  }
}
