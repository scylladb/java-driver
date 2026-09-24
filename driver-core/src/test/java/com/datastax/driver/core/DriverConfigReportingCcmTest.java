/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.ScyllaVersion;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Verifies driver-configuration reporting end-to-end against a live ScyllaDB (via CCM), by reading
 * back what the server actually stored in {@code system.clients.client_options}:
 *
 * <ul>
 *   <li>{@code SESSION_ID} is present on every one of the session's connections, with a single
 *       shared value;
 *   <li>{@code SESSION_ID} is shared across every {@code Session} obtained from the same {@code
 *       Cluster} (it is Cluster-scoped, not Session-scoped — see {@link Connection.Factory});
 *   <li>{@code DRIVER_CONFIG} is stored for exactly one connection (the control connection), as the
 *       full versioned JSON report;
 *   <li>a later control connection reports the datacenter the load balancing policy has inferred in
 *       the meantime, which the first one could not yet know;
 *   <li>with reporting disabled, {@code SESSION_ID} is still stored but {@code DRIVER_CONFIG} is
 *       not.
 * </ul>
 *
 * <p>The cluster under test uses the default configuration — no {@code withDriverConfigReporting}
 * call — so these also assert that reporting is enabled by default.
 *
 * <p>ScyllaDB-only: {@code system.clients.client_options} is a Scylla feature (added in ScyllaDB
 * 2026.1).
 */
@ScyllaVersion(
    minOSS = "2026.1",
    minEnterprise = "2026.1",
    description = "system.clients.client_options requires ScyllaDB 2026.1+")
public class DriverConfigReportingCcmTest extends CCMTestsSupport {

  private static final String DRIVER_NAME = "ScyllaDB Java Driver";

  @Test(groups = "short")
  public void should_store_session_id_on_all_connections_and_driver_config_on_control() {
    String sessionId = sessionId(cluster());

    // The session opens a control connection plus at least one pooled connection; system.clients is
    // updated asynchronously as connections are set up, so poll until the server reflects them.
    List<Row> rows = awaitClusterConnections(sessionId, 2);
    // Guard against the "only on control connection" assertion below being vacuously true because
    // the pool connection never showed up (e.g. it was slow, or SESSION_ID reporting regressed on
    // it) rather than because reporting is actually correct.
    assertThat(rows.size())
        .as("connections carrying this cluster's SESSION_ID")
        .isGreaterThanOrEqualTo(2);

    // DRIVER_CONFIG is stored for exactly one connection (the control connection, which
    // soleDriverConfig asserts), and what arrived is the versioned report shape once parsed rather
    // than merely a plausible string. The byte-for-byte round trip lives in the reconnect test
    // below, where a rebuild is comparable with what was sent; there is nothing to compare this
    // first report against, since the driver keeps no copy and rebuilding now would produce a
    // different, later report.
    //
    // Asserted by JSON type and not by value alone: asInt() coerces a string "1", and has() is
    // satisfied by an explicit null, so either would pass on a report of the wrong shape.
    JsonNode report = parse(soleDriverConfig(rows));
    JsonNode version = report.path("version");
    assertThat(version.isIntegralNumber()).as("version is a JSON integer").isTrue();
    assertThat(version.intValue()).isEqualTo(1);
    assertThat(
            report.path("connection").path("pool").path("shard-aware").path("enabled").isBoolean())
        .as("connection.pool.shard-aware.enabled is a JSON boolean")
        .isTrue();
  }

  @Test(groups = "short")
  public void should_report_the_inferred_datacenter_after_a_control_connection_reconnect() {
    // Its own Cluster rather than the class-level one, because this test is the only one here that
    // mutates the cluster it observes: forcing a reconnect leaves the superseded control connection
    // in system.clients until the server reaps its row, so two rows carry a DRIVER_CONFIG for a
    // while. should_store_session_id_on_all_connections_and_driver_config_on_control asserts there
    // is exactly one, and TestNG orders methods within a class alphabetically, which runs it after
    // this one -- against the very cluster this would have perturbed.
    try (Cluster cluster = register(createClusterBuilder().build())) {
      try (Session ignored = cluster.connect()) {
        String sessionId = sessionId(cluster);
        List<Row> rows = awaitClusterConnections(sessionId, 2);
        assertThat(rows).as("connections carrying this cluster's SESSION_ID").isNotEmpty();

        // The default load balancing policy is TokenAwarePolicy(DCAwareRoundRobinPolicy) with no
        // configured datacenter, so it infers one -- but only in Cluster.Manager.init(), which runs
        // after the first control connection's STARTUP. That first report can therefore say no more
        // than "a datacenter will be inferred".
        JsonNode first = parse(soleDriverConfig(rows));
        JsonNode firstPreference = first.path("connection").path("node-preference");
        assertThat(firstPreference.path("type").asText()).isEqualTo("dc-auto");
        assertThat(firstPreference.has("local-dc")).isFalse();

        // Force a new control connection. Every one of them builds its own report, so this is where
        // the datacenter the policy has since inferred reaches the server.
        Set<String> before = connectionKeys(rows);
        cluster.manager.controlConnection.triggerReconnect();

        String reconnected = awaitNewDriverConfig(sessionId, before);
        assertThat(reconnected)
            .as("DRIVER_CONFIG on the reconnected control connection")
            .isNotNull();

        String localDc = cluster.manager.controlConnection.connectedHost().getDatacenter();
        assertThat(localDc).as("datacenter of the node the control connection is on").isNotEmpty();
        JsonNode second = parse(reconnected);
        // dc-auto keeps an inferred datacenter under the plain local-dc key; only rack-auto
        // prefixes.
        assertThat(second.path("connection").path("node-preference").path("local-dc").asText())
            .isEqualTo(localDc);
        assertThat(
                second
                    .path("query")
                    .path("load-balancing")
                    .path("node-preference")
                    .path("local-dc")
                    .asText())
            .isEqualTo(localDc);

        // Byte-for-byte against a report built right now, rather than a spot-check of a few keys:
        // an oversized STARTUP value is silently truncated by an unchecked 16-bit length prefix
        // instead of being rejected (which is why MAX_DRIVER_CONFIG_LENGTH exists), and truncation,
        // reordering or encoding damage is exactly what a key-by-key check would miss. A rebuild is
        // a fair comparison only here: by now the protocol version is negotiated and PoolingOptions
        // settled, and the policy has inferred everything it is going to.
        assertThat(reconnected)
            .isEqualTo(cluster.manager.connectionFactory.buildDriverConfigReport());
      }
    }
  }

  /**
   * The single {@code DRIVER_CONFIG} among the given connections, which the control one carries.
   */
  private String soleDriverConfig(List<Row> rows) {
    List<String> driverConfigs = new ArrayList<String>();
    for (Row row : rows) {
      String driverConfig = clientOptions(row).get("DRIVER_CONFIG");
      if (driverConfig != null) {
        driverConfigs.add(driverConfig);
      }
    }
    assertThat(driverConfigs).hasSize(1);
    return driverConfigs.get(0);
  }

  /**
   * Polls {@code system.clients} until a connection of the given {@code SESSION_ID}'s cluster, and
   * outside {@code excludeKeys}, carries a {@code DRIVER_CONFIG}; returns it, or {@code null} if
   * none appears in time. Scoped to one cluster because every other driver connection to this CCM
   * node is equally "new" to a key snapshot — including the class-level session's control
   * connection, which carries a {@code DRIVER_CONFIG} of its own.
   */
  private String awaitNewDriverConfig(String sessionId, Set<String> excludeKeys) {
    long deadline = System.currentTimeMillis() + 60_000L;
    while (System.currentTimeMillis() < deadline) {
      for (Row row : newDriverConnections(excludeKeys)) {
        Map<String, String> options = clientOptions(row);
        if (!sessionId.equals(options.get("SESSION_ID"))) {
          continue;
        }
        String driverConfig = options.get("DRIVER_CONFIG");
        if (driverConfig != null) {
          return driverConfig;
        }
      }
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
    return null;
  }

  private JsonNode parse(String json) {
    try {
      return new ObjectMapper().readTree(json);
    } catch (IOException e) {
      throw new AssertionError("DRIVER_CONFIG is not valid JSON: " + json, e);
    }
  }

  @Test(groups = "short")
  public void should_share_session_id_across_multiple_sessions_from_same_cluster() {
    // The Cluster-scoped session id, as generated by the driver and reported by the class-level
    // session's connections.
    String expectedSessionId = sessionId(cluster());
    assertThat(awaitClusterConnections(expectedSessionId, 2))
        .as("initial connections carrying this cluster's SESSION_ID")
        .isNotEmpty();

    // A second Session from the SAME Cluster reuses that Cluster's single session id (see
    // Connection.Factory), so its connections must carry the identical SESSION_ID rather than a
    // fresh one.
    Set<String> existingConnections = connectionKeys(allDriverRows());
    try (Session secondSession = cluster().connect()) {
      List<Row> newRows = awaitNewDriverConnections(existingConnections, 1);
      assertThat(newRows).as("new driver connections from the second session").isNotEmpty();
      for (Row row : newRows) {
        assertThat(clientOptions(row).get("SESSION_ID")).isEqualTo(expectedSessionId);
      }
    }
  }

  @Test(groups = "short")
  public void should_store_session_id_but_no_driver_config_when_reporting_disabled() {
    // The class-level session stays connected for the whole test class, so its connections would
    // otherwise be indistinguishable from the disabled cluster's by driver_name alone. Snapshot the
    // connections that already exist, and only look at ones that appear after.
    Set<String> existingConnections = connectionKeys(allDriverRows());

    try (Cluster cluster =
        register(createClusterBuilder().withDriverConfigReporting(false).build())) {
      try (Session ignored = cluster.connect()) {
        List<Row> rows = awaitNewDriverConnections(existingConnections, 2);
        assertThat(rows.size())
            .as("new driver connections observed in system.clients")
            .isGreaterThanOrEqualTo(2);
        // SESSION_ID is sent regardless of the setting; only the config blob is suppressed.
        String sessionId = sessionId(cluster);
        for (Row row : rows) {
          assertThat(clientOptions(row))
              .containsEntry("SESSION_ID", sessionId)
              .doesNotContainKey("DRIVER_CONFIG");
        }
      }
    }
  }

  /**
   * The {@code SESSION_ID} the given (already initialized) cluster reports, read from the driver
   * side so that a cluster's connections can be told apart from any other cluster's in {@code
   * system.clients}.
   */
  private String sessionId(Cluster cluster) {
    return cluster.manager.connectionFactory.sessionId.toString();
  }

  /**
   * Polls {@code system.clients} until at least {@code min} connections carrying the given {@code
   * SESSION_ID} appear.
   */
  private List<Row> awaitClusterConnections(String sessionId, int min) {
    long deadline = System.currentTimeMillis() + 60_000L;
    List<Row> rows = clusterConnections(sessionId);
    while (rows.size() < min && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      rows = clusterConnections(sessionId);
    }
    return rows;
  }

  /**
   * Polls {@code system.clients} until at least {@code min} connections outside of {@code
   * excludeKeys} appear, i.e. new connections opened after the snapshot was taken.
   */
  private List<Row> awaitNewDriverConnections(Set<String> excludeKeys, int min) {
    long deadline = System.currentTimeMillis() + 60_000L;
    List<Row> rows = newDriverConnections(excludeKeys);
    while (rows.size() < min && System.currentTimeMillis() < deadline) {
      try {
        Thread.sleep(500L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      rows = newDriverConnections(excludeKeys);
    }
    return rows;
  }

  private List<Row> newDriverConnections(Set<String> excludeKeys) {
    List<Row> rows = new ArrayList<Row>();
    for (Row row : allDriverRows()) {
      if (!excludeKeys.contains(connectionKey(row))) {
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * The rows in {@code system.clients} for the connections carrying the given {@code SESSION_ID}.
   */
  private List<Row> clusterConnections(String sessionId) {
    List<Row> rows = new ArrayList<Row>();
    for (Row row : allDriverRows()) {
      if (sessionId.equals(clientOptions(row).get("SESSION_ID"))) {
        rows.add(row);
      }
    }
    return rows;
  }

  /** All rows in {@code system.clients} belonging to this driver, regardless of which session. */
  private List<Row> allDriverRows() {
    ResultSet result =
        session().execute("SELECT driver_name, address, port, client_options FROM system.clients");
    List<Row> rows = new ArrayList<Row>();
    for (Row row : result) {
      if (DRIVER_NAME.equals(row.getString("driver_name"))) {
        rows.add(row);
      }
    }
    return rows;
  }

  /** The client-side {@code (address, port)} identifying each of the given connections. */
  private Set<String> connectionKeys(List<Row> rows) {
    Set<String> keys = new HashSet<String>();
    for (Row row : rows) {
      keys.add(connectionKey(row));
    }
    return keys;
  }

  private String connectionKey(Row row) {
    return row.getObject("address") + ":" + row.getObject("port");
  }

  private Map<String, String> clientOptions(Row row) {
    Map<String, String> options = row.getMap("client_options", String.class, String.class);
    return options == null ? Collections.<String, String>emptyMap() : options;
  }
}
