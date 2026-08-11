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
package com.datastax.oss.driver.core.config;

import static com.datastax.oss.driver.core.config.DriverConfigReportingAssertions.assertDriverConfigPayload;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Verifies driver-configuration reporting end-to-end against a live server (via CCM), by reading
 * back what the server actually stored for each connection's {@code STARTUP} options.
 *
 * <p>This is the real-server counterpart of {@link DriverConfigReportingSimulacronIT}: where
 * Simulacron only proves what the driver <em>sends</em>, this confirms that a real server
 * <em>accepts</em> the extra {@code STARTUP} keys and <em>stores</em> them, so that (a) {@code
 * SESSION_ID} is present on every one of the session's connections with a single shared value, and
 * (b) {@code DRIVER_CONFIG} is stored for exactly one connection, which is the control connection —
 * matched by address and port against the control connection's channel, so the check cannot be
 * satisfied by a pooled connection.
 *
 * <p>The report itself describes the driver's own configuration, so it reads identically on both
 * backends; only the table that exposes the stored options differs: ScyllaDB uses {@code
 * system.clients.client_options}, while Apache Cassandra exposes it in {@code
 * system_views.clients.client_options} (added in Cassandra 4.1).
 */
@Category(ParallelizableTests.class)
@BackendRequirement(
    type = BackendType.SCYLLA,
    minInclusive = "2026.1.0",
    description = "system.clients.client_options is a ScyllaDB feature")
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "4.1",
    description = "system_views.clients.client_options was added in Cassandra 4.1")
public class DriverConfigReportingCcmIT {

  private static final String DRIVER_NAME = "ScyllaDB Java Driver";

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, true)
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  /**
   * The table exposing connected clients and their {@code STARTUP} options: {@code system.clients}
   * on ScyllaDB, {@code system_views.clients} on Apache Cassandra (the only per-backend difference;
   * the columns and {@code client_options} keys are identical).
   */
  private static String clientsTable() {
    return CcmBridge.isDistributionOf(BackendType.SCYLLA)
        ? "system.clients"
        : "system_views.clients";
  }

  @Test
  public void should_store_session_id_on_all_connections_and_driver_config_on_control() {
    CqlSession session = SESSION_RULE.session();

    // The session opens a control connection plus at least one pooled connection; wait until the
    // server reflects them (the clients table is updated asynchronously as connections are set up).
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(() -> driverConnections(session).size() >= 2);

    List<Row> rows = driverConnections(session);

    // Evidence for the record: dump exactly what the server stored per connection.
    System.out.println(clientsTable() + " rows for this driver session:");
    for (Row row : rows) {
      System.out.printf(
          "  %s:%s stage=%s client_options=%s%n",
          row.getObject("address"),
          row.getObject("port"),
          row.getString("connection_stage"),
          row.getMap("client_options", String.class, String.class));
    }

    // (a) Every row carries this session's SESSION_ID (that is what they were selected on), and
    // there is more than one of them — otherwise (b) below would be vacuous.
    assertThat(rows).hasSizeGreaterThanOrEqualTo(2);
    Set<String> sessionIds =
        rows.stream().map(row -> clientOptions(row).get("SESSION_ID")).collect(Collectors.toSet());
    assertThat(sessionIds).containsExactly(sessionId(session));

    // (b) DRIVER_CONFIG is stored for exactly one connection, and that connection is the control
    // one — identified independently of the reported options, by the local address and port of the
    // control connection's channel (which is what the server records as the client's address).
    List<Row> withDriverConfig =
        rows.stream()
            .filter(row -> clientOptions(row).get("DRIVER_CONFIG") != null)
            .collect(Collectors.toList());
    assertThat(withDriverConfig).hasSize(1);

    Row controlRow = withDriverConfig.get(0);
    InetSocketAddress controlAddress = controlConnectionAddress(session);
    assertThat(controlRow.getInetAddress("address")).isEqualTo(controlAddress.getAddress());
    assertThat(controlRow.getInt("port")).isEqualTo(controlAddress.getPort());

    // Its value round-trips through the server intact as the stage-2 payload: valid JSON carrying
    // the schema version and the full configuration.
    JsonNode report = assertDriverConfigPayload(clientOptions(controlRow).get("DRIVER_CONFIG"));

    // The report is configuration, not observation, so every field reads the same on both backends
    // — including the server-side internal-query timeout, even though only ScyllaDB actually gets
    // the "USING TIMEOUT" clause that turns advanced.metadata.schema.request-timeout into a
    // server-side limit.
    JsonNode timeout = report.path("control-plane").path("queries").path("system").path("timeout");
    assertThat(timeout.path("server-side-ms").asLong()).isPositive();

    // Likewise the shard-awareness intent: a property of the configuration, not of the peer.
    assertThat(
            report.path("connection").path("pool").path("shard-aware").path("enabled").asBoolean())
        .isTrue();
  }

  /**
   * The local address of the control connection's channel — the source address of that TCP
   * connection, and therefore what the server records in the clients table's {@code address} and
   * {@code port} columns (CCM connects directly, with no proxy or address translation in between).
   */
  private InetSocketAddress controlConnectionAddress(CqlSession session) {
    return (InetSocketAddress)
        ((InternalDriverContext) session.getContext())
            .getControlConnection()
            .channel()
            .localAddress();
  }

  /**
   * The {@code SESSION_ID} this session reports, read from the session-wide startup options — the
   * same map the driver copies into every connection's {@code STARTUP}.
   */
  private String sessionId(CqlSession session) {
    String sessionId =
        ((InternalDriverContext) session.getContext())
            .getStartupOptions()
            .get(StartupOptionsBuilder.SESSION_ID_KEY);
    assertThat(sessionId).isNotNull();
    return sessionId;
  }

  /**
   * The rows in the clients table that belong to this driver session's connections: this driver, in
   * a {@code READY} state, and carrying <em>this</em> session's {@code SESSION_ID}.
   *
   * <p>Scoping on the id value matters: {@code SESSION_ID} is sent unconditionally by every driver
   * session, and this class shares its CCM cluster with the other parallelizable ITs, so a
   * key-presence filter would also match their connections. The {@code READY} filter is what
   * excludes the transient protocol-version negotiation attempts (closed immediately).
   */
  private List<Row> driverConnections(CqlSession session) {
    String sessionId = sessionId(session);
    return session
        .execute(
            "SELECT address, port, connection_stage, driver_name, client_options FROM "
                + clientsTable())
        .all()
        .stream()
        .filter(row -> DRIVER_NAME.equals(row.getString("driver_name")))
        // connection_stage casing differs across backends; compare case-insensitively.
        .filter(row -> "READY".equalsIgnoreCase(row.getString("connection_stage")))
        .filter(row -> sessionId.equals(clientOptions(row).get("SESSION_ID")))
        .collect(Collectors.toList());
  }

  private Map<String, String> clientOptions(Row row) {
    Map<String, String> options = row.getMap("client_options", String.class, String.class);
    return options == null ? Collections.emptyMap() : options;
  }
}
