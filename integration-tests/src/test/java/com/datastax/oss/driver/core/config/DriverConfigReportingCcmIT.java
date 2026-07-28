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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * (b) {@code DRIVER_CONFIG} is stored for exactly one connection (the control connection).
 *
 * <p>Runs on both backends, asserting identical behavior — only the table that exposes the stored
 * options differs: ScyllaDB uses {@code system.clients.client_options}, while Apache Cassandra
 * exposes it in {@code system_views.clients.client_options} (added in Cassandra 4.1).
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

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

    // (a) Every connection carries SESSION_ID, and all of them share a single value (one session).
    Set<String> sessionIds =
        rows.stream().map(row -> clientOptions(row).get("SESSION_ID")).collect(Collectors.toSet());
    assertThat(sessionIds).doesNotContainNull().hasSize(1);

    // (b) DRIVER_CONFIG is stored for exactly one connection (the control connection), and its
    // value round-trips through the server intact as the stage-1 payload: valid JSON carrying
    // exactly the schema version.
    List<String> driverConfigs =
        rows.stream()
            .map(row -> clientOptions(row).get("DRIVER_CONFIG"))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    assertThat(driverConfigs).hasSize(1);
    assertStageOnePayload(driverConfigs.get(0));
  }

  /**
   * Asserts that a {@code DRIVER_CONFIG} value is the stage-1 payload: well-formed JSON whose
   * {@code version} is the integer {@code 1}. Guards against an incorrect schema version or a
   * malformed blob slipping through a mere key-presence check.
   */
  private static void assertStageOnePayload(String driverConfig) {
    JsonNode root;
    try {
      root = OBJECT_MAPPER.readTree(driverConfig);
    } catch (JsonProcessingException e) {
      throw new AssertionError("DRIVER_CONFIG is not valid JSON: " + driverConfig, e);
    }
    assertThat(root.path("version").isInt())
        .as("version is an integer in %s", driverConfig)
        .isTrue();
    assertThat(root.path("version").intValue()).isEqualTo(1);
  }

  /**
   * The rows in the clients table that belong to this driver session's connections: this driver, in
   * a {@code READY} state, and carrying the reporting {@code SESSION_ID}. Transient
   * protocol-version negotiation attempts (no driver identity, closed immediately) are excluded,
   * and their absence here is itself the confirmation that they leave no lingering session rows.
   */
  private List<Row> driverConnections(CqlSession session) {
    return session
        .execute(
            "SELECT address, port, connection_stage, driver_name, client_options FROM "
                + clientsTable())
        .all()
        .stream()
        .filter(row -> DRIVER_NAME.equals(row.getString("driver_name")))
        // connection_stage casing differs across backends; compare case-insensitively.
        .filter(row -> "READY".equalsIgnoreCase(row.getString("connection_stage")))
        .filter(row -> clientOptions(row).containsKey("SESSION_ID"))
        .collect(Collectors.toList());
  }

  private Map<String, String> clientOptions(Row row) {
    Map<String, String> options = row.getMap("client_options", String.class, String.class);
    return options == null ? Collections.emptyMap() : options;
  }
}
