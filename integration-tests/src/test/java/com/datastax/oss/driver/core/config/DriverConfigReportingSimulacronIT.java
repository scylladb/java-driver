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
import static com.datastax.oss.driver.internal.core.context.DefaultDriverConfigReporter.DRIVER_CONFIG_KEY;
import static com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder.CLIENT_ID_KEY;
import static com.datastax.oss.driver.internal.core.context.StartupOptionsBuilder.SESSION_ID_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End-to-end check of driver-configuration reporting against a mock server, asserting on the actual
 * CQL {@code STARTUP} frames the driver sends.
 *
 * <p>Simulacron records every inbound frame with its originating client connection, so we can
 * verify that:
 *
 * <ul>
 *   <li>{@code SESSION_ID} is present (and identical) on <em>every</em> session connection,
 *       <em>whatever</em> {@code advanced.driver-config-reporting.enabled} is set to — it is an
 *       innate startup option, not part of configuration reporting;
 *   <li>{@code DRIVER_CONFIG} is present only on the control connection, and only when {@code
 *       advanced.driver-config-reporting.enabled} is true (which is the default).
 * </ul>
 *
 * <p>The control connection is identified independently of the reported options: it is the only
 * connection that issues a {@code REGISTER} frame (to subscribe to cluster events).
 *
 * <p>Assertions are scoped to the session's real connections, identified by the {@code CLIENT_ID}
 * startup option that the driver always sends (independently of config reporting). This excludes
 * the short-lived connections opened by protocol-version negotiation: when the protocol version is
 * not pinned (the default), the driver first tries higher versions (DSE_V2, DSE_V1, V5) that
 * ScyllaDB rejects before the handshake completes, and Simulacron records each such rejected
 * attempt as a bare {@code STARTUP} ({@code CQL_VERSION} only) that carries no driver identity.
 */
@Category(ParallelizableTests.class)
public class DriverConfigReportingSimulacronIT {

  // A single node yields one dedicated control connection plus a pool connection (local.size
  // defaults to 1), i.e. at least two distinct session connections of which only the control one
  // registers for events.
  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @Before
  public void clearLogs() {
    SIMULACRON_RULE.cluster().clearLogs();
  }

  @Test
  public void should_report_session_id_on_all_connections_and_driver_config_only_on_control() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, true)
            .build();
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, loader)) {
      awaitControlAndPoolConnected();

      SocketAddress controlConnection = controlConnection().orElseThrow(AssertionError::new);
      List<QueryLog> startups = sessionStartups();

      // Sanity: we are actually observing more than one connection (control + at least one pool),
      // otherwise the "only on the control connection" assertion below would be vacuous.
      assertThat(distinctConnections(startups)).isGreaterThanOrEqualTo(2);

      // SESSION_ID is present on every session connection and has the same value everywhere.
      assertThat(startups).allSatisfy(log -> assertThat(options(log)).containsKey(SESSION_ID_KEY));
      assertThat(startups.stream().map(log -> options(log).get(SESSION_ID_KEY)).distinct())
          .hasSize(1);

      // DRIVER_CONFIG is present on exactly one connection, and that connection is the control one.
      List<QueryLog> withDriverConfig =
          startups.stream()
              .filter(log -> options(log).containsKey(DRIVER_CONFIG_KEY))
              .collect(Collectors.toList());
      assertThat(withDriverConfig).hasSize(1);
      assertThat(withDriverConfig.get(0).getConnection()).isEqualTo(controlConnection);

      // The payload is the stage-2 report: valid JSON carrying the schema version and the full
      // configuration (checked here via the always-present query.load-balancing.policy group).
      assertDriverConfigPayload(options(withDriverConfig.get(0)).get(DRIVER_CONFIG_KEY));
    }
  }

  @Test
  public void should_still_report_session_id_when_driver_config_reporting_is_disabled() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.DRIVER_CONFIG_REPORTING_ENABLED, false)
            .build();
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE, loader)) {
      awaitControlAndPoolConnected();

      List<QueryLog> startups = sessionStartups();
      assertThat(distinctConnections(startups)).isGreaterThanOrEqualTo(2);

      // SESSION_ID does not depend on the option: it is still sent, with a single shared value...
      assertThat(startups).allSatisfy(log -> assertThat(options(log)).containsKey(SESSION_ID_KEY));
      assertThat(startups.stream().map(log -> options(log).get(SESSION_ID_KEY)).distinct())
          .hasSize(1);
      // ... while the configuration itself is reported nowhere.
      assertThat(startups)
          .allSatisfy(log -> assertThat(options(log)).doesNotContainKey(DRIVER_CONFIG_KEY));
    }
  }

  @Test
  public void should_report_driver_config_by_default() {
    // No override for advanced.driver-config-reporting.enabled: exercises the shipped default.
    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      awaitControlAndPoolConnected();

      List<QueryLog> startups = sessionStartups();
      assertThat(distinctConnections(startups)).isGreaterThanOrEqualTo(2);

      assertThat(startups).allSatisfy(log -> assertThat(options(log)).containsKey(SESSION_ID_KEY));
      List<QueryLog> withDriverConfig =
          startups.stream()
              .filter(log -> options(log).containsKey(DRIVER_CONFIG_KEY))
              .collect(Collectors.toList());
      assertThat(withDriverConfig).hasSize(1);
      assertDriverConfigPayload(options(withDriverConfig.get(0)).get(DRIVER_CONFIG_KEY));
    }
  }

  /**
   * The {@code STARTUP} frames of the session's real connections (control + pool), identified by
   * the always-present {@code CLIENT_ID} option; excludes protocol-version negotiation attempts.
   */
  private List<QueryLog> sessionStartups() {
    return SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
        .filter(log -> log.getFrame().message instanceof Startup)
        .filter(log -> options(log).containsKey(CLIENT_ID_KEY))
        .collect(Collectors.toList());
  }

  /** The {@code STARTUP} options carried by a recorded frame. */
  private Map<String, String> options(QueryLog log) {
    return ((Startup) log.getFrame().message).options;
  }

  /** The connection that issued a {@code REGISTER} frame, i.e. the control connection. */
  private Optional<SocketAddress> controlConnection() {
    return SIMULACRON_RULE.cluster().getLogs().getQueryLogs().stream()
        .filter(log -> log.getFrame().message instanceof Register)
        .map(QueryLog::getConnection)
        .findFirst();
  }

  private long distinctConnections(List<QueryLog> logs) {
    return logs.stream().map(QueryLog::getConnection).distinct().count();
  }

  /**
   * Waits until both the control connection (identified by its {@code REGISTER}) and at least one
   * pool connection have sent their {@code STARTUP}, since pool connections may finish initializing
   * slightly after the session builder returns.
   */
  private void awaitControlAndPoolConnected() {
    await()
        .pollInterval(100, TimeUnit.MILLISECONDS)
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () -> controlConnection().isPresent() && distinctConnections(sessionStartups()) >= 2);
  }
}
