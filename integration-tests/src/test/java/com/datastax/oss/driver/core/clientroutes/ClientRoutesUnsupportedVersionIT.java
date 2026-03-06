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
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.ScyllaOnly;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesTopologyMonitor;
import java.time.Duration;
import java.util.UUID;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Negative integration test for the PrivateLink client routes feature.
 *
 * <p>This test class is intentionally <em>not</em> gated with a minimum version requirement: it
 * must run on <em>older</em> ScyllaDB Enterprise versions (before 2026.1) where the {@code
 * system.client_routes} table does not exist.
 *
 * <p>The single test here verifies that:
 *
 * <ol>
 *   <li>{@code system.client_routes} is truly absent on the running server — if it is present, the
 *       test fails loudly to signal that the version gate in {@link ClientRoutesIT} needs updating.
 *   <li>The driver gracefully opens a session even when the table is missing (no crash on init).
 *   <li>The {@link ClientRoutesTopologyMonitor} returns {@code null} for all host IDs — the feature
 *       is not accidentally activated.
 * </ol>
 *
 * <p>This class is annotated with {@code @ScyllaOnly} because {@code system.client_routes} is
 * specific to ScyllaDB Enterprise (not yet available on OSS) and has no equivalent in Apache
 * Cassandra. The {@code maxEnterprise = "2026.1"} constraint ensures this test is skipped on
 * versions that <em>do</em> support the feature (it would have nothing meaningful to assert there,
 * and the positive tests in {@link ClientRoutesIT} cover that range).
 */
@Category(IsolatedTests.class)
@ScyllaOnly(description = "system.client_routes is a ScyllaDB Enterprise-only feature")
@ScyllaRequirement(
    maxEnterprise = "2026.1",
    description =
        "Negative test for Enterprise versions where system.client_routes does not exist. "
            + "Skipped on Enterprise >= 2026.1 (use ClientRoutesIT for those versions).")
public class ClientRoutesUnsupportedVersionIT {

  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesUnsupportedVersionIT.class);

  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE).withKeyspace(false).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  // ---- helpers -----------------------------------------------------------

  private String nodeAddress() {
    return CCM_RULE.getCcmBridge().getNodeIpAddress(1);
  }

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

  // ---- test --------------------------------------------------------------

  /**
   * Verifies that on a ScyllaDB Enterprise version that predates the {@code system.client_routes}
   * table (i.e. Enterprise &lt; 2026.1), the driver behaves correctly:
   *
   * <ul>
   *   <li>The system table does <em>not</em> exist — confirmed via {@code system_schema.tables}.
   *   <li>A session with {@link ClientRoutesConfig} can still be opened (graceful degradation).
   *   <li>The {@link ClientRoutesTopologyMonitor} stays empty; {@code translate()} returns {@code
   *       null}.
   * </ul>
   *
   * <p>The assertion on the absent table acts as a built-in guard against stale version gates: if
   * {@code system.client_routes} ever appears on a version thought to be unsupported, this test
   * fails with a clear message rather than silently passing.
   */
  @Test
  public void should_not_activate_on_unsupported_version() {
    // This test targets Scylla Enterprise only; OSS boundary is not yet confirmed.
    if (!CcmBridge.SCYLLA_ENTERPRISE) {
      throw new AssumptionViolatedException(
          "Negative client-routes test targets Scylla Enterprise only "
              + "(OSS feature boundary not yet confirmed). Skipping on OSS.");
    }

    try (CqlSession admin = openAdminSession()) {

      // ── Step 1 ── confirm the table is genuinely absent ───────────────────────────────────────
      // If system.client_routes already exists on this build, the maxEnterprise gate in this
      // class's @ScyllaRequirement is wrong (or the feature shipped earlier than 2026.1).
      // Fail with an explicit message so the gate can be corrected.
      Row tableRow =
          admin
              .execute(
                  "SELECT table_name FROM system_schema.tables"
                      + " WHERE keyspace_name='system' AND table_name='client_routes'")
              .one();
      assertThat(tableRow)
          .as(
              "system.client_routes MUST NOT exist on Enterprise < 2026.1 — "
                  + "if this assertion fails the @ScyllaRequirement(minEnterprise) gate in "
                  + "ClientRoutesIT (or maxEnterprise here) is out of date and must be corrected.")
          .isNull();

      // ── Step 2 ── session must still open (graceful degradation) ──────────────────────────────
      String connectionId = UUID.randomUUID().toString();
      String nodeAddr = nodeAddress();

      // Use the real default table name (system.client_routes) — no user-space mirror — so any
      // successful query result would mean the feature is genuinely active (it should not be).
      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRoutesEndpoint(connectionId, nodeAddr + ":9042"))
              .build();

      try (CqlSession session = openClientRoutesSession(config)) {
        // The session must be fully usable despite the missing table.
        ResultSet rs =
            session.execute("SELECT release_version FROM system.local WHERE key='local'");
        assertThat(rs.one())
            .as("Session opened with ClientRoutesConfig must be usable even on unsupported server")
            .isNotNull();

        // ── Step 3 ── translation must return null ────────────────────────────────────────────
        ClientRoutesTopologyMonitor handler =
            ((InternalDriverContext) session.getContext()).getClientRoutesHandler();
        assertThat(handler)
            .as("ClientRoutesHandler must be non-null whenever ClientRoutesConfig is set")
            .isNotNull();

        UUID anyHostId = UUID.randomUUID();
        try {
          handler.resolve(anyHostId);
          fail(
              "ClientRoutesTopologyMonitor.resolve() must throw on an unsupported server — "
                  + "the feature must NOT be silently active when system.client_routes is absent");
        } catch (IllegalStateException | java.net.UnknownHostException e) {
          assertThat(e.getMessage()).contains("No client route found");
        }

        LOG.info(
            "Confirmed: client-routes feature correctly inactive on unsupported Enterprise {}",
            CCM_RULE.getCcmBridge().getScyllaUnparsedVersion().orElse("<unknown>"));
      }
    }
  }
}
