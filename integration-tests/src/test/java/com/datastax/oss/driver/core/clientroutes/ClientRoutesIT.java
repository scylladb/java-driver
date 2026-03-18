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
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirementRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesTopologyMonitor;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
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
 * (introduced in scylladb/scylladb#27323). Routes are populated via the ScyllaDB REST API (port
 * 10000, {@code /v2/client-routes}) rather than direct CQL writes to the system table.
 *
 * <p>The NLB simulation tests create ad-hoc CCM clusters with an NLB TCP proxy to verify end-to-end
 * behaviour including node replacement, mixed proxy/direct topologies, and SSL with hostname
 * verification through a proxy.
 */
@Category(IsolatedTests.class)
@ScyllaRequirement(
    minEnterprise = "2026.1",
    description = "system.client_routes requires ScyllaDB Enterprise >= 2026.1")
public class ClientRoutesIT {

  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesIT.class);

  private static final int NLB_BASE_PORT = 29042;
  private static final String NLB_ADDRESS = "127.254.254.254";
  private static final String CONNECTION_ID = "11111111-1111-1111-1111-111111111111";
  private static final String DC_NAME = "dc1";
  private static final String CERT_PASSWORD = "testpass";

  private static final BackendRequirementRule BACKEND_RULE = new BackendRequirementRule();

  public static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(1).build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE).withKeyspace(false).build();

  @ClassRule
  public static final TestRule CHAIN =
      RuleChain.outerRule(BACKEND_RULE).around(CCM_RULE).around(SESSION_RULE);

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

  private void postRoute(String connectionId, UUID hostId, String address, int port)
      throws IOException {
    postRoute(connectionId, hostId, address, port, null);
  }

  private void postRoute(
      String connectionId, UUID hostId, String address, int port, Integer tlsPort)
      throws IOException {
    StringBuilder json = new StringBuilder("[{");
    json.append("\"connection_id\":\"").append(connectionId).append("\",");
    json.append("\"host_id\":\"").append(hostId).append("\",");
    json.append("\"address\":\"").append(address).append("\",");
    json.append("\"port\":").append(port);
    if (tlsPort != null) {
      json.append(",\"tls_port\":").append(tlsPort);
    }
    json.append("}]");

    CcmBridge ccm = CCM_RULE.getCcmBridge();
    postJsonToApi("http://" + ccm.getNodeIpAddress(1) + ":10000/v2/client-routes", json.toString());
  }

  private void waitForRouteVisible(String connectionId) {
    waitForRoutesVisibleOnNode(CCM_RULE.getCcmBridge(), 1, 1, null, connectionId);
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

  private ClientRoutesTopologyMonitor handlerOf(CqlSession session) {
    ClientRoutesTopologyMonitor handler =
        (ClientRoutesTopologyMonitor)
            ((InternalDriverContext) session.getContext()).getTopologyMonitor();
    assertThat(handler)
        .as("ClientRoutesHandler must be non-null when ClientRoutesConfig is set")
        .isNotNull();
    return handler;
  }

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

  private InetSocketAddress tryResolve(ClientRoutesTopologyMonitor handler, UUID hostId) {
    try {
      return handler.resolve(hostId);
    } catch (IllegalStateException e) {
      return null;
    } catch (UnknownHostException e) {
      throw new RuntimeException("DNS resolution failed for host_id=" + hostId, e);
    }
  }

  private static class NodeClassification {
    final Map<UUID, InetSocketAddress> proxied = new HashMap<>();
    final Map<UUID, InetSocketAddress> direct = new HashMap<>();
    int proxiedConnected;
    int directConnected;
  }

  private NodeClassification classifyNodes(CqlSession session) {
    NodeClassification result = new NodeClassification();
    for (Node node : session.getMetadata().getNodes().values()) {
      InetSocketAddress addr = (InetSocketAddress) node.getEndPoint().resolve();
      String ip = addr.getAddress().getHostAddress();
      UUID hostId = node.getHostId();
      boolean connected = node.getOpenConnections() > 0;
      LOG.info(
          "Node {} endpoint: {} (ip={}, port={}, connected={})",
          hostId,
          addr,
          ip,
          addr.getPort(),
          connected);
      if (NLB_ADDRESS.equals(ip)) {
        result.proxied.put(hostId, addr);
        if (connected) result.proxiedConnected++;
      } else {
        result.direct.put(hostId, addr);
        if (connected) result.directConnected++;
      }
    }
    return result;
  }

  private void assertNodeClassification(
      CqlSession session, int expectedProxied, int expectedDirect) {
    NodeClassification c = classifyNodes(session);
    assertThat(c.proxied).as("Expected %d proxied nodes", expectedProxied).hasSize(expectedProxied);
    assertThat(c.direct).as("Expected %d direct nodes", expectedDirect).hasSize(expectedDirect);
    for (InetSocketAddress addr : c.direct.values()) {
      assertThat(addr.getPort()).as("Direct node should use native transport port").isEqualTo(9042);
    }
  }

  private void awaitConnectedNodes(CqlSession session, int expectedProxied, int expectedDirect) {
    await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .until(
            () -> {
              NodeClassification c = classifyNodes(session);
              LOG.info(
                  "Waiting for connected nodes: proxied={}/{}, direct={}/{}",
                  c.proxiedConnected,
                  expectedProxied,
                  c.directConnected,
                  expectedDirect);
              return c.proxiedConnected >= expectedProxied && c.directConnected >= expectedDirect;
            });
  }

  private CqlSession openNlbSession(ClientRoutesConfig config, NlbSimulator nlb) {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactPoint(new InetSocketAddress(nlb.getBindAddress(), nlb.getDiscoveryPort()))
            .withClientRoutesConfig(config)
            .withConfigLoader(
                SessionUtils.configLoaderBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(15))
                    .withDuration(
                        DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(15))
                    .build())
            .build();
  }

  private void assertQueryWorks(CqlSession session) {
    ResultSet rs = session.execute("SELECT release_version FROM system.local WHERE key='local'");
    Row row = rs.one();
    assertThat(row).as("Query via NLB should return a result").isNotNull();
  }

  private boolean querySucceeds(CqlSession session) {
    try {
      ResultSet rs = session.execute("SELECT release_version FROM system.local WHERE key='local'");
      return rs.one() != null;
    } catch (AllNodesFailedException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("querySucceeds: unexpected exception", e);
      return false;
    }
  }

  private Map<Integer, UUID> collectHostIds(CcmBridge ccm, int nodeCount) {
    return collectHostIds(ccm, nodeCount, null);
  }

  private Map<Integer, UUID> collectHostIds(CcmBridge ccm, int nodeCount, String truststorePath) {
    Map<Integer, UUID> hostIds = new HashMap<>();

    Map<String, Integer> ipToNodeId = new HashMap<>();
    for (int i = 1; i <= nodeCount; i++) {
      ipToNodeId.put(ccm.getNodeIpAddress(i), i);
    }

    ProgrammaticDriverConfigLoaderBuilder loaderBuilder =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    if (truststorePath != null) {
      loaderBuilder
          .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
          .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true)
          .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, truststorePath)
          .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, CERT_PASSWORD);
    }

    try (CqlSession adminSession =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactPoint(new InetSocketAddress(ccm.getNodeIpAddress(1), 9042))
                .withConfigLoader(loaderBuilder.build())
                .build()) {
      for (Node node : adminSession.getMetadata().getNodes().values()) {
        InetSocketAddress addr = (InetSocketAddress) node.getEndPoint().resolve();
        String ip = addr.getAddress().getHostAddress();
        Integer nodeId = ipToNodeId.get(ip);
        if (nodeId != null && node.getHostId() != null) {
          hostIds.put(nodeId, node.getHostId());
        }
      }
    }

    return hostIds;
  }

  private void postJsonToApi(String apiUrl, String jsonBody) throws IOException {
    LOG.info("Posting to {}: {}", apiUrl, jsonBody);

    HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
    try {
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);
      conn.setConnectTimeout(5000);
      conn.setReadTimeout(5000);

      try (OutputStream os = conn.getOutputStream()) {
        os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
      }

      int responseCode = conn.getResponseCode();
      if (responseCode >= 400) {
        StringBuilder errorBody = new StringBuilder();
        try (BufferedReader br =
            new BufferedReader(
                new InputStreamReader(conn.getErrorStream(), StandardCharsets.UTF_8))) {
          String line;
          while ((line = br.readLine()) != null) {
            errorBody.append(line);
          }
        }
        throw new IOException(
            "Failed to POST to " + apiUrl + ": HTTP " + responseCode + " - " + errorBody);
      }

      // Drain response body to allow connection reuse
      try (InputStream is = conn.getInputStream()) {
        byte[] buf = new byte[1024];
        while (is.read(buf) >= 0) {
          // discard
        }
      } catch (IOException ignored) {
        // no body to drain
      }

      LOG.info("POST successful to {} (HTTP {})", apiUrl, responseCode);
    } finally {
      conn.disconnect();
    }
  }

  private void postClientRoutes(CcmBridge ccm, Map<Integer, UUID> hostIds, NlbSimulator nlb)
      throws IOException {
    if (hostIds.isEmpty()) {
      throw new IllegalStateException("No active nodes to post routes to");
    }

    StringBuilder json = new StringBuilder("[");
    boolean first = true;
    for (Map.Entry<Integer, UUID> entry : hostIds.entrySet()) {
      int nodeId = entry.getKey();
      UUID hostId = entry.getValue();
      int proxyPort = nlb.getNodePort(nodeId);

      if (!first) json.append(",");
      first = false;
      json.append("{")
          .append("\"connection_id\":\"")
          .append(CONNECTION_ID)
          .append("\",")
          .append("\"host_id\":\"")
          .append(hostId)
          .append("\",")
          .append("\"address\":\"")
          .append(NLB_ADDRESS)
          .append("\",")
          .append("\"port\":")
          .append(proxyPort)
          .append("}");
    }
    json.append("]");

    // Post to ALL active nodes to ensure routes are available cluster-wide,
    // regardless of which node the NLB routes the control connection to.
    // system.client_routes may use local storage or have replication delay,
    // so posting to a single node risks the session connecting to a node
    // that doesn't have the routes yet.
    for (int nodeId : hostIds.keySet()) {
      postJsonToApi(
          "http://" + ccm.getNodeIpAddress(nodeId) + ":10000/v2/client-routes", json.toString());
    }
  }

  private void waitForRoutesVisible(CcmBridge ccm, int expectedCount) {
    waitForRoutesVisibleOnNode(ccm, 1, expectedCount, null, CONNECTION_ID);
  }

  private void waitForRoutesVisibleOnAllNodes(
      CcmBridge ccm, Collection<Integer> nodeIds, int expectedCount) {
    waitForRoutesVisibleOnAllNodes(ccm, nodeIds, expectedCount, null);
  }

  private void waitForRoutesVisibleOnAllNodes(
      CcmBridge ccm, Collection<Integer> nodeIds, int expectedCount, String truststorePath) {
    for (int nodeId : nodeIds) {
      waitForRoutesVisibleOnNode(ccm, nodeId, expectedCount, truststorePath, CONNECTION_ID);
    }
  }

  private void waitForRoutesVisibleOnNode(
      CcmBridge ccm, int nodeId, int expectedCount, String truststorePath, String connectionId) {
    ProgrammaticDriverConfigLoaderBuilder loaderBuilder =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5));
    if (truststorePath != null) {
      loaderBuilder
          .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
          .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, true)
          .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, truststorePath)
          .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, CERT_PASSWORD);
    }
    try (CqlSession adminSession =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactPoint(new InetSocketAddress(ccm.getNodeIpAddress(nodeId), 9042))
                .withConfigLoader(loaderBuilder.build())
                .build()) {
      await()
          .atMost(10, TimeUnit.SECONDS)
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .until(
              () -> {
                ResultSet rs =
                    adminSession.execute(
                        SimpleStatement.newInstance(
                            "SELECT * FROM system.client_routes WHERE connection_id = ?"
                                + " ALLOW FILTERING",
                            connectionId));
                long count = rs.all().size();
                LOG.info(
                    "Waiting for routes on node {}: found {} of {} expected",
                    nodeId,
                    count,
                    expectedCount);
                return count >= expectedCount;
              });
    }
  }

  @Test
  public void should_load_routes_from_table_on_init() throws Exception {
    String connectionId = UUID.randomUUID().toString();
    String nodeAddr = nodeAddress();

    UUID hostId;
    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
      Row localRow = admin.execute("SELECT host_id FROM system.local WHERE key='local'").one();
      assertThat(localRow).isNotNull();
      hostId = localRow.getUuid("host_id");
      assertThat(hostId).isNotNull();
    }

    postRoute(connectionId, hostId, nodeAddr, 9042);
    waitForRouteVisible(connectionId);

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, nodeAddr))
            .build();

    try (CqlSession session = openClientRoutesSession(config)) {
      ResultSet rs = session.execute("SELECT release_version FROM system.local WHERE key='local'");
      assertThat(rs.one()).isNotNull();

      InetSocketAddress translated = handlerOf(session).resolve(hostId);
      assertThat(translated)
          .as("Handler should have a translated address for host_id=%s", hostId)
          .isNotNull();
      assertThat(translated.getPort()).isEqualTo(9042);

      LOG.info("Translated host_id={} -> {}", hostId, translated);
    }
  }

  @Test
  public void should_refresh_routes_after_table_update() throws Exception {
    String connectionId = UUID.randomUUID().toString();
    UUID hostId = UUID.randomUUID();
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, nodeAddr))
            .build();

    try (CqlSession session = openClientRoutesSession(config)) {
      ClientRoutesTopologyMonitor handler = handlerOf(session);

      assertThat(handler.resolve(hostId)).isNull();

      postRoute(connectionId, hostId, nodeAddr, 9042);
      waitForRouteVisible(connectionId);
      handler.refresh().toCompletableFuture().get(10, TimeUnit.SECONDS);

      await()
          .atMost(15, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                InetSocketAddress resolved = handler.resolve(hostId);
                assertThat(resolved).isNotNull();
                assertThat(resolved.getAddress().getHostAddress()).isEqualTo(nodeAddr);
                assertThat(resolved.getPort()).isEqualTo(9042);
              });
    }
  }

  @Test
  public void should_start_session_when_table_is_empty() throws UnknownHostException {
    String nodeAddr = nodeAddress();
    String connectionId = UUID.randomUUID().toString();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, nodeAddr))
            .build();

    try (CqlSession session = openClientRoutesSession(config)) {
      ResultSet rs = session.execute("SELECT release_version FROM system.local WHERE key='local'");
      assertThat(rs.one()).isNotNull();

      // Route cache should be empty (no routes posted)
      assertThat(handlerOf(session).resolve(UUID.randomUUID())).isNull();

      // Session should still have connected nodes (via direct connection, not routes)
      assertThat(session.getMetadata().getNodes())
          .as("Session should have connected nodes even when routes table is empty")
          .isNotEmpty();
    }
  }

  @Test
  public void should_select_non_ssl_port_when_ssl_not_configured() throws Exception {
    String connectionId = UUID.randomUUID().toString();
    UUID hostId = UUID.randomUUID();
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }

    postRoute(connectionId, hostId, nodeAddr, 9042, 9142);
    waitForRouteVisible(connectionId);

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, nodeAddr))
            .build();

    try (CqlSession session = openClientRoutesSession(config)) {
      ClientRoutesTopologyMonitor handler = handlerOf(session);

      InetSocketAddress resolved = handler.resolve(hostId);
      assertThat(resolved).isNotNull();
      assertThat(resolved.getPort()).isEqualTo(9042);
    }
  }

  @Test
  public void should_refresh_routes_after_control_connection_reconnect() throws Exception {
    String connectionId = UUID.randomUUID().toString();
    UUID hostId = UUID.randomUUID();
    String nodeAddr = nodeAddress();

    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }

    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, nodeAddr))
            .build();

    try (CqlSession session = openClientRoutesSession(config)) {
      ClientRoutesTopologyMonitor handler = handlerOf(session);
      assertThat(tryResolve(handler, hostId)).isNull();

      postRoute(connectionId, hostId, nodeAddr, 9042);
      waitForRouteVisible(connectionId);

      ((InternalDriverContext) session.getContext()).getControlConnection().reconnectNow();

      await().atMost(15, TimeUnit.SECONDS).until(() -> tryResolve(handler, hostId) != null);

      assertThat(tryResolve(handler, hostId))
          .as("Route should be available after reconnect and refresh for host_id=%s", hostId)
          .isNotNull()
          .extracting(InetSocketAddress::getPort)
          .isEqualTo(9042);
    }
  }

  @Test
  public void should_survive_full_node_replacement_through_nlb() throws Exception {
    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }
    try (CcmBridge ccm = CcmBridge.builder().withNodes(2).build()) {
      ccm.create();
      ccm.start();

      NlbSimulator nlb = new NlbSimulator(ccm, NLB_ADDRESS, NLB_BASE_PORT);
      try {
        nlb.addNode(1);
        nlb.addNode(2);

        Map<Integer, UUID> hostIds = collectHostIds(ccm, 2);
        postClientRoutes(ccm, hostIds, nlb);
        waitForRoutesVisibleOnAllNodes(ccm, hostIds.keySet(), hostIds.size());
        ClientRoutesConfig config =
            ClientRoutesConfig.builder()
                .addEndpoint(new ClientRouteProxy(CONNECTION_ID, NLB_ADDRESS))
                .build();

        try (CqlSession session = openNlbSession(config, nlb)) {
          assertQueryWorks(session);

          assertThat(((InternalDriverContext) session.getContext()).getTopologyMonitor())
              .isInstanceOf(ClientRoutesTopologyMonitor.class);
          assertNodeClassification(session, 2, 0);
          ccm.add(3, DC_NAME);
          ccm.add(4, DC_NAME);

          await()
              .atMost(30, TimeUnit.SECONDS)
              .pollInterval(2, TimeUnit.SECONDS)
              .until(() -> session.getMetadata().getNodes().size() >= 4);

          nlb.addNode(3);
          nlb.addNode(4);

          Map<Integer, UUID> allHostIds = collectHostIds(ccm, 4);
          postClientRoutes(ccm, allHostIds, nlb);
          awaitConnectedNodes(session, 4, 0);
          assertQueryWorks(session);

          ccm.decommission(1);
          nlb.removeNode(1);
          allHostIds.remove(1);
          postClientRoutes(ccm, allHostIds, nlb);
          waitForRoutesVisibleOnAllNodes(ccm, allHostIds.keySet(), allHostIds.size());

          await()
              .atMost(30, TimeUnit.SECONDS)
              .pollInterval(2, TimeUnit.SECONDS)
              .until(() -> querySucceeds(session) && session.getMetadata().getNodes().size() <= 3);

          ccm.decommission(2);
          nlb.removeNode(2);
          allHostIds.remove(2);
          postClientRoutes(ccm, allHostIds, nlb);
          waitForRoutesVisibleOnAllNodes(ccm, allHostIds.keySet(), allHostIds.size());

          await()
              .atMost(30, TimeUnit.SECONDS)
              .pollInterval(2, TimeUnit.SECONDS)
              .until(() -> querySucceeds(session) && session.getMetadata().getNodes().size() <= 2);

          awaitConnectedNodes(session, 2, 0);
          assertQueryWorks(session);
          assertNodeClassification(session, 2, 0);

          for (int i = 0; i < 10; i++) {
            assertQueryWorks(session);
          }
        }
      } finally {
        nlb.close();
      }
    }
  }

  @Test
  public void should_select_tls_port_when_ssl_configured() throws Exception {
    // Verify system.client_routes exists using the shared cluster (same Scylla version)
    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }

    // Create a dedicated SSL-enabled cluster
    try (CcmBridge ccm = CcmBridge.builder().withNodes(1).withSsl().build()) {
      ccm.create();
      ccm.start();

      String nodeAddr = ccm.getNodeIpAddress(1);
      String truststorePath = CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath();
      String truststorePassword = CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;

      // Collect host_id, post route, and wait for visibility using a single SSL admin session
      String connectionId = UUID.randomUUID().toString();
      UUID hostId;
      try (CqlSession sslAdmin =
          (CqlSession)
              SessionUtils.baseBuilder()
                  .addContactPoint(new InetSocketAddress(nodeAddr, 9042))
                  .withConfigLoader(
                      SessionUtils.configLoaderBuilder()
                          .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                          .withClass(
                              DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS,
                              DefaultSslEngineFactory.class)
                          .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, false)
                          .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, truststorePath)
                          .withString(
                              DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, truststorePassword)
                          .build())
                  .build()) {
        Row localRow = sslAdmin.execute("SELECT host_id FROM system.local WHERE key='local'").one();
        assertThat(localRow).isNotNull();
        hostId = localRow.getUuid("host_id");
        assertThat(hostId).isNotNull();

        // Post route: port=19999 (invalid non-SSL) and tls_port=9042 (correct SSL port).
        // If the handler incorrectly picks port instead of tls_port, connections will fail.
        StringBuilder json = new StringBuilder("[{");
        json.append("\"connection_id\":\"").append(connectionId).append("\",");
        json.append("\"host_id\":\"").append(hostId).append("\",");
        json.append("\"address\":\"").append(nodeAddr).append("\",");
        json.append("\"port\":19999,\"tls_port\":9042}]");
        postJsonToApi("http://" + nodeAddr + ":10000/v2/client-routes", json.toString());

        // Wait for route visibility
        await()
            .atMost(10, TimeUnit.SECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .until(
                () -> {
                  ResultSet rs =
                      sslAdmin.execute(
                          SimpleStatement.newInstance(
                              "SELECT * FROM system.client_routes WHERE connection_id = ?"
                                  + " ALLOW FILTERING",
                              connectionId));
                  return rs.all().size() >= 1;
                });
      }

      // Open session with SSL and client routes
      ClientRoutesConfig config =
          ClientRoutesConfig.builder()
              .addEndpoint(new ClientRouteProxy(connectionId, nodeAddr))
              .build();
      try (CqlSession session =
          (CqlSession)
              SessionUtils.baseBuilder()
                  .addContactPoint(new InetSocketAddress(nodeAddr, 9042))
                  .withClientRoutesConfig(config)
                  .withConfigLoader(
                      SessionUtils.configLoaderBuilder()
                          .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                          .withClass(
                              DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS,
                              DefaultSslEngineFactory.class)
                          .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, false)
                          .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, truststorePath)
                          .withString(
                              DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, truststorePassword)
                          .build())
                  .build()) {

        ClientRoutesTopologyMonitor handler = handlerOf(session);
        InetSocketAddress resolved = handler.resolve(hostId);
        assertThat(resolved)
            .as("Handler should resolve route for host_id=%s with SSL configured", hostId)
            .isNotNull();
        assertThat(resolved.getPort())
            .as("Should select tls_port (9042) when SSL is configured, not port (19999)")
            .isEqualTo(9042);

        // Verify actual SSL query works through the client routes endpoint
        ResultSet rs =
            session.execute("SELECT release_version FROM system.local WHERE key='local'");
        assertThat(rs.one()).isNotNull();
      }
    }
  }

  @Test
  public void should_use_shard_awareness_through_pp2_nlb() throws Exception {
    // Verify server supports client_routes
    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }
    try (CcmBridge ccm = CcmBridge.builder().withNodes(1).build()) {
      ccm.create();
      ccm.start();

      // Create PP2-enabled NLB
      NlbSimulator nlb =
          new NlbSimulator(ccm, NLB_ADDRESS, NLB_BASE_PORT, /* proxyProtocol= */ true);
      try {
        nlb.addNode(1);
        Map<Integer, UUID> hostIds = collectHostIds(ccm, 1);
        postClientRoutes(ccm, hostIds, nlb);
        waitForRoutesVisibleOnAllNodes(ccm, hostIds.keySet(), hostIds.size());

        ClientRoutesConfig config =
            ClientRoutesConfig.builder()
                .addEndpoint(new ClientRouteProxy(CONNECTION_ID, NLB_ADDRESS))
                .withProxyProtocol(true)
                .build();

        try (CqlSession session = openNlbSession(config, nlb)) {
          assertQueryWorks(session);

          // With PP2, the NLB forwards the driver's original source port to ScyllaDB.
          // The driver binds shard-specific local ports (advanced shard awareness, on by default),
          // so ScyllaDB can route each connection to the correct shard. Verify the pool works
          // correctly by running queries and waiting for the full pool to be established.
          Node node = session.getMetadata().getNodes().values().iterator().next();
          int shardCount =
              node.getShardingInfo() != null ? node.getShardingInfo().getShardsCount() : 1;

          // Wait for full shard-aware pool to be established (one connection per shard)
          await()
              .atMost(30, TimeUnit.SECONDS)
              .pollInterval(1, TimeUnit.SECONDS)
              .until(() -> node.getOpenConnections() >= shardCount);

          // Run queries to verify shard-aware routing works end-to-end through the PP2 NLB
          for (int i = 0; i < 20; i++) {
            assertQueryWorks(session);
          }
        }
      } finally {
        nlb.close();
      }
    }
  }

  @Test
  public void should_work_with_mixed_proxy_and_direct_nodes() throws Exception {
    try (CqlSession admin = openAdminSession()) {
      requireSystemClientRoutesTable(admin);
    }
    try (CcmBridge ccm = CcmBridge.builder().withNodes(3).build()) {
      ccm.create();
      ccm.start();

      NlbSimulator nlb = new NlbSimulator(ccm, NLB_ADDRESS, NLB_BASE_PORT);
      try {
        nlb.addNode(1);

        Map<Integer, UUID> allHostIds = collectHostIds(ccm, 3);

        Map<Integer, UUID> proxiedHostIds = new HashMap<>();
        proxiedHostIds.put(1, allHostIds.get(1));
        postClientRoutes(ccm, proxiedHostIds, nlb);
        waitForRoutesVisible(ccm, 1);

        ClientRoutesConfig config =
            ClientRoutesConfig.builder()
                .addEndpoint(new ClientRouteProxy(CONNECTION_ID, NLB_ADDRESS))
                .build();

        try (CqlSession session = openNlbSession(config, nlb)) {
          assertQueryWorks(session);
          awaitConnectedNodes(session, 1, 2);
          assertNodeClassification(session, 1, 2);

          for (int i = 0; i < 20; i++) {
            assertQueryWorks(session);
          }
        }
      } finally {
        nlb.close();
      }
    }
  }
}
