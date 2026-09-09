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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.clientroutes.ClientRouteRecord;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.MalformedInputException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
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
   *
   * <p>Also overrides {@link #runAdminQuery} to capture issued query strings and return an empty
   * result, so tests can verify which queries were executed without touching the network.
   */
  @SuppressWarnings("NewClassNamingConvention")
  static class TestableClientRoutesTopologyMonitor extends ClientRoutesTopologyMonitor {
    final List<String> capturedQueries = new ArrayList<>();

    private static final AdminResult EMPTY_RESULT = AdminResultTestHelper.mockResult();

    volatile AdminResult nextQueryResult = EMPTY_RESULT;
    volatile boolean failNextQuery = false;

    TestableClientRoutesTopologyMonitor(InternalDriverContext ctx, ClientRoutesConfig cfg) {
      super(ctx, cfg);
    }

    void setRoutes(Map<UUID, ClientRouteRecord> routes) {
      setResolvedRoutes(routes);
    }

    Map<UUID, ClientRouteRecord> getRoutes() {
      return getResolvedRoutes();
    }

    void mergeRoutesForTest(Map<UUID, ClientRouteRecord> incoming) {
      mergeRoutes(incoming);
    }

    void removeRouteForTest(UUID hostId) {
      removeRoute(hostId);
    }

    void setNextQueryResult(AdminResult result) {
      this.nextQueryResult = result;
    }

    String lastCapturedQuery() {
      return capturedQueries.get(capturedQueries.size() - 1);
    }

    @Override
    @NonNull
    protected CompletionStage<AdminResult> runAdminQuery(
        @NonNull DriverChannel channel, @NonNull String queryString, @NonNull Duration timeout) {
      capturedQueries.add(queryString);
      if (failNextQuery) {
        CompletableFuture<AdminResult> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("simulated failure"));
        return failed;
      }
      return CompletableFuture.completedFuture(nextQueryResult);
    }
  }

  private EventBus eventBus;
  private String connectionId;

  @Before
  public void setup() {
    eventBus = new EventBus("test");
    connectionId = UUID.randomUUID().toString();

    when(context.getSessionName()).thenReturn("test-session");
    when(context.getEventBus()).thenReturn(eventBus);
    when(context.getControlConnection()).thenReturn(controlConnection);
    when(context.getConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(defaultProfile.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT)).thenReturn(false);
    when(context.getSslEngineFactory()).thenReturn(Optional.empty());
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, "host1"))
            .build();
    handler = new TestableClientRoutesTopologyMonitor(context, config);
  }

  /**
   * Stubs the control connection for init and calls {@link
   * TestableClientRoutesTopologyMonitor#init()}. Only tests that exercise the reconnect / event
   * path need this; tests that manipulate the routes cache directly should not call it.
   */
  private void initHandler() {
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));
    when(controlConnection.init(anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    handler.init();
  }

  /**
   * Mocks a {@code system.client_routes} row. Stubs are lenient because callers use only the
   * columns their scenario reaches, and the class runs under the strict {@link MockitoJUnitRunner}.
   * A null {@code hostId} or {@code address} makes the corresponding {@code isNull()} answer true;
   * a null {@code port} makes {@code portColumn} absent; a null {@code connectionId} makes {@code
   * contains("connection_id")} answer false.
   */
  private static AdminRow mockRouteRow(
      UUID hostId, String address, String portColumn, Integer port, String connectionId) {
    AdminRow row = Mockito.mock(AdminRow.class);
    Mockito.lenient().when(row.isNull("host_id")).thenReturn(hostId == null);
    Mockito.lenient().when(row.getUuid("host_id")).thenReturn(hostId);
    Mockito.lenient().when(row.isNull("address")).thenReturn(address == null);
    Mockito.lenient().when(row.getString("address")).thenReturn(address);
    Mockito.lenient().when(row.isNull(portColumn)).thenReturn(port == null);
    Mockito.lenient().when(row.getInteger(portColumn)).thenReturn(port);
    Mockito.lenient().when(row.contains("connection_id")).thenReturn(connectionId != null);
    Mockito.lenient().when(row.isNull("connection_id")).thenReturn(connectionId == null);
    Mockito.lenient().when(row.getString("connection_id")).thenReturn(connectionId);
    return row;
  }

  /** Shorthand for the non-SSL case with no {@code connection_id}. */
  private static AdminRow mockRouteRow(UUID hostId, String address, Integer port) {
    return mockRouteRow(hostId, address, "port", port, null);
  }

  // ---- resolve() -------------------------------------------------------

  @Test
  public void should_return_null_for_unknown_host_id() throws UnknownHostException {
    assertThat(handler.resolve(UUID.randomUUID())).isNull();
  }

  @Test
  public void should_resolve_known_host_id() throws UnknownHostException {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "127.0.0.1", 9042)));

    InetSocketAddress result = handler.resolve(hostId);

    assertThat(result).isNotNull();
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_throw_after_close() {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "127.0.0.1", 9042)));

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
            hostId, new ClientRouteRecord(hostId, "this.host.does.not.exist.invalid", 9042)));

    assertThatThrownBy(() -> handler.resolve(hostId)).isInstanceOf(UnknownHostException.class);
  }

  @Test
  public void should_refresh_updates_routes() throws UnknownHostException {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();

    handler.setRoutes(ImmutableMap.of(hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042)));
    assertThat(handler.resolve(hostId1)).isNotNull();
    assertThat(handler.resolve(hostId2)).isNull();

    // Simulate a refresh that swaps in a different set of routes
    handler.setRoutes(ImmutableMap.of(hostId2, new ClientRouteRecord(hostId2, "127.0.0.2", 9042)));

    assertThat(handler.resolve(hostId1)).isNull();
    assertThat(handler.resolve(hostId2)).isNotNull();
  }

  // ---- Merge behavior tests -----------------------------------------------

  @Test
  public void should_preserve_existing_routes_on_merge() throws UnknownHostException {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();
    UUID hostId3 = UUID.randomUUID();

    // Initial routes: hostId1 and hostId2
    handler.setRoutes(
        ImmutableMap.of(
            hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042),
            hostId2, new ClientRouteRecord(hostId2, "127.0.0.2", 9042)));

    // Verify initial state
    assertThat(handler.resolve(hostId1).getPort()).isEqualTo(9042);
    assertThat(handler.resolve(hostId2).getPort()).isEqualTo(9042);
    assertThat(handler.resolve(hostId3)).isNull();

    // Simulate a targeted merge that adds hostId3
    Map<UUID, ClientRouteRecord> incoming = new HashMap<>();
    incoming.put(hostId3, new ClientRouteRecord(hostId3, "127.0.0.3", 9043));
    handler.mergeRoutesForTest(incoming);

    // All three hosts should now be resolvable
    assertThat(handler.resolve(hostId1).getPort()).isEqualTo(9042);
    assertThat(handler.resolve(hostId2).getPort()).isEqualTo(9042);
    assertThat(handler.resolve(hostId3).getPort()).isEqualTo(9043);
  }

  @Test
  public void should_update_existing_route_on_merge() throws UnknownHostException {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();

    // Initial routes
    handler.setRoutes(
        ImmutableMap.of(
            hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042),
            hostId2, new ClientRouteRecord(hostId2, "127.0.0.2", 9042)));

    // Verify initial port
    assertThat(handler.resolve(hostId1).getPort()).isEqualTo(9042);
    assertThat(handler.resolve(hostId2).getPort()).isEqualTo(9042);

    // Simulate a targeted update that changes hostId1's port
    Map<UUID, ClientRouteRecord> incoming = new HashMap<>();
    incoming.put(hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9999));
    handler.mergeRoutesForTest(incoming);

    // hostId1 should have new port, hostId2 should be unchanged
    assertThat(handler.resolve(hostId1).getPort()).isEqualTo(9999);
    assertThat(handler.resolve(hostId2).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_return_configured_connection_ids() {
    // The handler was created with one endpoint in setup()
    ClientRoutesConfig cfg = handler.getClientRoutesConfig();
    assertThat(cfg.getEndpoints()).hasSize(1);
    assertThat(cfg.getEndpoints().get(0).getConnectionId()).isNotNull();
  }

  // ---- Reconnect re-query tests -------------------------------------------

  @Test
  public void should_query_routes_on_init() {
    // init() pre-loads routes; verify one query was issued
    initHandler();

    assertThat(handler.capturedQueries).hasSize(1);
    String query = handler.capturedQueries.get(0);
    assertThat(query)
        .startsWith(
            "SELECT host_id, address, port, tls_port, connection_id FROM system.client_routes");
  }

  @Test
  public void should_requery_routes_on_refresh() {
    // init() issues the first query; refresh() should issue another
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    // Simulate reconnect-triggered refresh (called by ControlConnection.onSuccessfulReconnect)
    handler.refresh();

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit + 1);
  }

  @Test
  public void should_issue_full_scan_query_on_refresh() {
    initHandler();

    handler.refresh();

    // refresh() triggers queryClientRoutesAndCache(null, null) — a full scan scoped to the
    // configured connection IDs, with ALLOW FILTERING (no host_id filter)
    assertThat(handler.lastCapturedQuery())
        .contains("WHERE connection_id IN (")
        .contains(connectionId)
        .contains("ALLOW FILTERING")
        .doesNotContain("host_id IN");
  }

  @Test
  public void should_issue_targeted_query_on_client_routes_change_event_with_both_ids() {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    String hostId = UUID.randomUUID().toString();
    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList(connectionId), Collections.singletonList(hostId));
    eventBus.fire(event);

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit + 1);
    // Both partition key components provided → no ALLOW FILTERING
    assertThat(handler.lastCapturedQuery())
        .contains("WHERE connection_id IN (")
        .contains("AND host_id IN (")
        .doesNotContain("ALLOW FILTERING");
  }

  @Test
  public void should_not_requery_routes_on_refresh_after_close() throws Exception {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    handler.close();

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    // close() sets the closed flag — refresh is a no-op
    assertThat(handler.capturedQueries).hasSize(queriesAfterInit);
  }

  @Test
  public void should_not_requery_routes_on_change_event_after_close() {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    handler.close();

    String hostId = UUID.randomUUID().toString();
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList(connectionId), Collections.singletonList(hostId)));

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit);
  }

  @Test
  public void should_requery_routes_on_multiple_refreshes() {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    handler.refresh();
    handler.refresh();
    handler.refresh();

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit + 3);
  }

  // ---- Query building edge cases ------------------------------------------

  @Test
  public void should_issue_connection_ids_only_query_on_change_event_with_no_host_ids() {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList(connectionId), Collections.emptyList());
    eventBus.fire(event);

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit + 1);
    assertThat(handler.lastCapturedQuery())
        .contains("WHERE connection_id IN (")
        .contains("ALLOW FILTERING")
        .doesNotContain("host_id IN");
  }

  @Test
  public void should_fall_back_to_configured_connection_ids_on_empty_change_event() {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent("UPDATED", Collections.emptyList(), Collections.emptyList());
    eventBus.fire(event);

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit + 1);
    assertThat(handler.lastCapturedQuery())
        .contains(connectionId)
        .contains("ALLOW FILTERING")
        .doesNotContain("host_id IN");
  }

  // ---- CQL injection prevention tests ------------------------------------

  @Test
  public void should_escape_single_quotes_in_connection_ids() {
    String maliciousId = "id') OR 1=1 --";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(maliciousId, "host1"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));
    when(controlConnection.init(anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    h.init();

    // Single quotes in the connection ID must be escaped (doubled)
    assertThat(h.lastCapturedQuery()).contains("'id'') OR 1=1 --'");
  }

  @Test
  public void should_reject_invalid_host_id_format() {
    initHandler();

    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList("not-a-uuid; DROP TABLE foo"));

    assertThatThrownBy(() -> eventBus.fire(event))
        .hasCauseInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid host ID");
  }

  @Test
  public void should_accept_valid_uuid_host_ids() {
    initHandler();
    int queriesAfterInit = handler.capturedQueries.size();

    String hostId = UUID.randomUUID().toString();
    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList(connectionId), Collections.singletonList(hostId));
    eventBus.fire(event);

    assertThat(handler.capturedQueries).hasSize(queriesAfterInit + 1);
    assertThat(handler.lastCapturedQuery()).contains("host_id IN (" + hostId + ")");
  }

  // ---- Refresh queue tests --------------------------------------------------

  /**
   * Creates a handler whose first post-init query blocks on a delayed future. Subsequent queries
   * (from queue drains) complete immediately with the provided empty result.
   */
  private TestableClientRoutesTopologyMonitor createDelayedHandler(
      CompletableFuture<AdminResult> delayedFuture, AdminResult emptyResult) {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, "host1"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config) {
          volatile boolean initDone = false;
          volatile boolean firstPostInitDone = false;

          @Override
          @NonNull
          protected CompletionStage<AdminResult> runAdminQuery(
              @NonNull DriverChannel channel,
              @NonNull String queryString,
              @NonNull Duration timeout) {
            capturedQueries.add(queryString);
            if (!initDone) {
              initDone = true;
              return CompletableFuture.completedFuture(emptyResult);
            }
            if (!firstPostInitDone) {
              firstPostInitDone = true;
              return delayedFuture;
            }
            return CompletableFuture.completedFuture(emptyResult);
          }
        };

    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));
    when(controlConnection.init(anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    h.init();
    return h;
  }

  @Test
  public void should_queue_and_drain_concurrent_refresh_requests() throws Exception {
    CompletableFuture<AdminResult> delayedFuture = new CompletableFuture<>();
    AdminResult emptyResult = AdminResultTestHelper.mockResult();
    TestableClientRoutesTopologyMonitor h = createDelayedHandler(delayedFuture, emptyResult);
    int queriesAfterInit = h.capturedQueries.size();

    // Fire two refresh requests while the first is still in-flight
    h.refresh();
    h.refresh();

    // Only one query issued so far; the second is queued
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);

    // Complete the in-flight query — the queued request should drain and fire a second query
    delayedFuture.complete(emptyResult);
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 2);
  }

  @Test
  public void should_coalesce_two_full_refreshes_into_one() throws Exception {
    CompletableFuture<AdminResult> delayedFuture = new CompletableFuture<>();
    AdminResult emptyResult = AdminResultTestHelper.mockResult();
    TestableClientRoutesTopologyMonitor h = createDelayedHandler(delayedFuture, emptyResult);
    int queriesAfterInit = h.capturedQueries.size();

    // Three full refreshes while the first is in-flight
    h.refresh();
    h.refresh();
    h.refresh();

    // Only one query in-flight; the other two coalesce into a single queued request
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);

    delayedFuture.complete(emptyResult);

    // Drain fires exactly one more query (not two)
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 2);
  }

  @Test
  public void should_coalesce_targeted_refreshes_and_merge_host_ids() throws Exception {
    CompletableFuture<AdminResult> delayedFuture = new CompletableFuture<>();
    AdminResult emptyResult = AdminResultTestHelper.mockResult();
    TestableClientRoutesTopologyMonitor h = createDelayedHandler(delayedFuture, emptyResult);
    int queriesAfterInit = h.capturedQueries.size();

    // Start a full refresh (blocks on delayedFuture)
    h.refresh();
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);

    // Queue two targeted events with different host IDs
    String hostIdA = UUID.randomUUID().toString();
    String hostIdB = UUID.randomUUID().toString();
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList(hostIdA)));
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList(hostIdB)));

    // Still one query in-flight; two events coalesced into one queued request
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);

    delayedFuture.complete(emptyResult);

    // Drain fires one query containing both host IDs
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 2);
    String drainedQuery = h.lastCapturedQuery();
    assertThat(drainedQuery).contains("host_id IN (");
    assertThat(drainedQuery).contains(hostIdA);
    assertThat(drainedQuery).contains(hostIdB);
  }

  @Test
  public void should_upgrade_queued_targeted_to_full_when_full_arrives() throws Exception {
    CompletableFuture<AdminResult> delayedFuture = new CompletableFuture<>();
    AdminResult emptyResult = AdminResultTestHelper.mockResult();
    TestableClientRoutesTopologyMonitor h = createDelayedHandler(delayedFuture, emptyResult);
    int queriesAfterInit = h.capturedQueries.size();

    // Start a targeted refresh (blocks on delayedFuture)
    String hostId = UUID.randomUUID().toString();
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList(connectionId), Collections.singletonList(hostId)));
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);

    // Queue a full refresh — should upgrade the queued request
    h.refresh();

    delayedFuture.complete(emptyResult);

    // The drained query should be a full refresh (no host_id filter, has ALLOW FILTERING)
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 2);
    String drainedQuery = h.lastCapturedQuery();
    assertThat(drainedQuery).doesNotContain("host_id IN");
    assertThat(drainedQuery).contains("ALLOW FILTERING");
  }

  @Test
  public void should_not_drain_queued_refresh_after_close() throws Exception {
    CompletableFuture<AdminResult> delayedFuture = new CompletableFuture<>();
    AdminResult emptyResult = AdminResultTestHelper.mockResult();
    TestableClientRoutesTopologyMonitor h = createDelayedHandler(delayedFuture, emptyResult);
    int queriesAfterInit = h.capturedQueries.size();

    // Start a refresh (blocks on delayedFuture), then queue another
    h.refresh();
    h.refresh();
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);

    // Close before the in-flight refresh completes
    h.close();
    delayedFuture.complete(emptyResult);

    // Queued request should NOT drain — only the original in-flight query ran
    assertThat(h.capturedQueries).hasSize(queriesAfterInit + 1);
  }

  // ---- Concurrent mergeRoutes CAS retry test --------------------------------

  @Test
  public void should_handle_concurrent_merge_routes() throws Exception {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();
    UUID hostId3 = UUID.randomUUID();

    handler.setRoutes(ImmutableMap.of(hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042)));

    // CyclicBarrier forces both threads to reach the merge call at roughly the same time,
    // maximising the chance of actual CAS contention on the routes cache.
    CyclicBarrier barrier = new CyclicBarrier(2);

    // Run two concurrent merges
    Thread t1 =
        new Thread(
            () -> {
              try {
                barrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              Map<UUID, ClientRouteRecord> incoming = new HashMap<>();
              incoming.put(hostId2, new ClientRouteRecord(hostId2, "127.0.0.2", 9042));
              handler.mergeRoutesForTest(incoming);
            });
    Thread t2 =
        new Thread(
            () -> {
              try {
                barrier.await();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              Map<UUID, ClientRouteRecord> incoming = new HashMap<>();
              incoming.put(hostId3, new ClientRouteRecord(hostId3, "127.0.0.3", 9042));
              handler.mergeRoutesForTest(incoming);
            });

    t1.start();
    t2.start();
    t1.join(5000);
    t2.join(5000);

    // All three hosts should be present regardless of CAS retry ordering
    assertThat(handler.getRoutes()).containsKeys(hostId1, hostId2, hostId3);
  }

  // ---- Null control connection channel ------------------------------------

  @Test
  public void should_not_throw_when_control_connection_channel_is_null() throws Exception {
    // controlConnection.channel() is not stubbed in setup(), so it returns null by default
    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    // No exception; routes cache unchanged (still empty)
    assertThat(handler.getRoutes()).isEmpty();
  }

  // ---- connectionAddr override tests ---------------------------------------

  @Test
  public void should_apply_connection_addr_override_when_connection_id_matches() throws Exception {
    String connId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    // The address column is populated deliberately -- this test means "the override beats a
    // populated column", not "the override fills in for a missing one". effectiveAddress
    // short-circuits the column once an override matches, so these stubs go unread; they are
    // lenient rather than deleted, because deleting them would weaken the test to the latter
    // claim. should_apply_override_when_table_address_cell_is_malformed pins the short-circuit
    // itself.
    Mockito.lenient().when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    Mockito.lenient().when(row.getString("address")).thenReturn("original.example.com");
    when(row.getInteger("port")).thenReturn(9042);
    when(row.contains("connection_id")).thenReturn(true);
    when(row.isNull("connection_id")).thenReturn(false);
    when(row.getString("connection_id")).thenReturn(connId);

    h.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).containsKey(hostId);
    assertThat(h.getRoutes().get(hostId).getHostname()).isEqualTo("override.example.com");
  }

  @Test
  public void should_not_apply_override_when_connection_id_does_not_match() throws Exception {
    String configConnId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(configConnId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("original.example.com");
    when(row.getInteger("port")).thenReturn(9042);
    when(row.contains("connection_id")).thenReturn(true);
    when(row.isNull("connection_id")).thenReturn(false);
    when(row.getString("connection_id")).thenReturn("conn-2");

    h.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).containsKey(hostId);
    assertThat(h.getRoutes().get(hostId).getHostname()).isEqualTo("original.example.com");
  }

  @Test
  public void should_not_apply_override_when_connection_id_absent() throws Exception {
    String configConnId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(configConnId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("original.example.com");
    when(row.getInteger("port")).thenReturn(9042);
    when(row.contains("connection_id")).thenReturn(false);

    h.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).containsKey(hostId);
    assertThat(h.getRoutes().get(hostId).getHostname()).isEqualTo("original.example.com");
  }

  @Test
  public void should_selectively_apply_override_to_matching_routes_only() throws Exception {
    String connId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId1 = UUID.randomUUID();
    AdminRow matchingRow = Mockito.mock(AdminRow.class);
    when(matchingRow.isNull("host_id")).thenReturn(false);
    // Address stubs lenient for the reason given in
    // should_apply_connection_addr_override_when_connection_id_matches.
    Mockito.lenient().when(matchingRow.isNull("address")).thenReturn(false);
    when(matchingRow.isNull("port")).thenReturn(false);
    when(matchingRow.getUuid("host_id")).thenReturn(hostId1);
    Mockito.lenient().when(matchingRow.getString("address")).thenReturn("original-1.example.com");
    when(matchingRow.getInteger("port")).thenReturn(9042);
    when(matchingRow.contains("connection_id")).thenReturn(true);
    when(matchingRow.isNull("connection_id")).thenReturn(false);
    when(matchingRow.getString("connection_id")).thenReturn(connId);

    UUID hostId2 = UUID.randomUUID();
    AdminRow nonMatchingRow = Mockito.mock(AdminRow.class);
    when(nonMatchingRow.isNull("host_id")).thenReturn(false);
    when(nonMatchingRow.isNull("address")).thenReturn(false);
    when(nonMatchingRow.isNull("port")).thenReturn(false);
    when(nonMatchingRow.getUuid("host_id")).thenReturn(hostId2);
    when(nonMatchingRow.getString("address")).thenReturn("original-2.example.com");
    when(nonMatchingRow.getInteger("port")).thenReturn(9042);
    when(nonMatchingRow.contains("connection_id")).thenReturn(true);
    when(nonMatchingRow.isNull("connection_id")).thenReturn(false);
    when(nonMatchingRow.getString("connection_id")).thenReturn("conn-other");

    h.setNextQueryResult(AdminResultTestHelper.mockResult(matchingRow, nonMatchingRow));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes().get(hostId1).getHostname()).isEqualTo("override.example.com");
    assertThat(h.getRoutes().get(hostId2).getHostname()).isEqualTo("original-2.example.com");
  }

  // ---- Row parsing tests --------------------------------------------------

  @Test
  public void should_skip_rows_with_null_required_fields_and_still_process_valid_rows()
      throws Exception {
    UUID validHostId = UUID.randomUUID();

    AdminRow nullRow = Mockito.mock(AdminRow.class);
    when(nullRow.isNull("host_id")).thenReturn(true);

    AdminRow validRow = Mockito.mock(AdminRow.class);
    when(validRow.isNull("host_id")).thenReturn(false);
    when(validRow.isNull("address")).thenReturn(false);
    when(validRow.isNull("port")).thenReturn(false);
    when(validRow.getUuid("host_id")).thenReturn(validHostId);
    when(validRow.getString("address")).thenReturn("127.0.0.1");
    when(validRow.getInteger("port")).thenReturn(9042);
    when(validRow.contains("connection_id")).thenReturn(false);

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(nullRow, validRow));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(validHostId);
    assertThat(handler.getRoutes().get(validHostId).getHostname()).isEqualTo("127.0.0.1");
  }

  @Test
  public void should_use_regular_port_when_ssl_disabled() throws Exception {
    // Default handler has SSL disabled — should pick the regular port column
    UUID hostId = UUID.randomUUID();

    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("127.0.0.1");
    when(row.getInteger("port")).thenReturn(9042);
    when(row.contains("connection_id")).thenReturn(false);

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsKey(hostId);
    assertThat(handler.getRoutes().get(hostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_use_tls_port_when_ssl_enabled() throws Exception {
    // Recreate handler with SSL enabled
    when(context.getSslEngineFactory())
        .thenReturn(
            Optional.of(Mockito.mock(com.datastax.oss.driver.api.core.ssl.SslEngineFactory.class)));
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(UUID.randomUUID().toString(), "host1"))
            .build();
    TestableClientRoutesTopologyMonitor sslHandler =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();

    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("127.0.0.1");
    when(row.isNull("tls_port")).thenReturn(false);
    when(row.getInteger("tls_port")).thenReturn(9142);
    when(row.contains("connection_id")).thenReturn(false);

    sslHandler.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    sslHandler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(sslHandler.getRoutes()).containsKey(hostId);
    assertThat(sslHandler.getRoutes().get(hostId).getPort()).isEqualTo(9142);
  }

  @Test
  public void should_skip_route_when_ssl_enabled_but_tls_port_absent() throws Exception {
    // Recreate handler with SSL enabled
    when(context.getSslEngineFactory())
        .thenReturn(
            Optional.of(Mockito.mock(com.datastax.oss.driver.api.core.ssl.SslEngineFactory.class)));
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(UUID.randomUUID().toString(), "host1"))
            .build();
    TestableClientRoutesTopologyMonitor sslHandler =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();

    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("127.0.0.1");
    // tls_port is null (default Mockito behavior for isNull) → route should be skipped

    sslHandler.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    sslHandler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    // tls_port absent with SSL enabled → route must be skipped
    assertThat(sslHandler.getRoutes()).doesNotContainKey(hostId);
  }

  @Test
  public void should_skip_route_when_port_is_null() throws Exception {
    UUID hostId = UUID.randomUUID();

    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(true);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("127.0.0.1");

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    // port is null → route must be skipped
    assertThat(handler.getRoutes()).doesNotContainKey(hostId);
  }

  @Test
  public void should_skip_zero_port_row_without_losing_the_refresh() throws Exception {
    // Port 0 passes the null check, but ClientRouteRecord's constructor rejects it. Unguarded,
    // that IllegalArgumentException escapes the row loop and the whole refresh is discarded, so
    // the assertion that matters is that the good row survives.
    UUID badHostId = UUID.randomUUID();
    UUID goodHostId = UUID.randomUUID();

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(badHostId, "127.0.0.1", 0), mockRouteRow(goodHostId, "127.0.0.2", 9042)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(goodHostId);
    assertThat(handler.getRoutes().get(goodHostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_skip_empty_address_row_without_losing_the_refresh() throws Exception {
    // The sibling of the zero-port case: isNull("address") is false for an empty string, and
    // ClientRouteRecord's constructor rejects it from inside the row loop. No override is
    // configured for this row, so the empty address is also the effective one.
    UUID badHostId = UUID.randomUUID();
    UUID goodHostId = UUID.randomUUID();

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(badHostId, "", 9042), mockRouteRow(goodHostId, "127.0.0.2", 9042)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(goodHostId);
    assertThat(handler.getRoutes().get(goodHostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_cache_route_when_override_replaces_empty_address() throws Exception {
    // An empty address column is only unusable if nothing replaces it. ClientRouteProxy documents
    // the configured address as overriding the table's, and forbids a blank override, so the
    // effective address here is always valid -- validating before the override would drop a row
    // that is perfectly routable.
    String connId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    h.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(hostId, "", "port", 9042, connId)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).containsOnlyKeys(hostId);
    assertThat(h.getRoutes().get(hostId).getHostname()).isEqualTo("override.example.com");
  }

  @Test
  public void should_skip_row_when_host_id_cell_is_malformed() throws Exception {
    // A non-null host_id blob of the wrong length makes UuidCodec.decode throw before any column
    // guard can inspect the value. Nothing about that exception is specific to the columns the
    // loop validates, so only a general per-row catch keeps the rest of the batch.
    UUID goodHostId = UUID.randomUUID();

    AdminRow badRow = mockRouteRow(UUID.randomUUID(), "127.0.0.1", 9042);
    Mockito.doThrow(
            new IllegalArgumentException(
                "Unexpected number of bytes for a UUID, expected 16, got 4"))
        .when(badRow)
        .getUuid("host_id");

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(badRow, mockRouteRow(goodHostId, "127.0.0.2", 9042)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(goodHostId);
  }

  @Test
  public void should_skip_row_when_tls_port_is_out_of_range() throws Exception {
    when(context.getSslEngineFactory())
        .thenReturn(
            Optional.of(Mockito.mock(com.datastax.oss.driver.api.core.ssl.SslEngineFactory.class)));
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(UUID.randomUUID().toString(), "host1"))
            .build();
    TestableClientRoutesTopologyMonitor sslHandler =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID badHostId = UUID.randomUUID();
    UUID goodHostId = UUID.randomUUID();

    sslHandler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(badHostId, "127.0.0.1", "tls_port", 0, null),
            mockRouteRow(goodHostId, "127.0.0.2", "tls_port", 9142, null)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    sslHandler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(sslHandler.getRoutes()).containsOnlyKeys(goodHostId);
    assertThat(sslHandler.getRoutes().get(goodHostId).getPort()).isEqualTo(9142);
  }

  @Test
  public void should_keep_cached_route_when_targeted_refresh_returns_unusable_row()
      throws Exception {
    // A targeted refresh removes the host IDs the event named but the query did not return,
    // reading their absence as a server-side delete. A row that came back and was skipped is not
    // a delete: evicting it would drop a working route back to the node's private address, which
    // is unreachable in the very deployment client routes exist for.
    UUID hostId = UUID.randomUUID();
    initHandler();
    handler.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "127.0.0.1", 9042)));

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(hostId, "127.0.0.2", 0)));
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList(hostId.toString())));

    assertThat(handler.getRoutes()).containsOnlyKeys(hostId);
    assertThat(handler.getRoutes().get(hostId).getHostname()).isEqualTo("127.0.0.1");
    assertThat(handler.getRoutes().get(hostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_keep_route_named_by_an_unusable_row_but_evict_the_absent_one()
      throws Exception {
    // No usable row is not the same as no row: the consecutive-empty guard clears the cache after
    // MAX_CONSECUTIVE_EMPTY_RESULTS (3) empty results, on the theory that the routes were removed
    // server-side, and a table of malformed rows must not reach it. Keeping the *whole* cache is
    // not the answer either -- a host the result never named is genuinely gone, whether or not the
    // rows it did name were usable.
    UUID namedHostId = UUID.randomUUID();
    UUID absentHostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(
            namedHostId,
            new ClientRouteRecord(namedHostId, "127.0.0.1", 9042),
            absentHostId,
            new ClientRouteRecord(absentHostId, "127.0.0.2", 9042)));

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(namedHostId, "127.0.0.3", 0)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    for (int i = 0; i < 4; i++) {
      handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    assertThat(handler.getRoutes()).containsOnlyKeys(namedHostId);
    assertThat(handler.getRoutes().get(namedHostId).getHostname()).isEqualTo("127.0.0.1");
  }

  @Test
  public void should_keep_all_cached_routes_when_no_row_names_a_readable_host_id()
      throws Exception {
    // The extreme of the provable-absence rule: not a single row named a host_id the driver could
    // read, so the pass learned nothing about which hosts still exist. It must evict nothing, and
    // must not count towards the consecutive-empty guard either -- that would clear the cache on
    // the third such pass and strand every node on its private address.
    UUID cachedHostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(cachedHostId, new ClientRouteRecord(cachedHostId, "127.0.0.1", 9042)));

    AdminRow malformedRow = Mockito.mock(AdminRow.class);
    Mockito.lenient().when(malformedRow.isNull("host_id")).thenReturn(false);
    Mockito.doThrow(
            new IllegalArgumentException(
                "Unexpected number of bytes for a UUID, expected 16, got 4"))
        .when(malformedRow)
        .getUuid("host_id");

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(malformedRow, mockRouteRow(null, "127.0.0.2", 9042)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    for (int i = 0; i < 4; i++) {
      handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    assertThat(handler.getRoutes()).containsOnlyKeys(cachedHostId);
    assertThat(handler.getRoutes().get(cachedHostId).getHostname()).isEqualTo("127.0.0.1");
  }

  @Test
  public void should_keep_cached_route_when_targeted_refresh_row_has_no_address() throws Exception {
    // The sibling of the case above, for the one skip that used to happen before the row's
    // host_id had been read: an absent address column. The host still has to count as present in
    // the result, or the removal sweep reads it as a server-side delete.
    UUID hostId = UUID.randomUUID();
    initHandler();
    handler.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "127.0.0.1", 9042)));

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(mockRouteRow(hostId, null, 9042)));
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList(hostId.toString())));

    assertThat(handler.getRoutes()).containsOnlyKeys(hostId);
    assertThat(handler.getRoutes().get(hostId).getHostname()).isEqualTo("127.0.0.1");
    assertThat(handler.getRoutes().get(hostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_keep_cached_route_when_targeted_refresh_row_has_unreadable_host_id()
      throws Exception {
    // A row whose identity the driver cannot read is still a row: something came back for one of
    // the host IDs this event named, and there is no telling which. So no event ID can be shown
    // to be absent, and the removal sweep must evict nothing -- otherwise a single malformed
    // host_id cell drops a working route back to the node's unreachable private address.
    UUID hostId = UUID.randomUUID();
    initHandler();
    handler.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "127.0.0.1", 9042)));

    AdminRow unreadableRow = Mockito.mock(AdminRow.class);
    Mockito.lenient().when(unreadableRow.isNull("host_id")).thenReturn(false);
    Mockito.doThrow(
            new IllegalArgumentException(
                "Unexpected number of bytes for a UUID, expected 16, got 4"))
        .when(unreadableRow)
        .getUuid("host_id");

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(unreadableRow));
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList(hostId.toString())));

    assertThat(handler.getRoutes()).containsOnlyKeys(hostId);
    assertThat(handler.getRoutes().get(hostId).getHostname()).isEqualTo("127.0.0.1");
    assertThat(handler.getRoutes().get(hostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_keep_rebuilt_route_when_a_row_had_an_unreadable_host_id_and_cache_was_empty()
      throws Exception {
    // Evicting nothing is only half the rule: the sweep reads the keep-set as the complete list
    // of host IDs it may not remove, so the routes this pass just rebuilt have to be in it too.
    // With an unattributable row in the pass the set is the cached keys, and an empty cache made
    // that empty -- so hostId was merged by mergeRoutes and removed again by the sweep below it,
    // in one pass. The pass that discovers a host is exactly the pass with no cache entry for it.
    UUID hostId = UUID.randomUUID();
    initHandler();
    assertThat(handler.getRoutes()).isEmpty();

    AdminRow unreadableRow = Mockito.mock(AdminRow.class);
    Mockito.lenient().when(unreadableRow.isNull("host_id")).thenReturn(true);

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(hostId, "127.0.0.9", 9043), unreadableRow));
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            Collections.singletonList(hostId.toString())));

    assertThat(handler.getRoutes()).containsOnlyKeys(hostId);
    assertThat(handler.getRoutes().get(hostId).getHostname()).isEqualTo("127.0.0.9");
    assertThat(handler.getRoutes().get(hostId).getPort()).isEqualTo(9043);
  }

  @Test
  public void should_keep_both_a_rebuilt_and_a_carried_over_route_when_a_row_was_unreadable()
      throws Exception {
    // The same defect without an empty cache: what decides it is whether the rebuilt host is
    // already cached, not whether anything is. The cache holds only carriedHostId, so the keep-set
    // was {carriedHostId} and the sweep removed the freshly rebuilt rebuiltHostId. Both belong:
    // one because the pass rebuilt it, one because the unattributable row leaves its absence
    // unproven.
    UUID rebuiltHostId = UUID.randomUUID();
    UUID carriedHostId = UUID.randomUUID();
    initHandler();
    handler.setRoutes(
        ImmutableMap.of(carriedHostId, new ClientRouteRecord(carriedHostId, "127.0.0.1", 9042)));

    AdminRow unreadableRow = Mockito.mock(AdminRow.class);
    Mockito.lenient().when(unreadableRow.isNull("host_id")).thenReturn(true);

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(rebuiltHostId, "127.0.0.9", 9043), unreadableRow));
    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            java.util.Arrays.asList(rebuiltHostId.toString(), carriedHostId.toString())));

    assertThat(handler.getRoutes()).containsOnlyKeys(rebuiltHostId, carriedHostId);
    assertThat(handler.getRoutes().get(rebuiltHostId).getHostname()).isEqualTo("127.0.0.9");
    assertThat(handler.getRoutes().get(rebuiltHostId).getPort()).isEqualTo(9043);
    assertThat(handler.getRoutes().get(carriedHostId).getHostname()).isEqualTo("127.0.0.1");
  }

  @Test
  public void should_keep_cached_route_absent_from_a_refresh_that_saw_an_unreadable_row()
      throws Exception {
    // The same rule on the full-refresh writer, in the mixed case: some rows were readable, so
    // the presence set is non-empty -- but it is incomplete, because the unreadable row could
    // have been absentHostId's. Absence from an incomplete set is not a delete, so B survives.
    // should_keep_route_named_by_an_unusable_row_but_evict_the_absent_one is the counterweight:
    // once every row names a readable host_id, the absent one does get evicted.
    UUID goodHostId = UUID.randomUUID();
    UUID absentHostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(
            goodHostId,
            new ClientRouteRecord(goodHostId, "127.0.0.1", 9042),
            absentHostId,
            new ClientRouteRecord(absentHostId, "127.0.0.2", 9042)));

    AdminRow unreadableRow = Mockito.mock(AdminRow.class);
    Mockito.lenient().when(unreadableRow.isNull("host_id")).thenReturn(true);

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(goodHostId, "127.0.0.9", 9043), unreadableRow));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(goodHostId, absentHostId);
    assertThat(handler.getRoutes().get(goodHostId).getHostname()).isEqualTo("127.0.0.9");
    assertThat(handler.getRoutes().get(goodHostId).getPort()).isEqualTo(9043);
    assertThat(handler.getRoutes().get(absentHostId).getHostname()).isEqualTo("127.0.0.2");
  }

  @Test
  public void should_apply_override_when_table_address_cell_is_malformed() throws Exception {
    // ClientRouteProxy documents connection_addr as replacing the table's address column outright
    // for a matching connection_id, and does not qualify that on the column being decodable. The
    // text codec rejects malformed UTF-8 with an IllegalArgumentException, so reading the column
    // before resolving the override would let a cell the driver is about to discard cost the
    // route. Sibling of should_cache_route_when_override_replaces_absent_address.
    String connId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.isNull("port")).thenReturn(false);
    when(row.getInteger("port")).thenReturn(9042);
    when(row.contains("connection_id")).thenReturn(true);
    when(row.isNull("connection_id")).thenReturn(false);
    when(row.getString("connection_id")).thenReturn(connId);
    // Both stubs are lenient because a passing run never reaches them -- that is the assertion.
    // The doThrow is the trap: if effectiveAddress read the column before resolving the override,
    // it would fire, the row would be skipped, and the route below would be missing.
    Mockito.lenient().when(row.isNull("address")).thenReturn(false);
    Mockito.lenient()
        .doThrow(new IllegalArgumentException(new MalformedInputException(1)))
        .when(row)
        .getString("address");

    h.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).containsOnlyKeys(hostId);
    assertThat(h.getRoutes().get(hostId).getHostname()).isEqualTo("override.example.com");
  }

  @Test
  public void should_keep_cached_route_for_unusable_row_on_full_refresh() throws Exception {
    // A full refresh replaces the cache outright because a host missing from the result was
    // deleted server-side. A host whose row came back unusable is not missing, and one good row
    // in the same pass must not carry its eviction:
    // should_replace_non_empty_cache_with_non_empty_query_result pins the other half of the rule.
    // This holds because one connection ID is configured, so host_id is the whole route identity;
    // the multi-endpoint tests at the end of this class pin what happens when it is not.
    UUID unusableHostId = UUID.randomUUID();
    UUID goodHostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(unusableHostId, new ClientRouteRecord(unusableHostId, "127.0.0.1", 9042)));

    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(unusableHostId, "127.0.0.2", 0),
            mockRouteRow(goodHostId, "127.0.0.3", 9043)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(unusableHostId, goodHostId);
    assertThat(handler.getRoutes().get(unusableHostId).getHostname()).isEqualTo("127.0.0.1");
    assertThat(handler.getRoutes().get(unusableHostId).getPort()).isEqualTo(9042);
    assertThat(handler.getRoutes().get(goodHostId).getPort()).isEqualTo(9043);
  }

  @Test
  public void should_prefer_usable_row_over_cached_route_for_same_host() throws Exception {
    // Carrying a cached record over for an unusable row must never shadow a usable row for the
    // same host: system.client_routes is keyed (connection_id, host_id), so with several proxies
    // one host_id can appear more than once in a single result.
    UUID hostId = UUID.randomUUID();
    UUID otherHostId = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(
            hostId,
            new ClientRouteRecord(hostId, "127.0.0.1", 9042),
            otherHostId,
            new ClientRouteRecord(otherHostId, "127.0.0.2", 9042)));

    // Two rows for hostId -- one usable, one not -- plus an unusable row for otherHostId, so the
    // carry-over runs and has to leave the freshly-read record for hostId alone.
    handler.setNextQueryResult(
        AdminResultTestHelper.mockResult(
            mockRouteRow(hostId, "127.0.0.9", 9042),
            mockRouteRow(hostId, "", 9042),
            mockRouteRow(otherHostId, "127.0.0.8", 0)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).containsOnlyKeys(hostId, otherHostId);
    assertThat(handler.getRoutes().get(hostId).getHostname()).isEqualTo("127.0.0.9");
    assertThat(handler.getRoutes().get(otherHostId).getHostname()).isEqualTo("127.0.0.2");
  }

  @Test
  public void should_cache_route_when_override_replaces_absent_address() throws Exception {
    // The sibling of should_cache_route_when_override_replaces_empty_address. ClientRouteProxy
    // documents the configured address as replacing the table's column for a matching
    // connection_id, without qualification, so an absent column and an empty one have to reach
    // the same route -- the override is what the driver connects to either way.
    String connId = "conn-1";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId, "override.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    h.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(hostId, null, "port", 9042, connId)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).containsOnlyKeys(hostId);
    assertThat(h.getRoutes().get(hostId).getHostname()).isEqualTo("override.example.com");
    assertThat(h.getRoutes().get(hostId).getPort()).isEqualTo(9042);
  }

  // ---- Empty-result cache guard tests ------------------------------------

  @Test
  public void should_not_replace_non_empty_cache_with_empty_query_result() throws Exception {
    UUID hostId = UUID.randomUUID();
    handler.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "127.0.0.1", 9042)));

    // Simulate a full refresh that returns 0 rows (e.g. routes not visible on the queried node)
    handler.setNextQueryResult(AdminResultTestHelper.mockResult());
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    // Cache must be preserved — the empty result should not wipe valid routes
    assertThat(handler.getRoutes()).containsOnlyKeys(hostId);
    assertThat(handler.getRoutes().get(hostId).getPort()).isEqualTo(9042);
  }

  @Test
  public void should_replace_empty_cache_with_empty_query_result() throws Exception {
    assertThat(handler.getRoutes()).isEmpty();

    // Full refresh with 0 rows when cache is also empty — no-op
    handler.setNextQueryResult(AdminResultTestHelper.mockResult());
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(handler.getRoutes()).isEmpty();
  }

  @Test
  public void should_replace_non_empty_cache_with_non_empty_query_result() throws Exception {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();
    handler.setRoutes(ImmutableMap.of(hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042)));

    // Full refresh returns a different route set
    AdminRow newRow = Mockito.mock(AdminRow.class);
    when(newRow.isNull("host_id")).thenReturn(false);
    when(newRow.isNull("address")).thenReturn(false);
    when(newRow.isNull("port")).thenReturn(false);
    when(newRow.getUuid("host_id")).thenReturn(hostId2);
    when(newRow.getString("address")).thenReturn("127.0.0.2");
    when(newRow.getInteger("port")).thenReturn(9043);
    when(newRow.contains("connection_id")).thenReturn(false);

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(newRow));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    handler.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    // Cache should be fully replaced with the new results
    assertThat(handler.getRoutes()).containsOnlyKeys(hostId2);
    assertThat(handler.getRoutes().get(hostId2).getPort()).isEqualTo(9043);
  }

  // ---- error handling in queryAndResolveRoutes() --------------------------

  @Test
  public void should_not_propagate_exception_when_query_fails() throws Exception {
    handler.failNextQuery = true;
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    // The stage must complete normally (no exception) even though the query failed.
    CompletionStage<Void> stage = handler.refresh();
    stage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    // The routes cache must remain untouched (still empty).
    assertThat(handler.getRoutes()).isEmpty();
  }

  // ---- buildNodeEndPoint fallback -----------------------------------------

  @Test
  public void should_build_default_endpoint_when_host_id_is_null() {
    // row.getUuid("host_id") returns null, triggering the hostId == null
    // branch in buildNodeEndPoint which delegates to super.buildNodeEndPoint().
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.getUuid("host_id")).thenReturn(null);
    when(row.contains("peer")).thenReturn(false); // local-node row → super returns localEndPoint
    EndPoint localEndPoint = Mockito.mock(EndPoint.class);

    EndPoint result = handler.buildNodeEndPoint(row, null, localEndPoint);

    // hostId == null branch → super.buildNodeEndPoint() is called → returns localEndPoint
    assertThat(result).isNotInstanceOf(ClientRoutesEndPoint.class);
    assertThat(result).isSameAs(localEndPoint);
  }

  @Test
  public void should_build_client_routes_endpoint_when_host_id_non_null() {
    // Even with empty routes cache, a ClientRoutesEndPoint is created so it can
    // resolve to PrivateLink address once the cache is populated.
    assertThat(handler.getRoutes()).isEmpty();

    UUID hostId = UUID.randomUUID();
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.contains("peer")).thenReturn(false);
    EndPoint localEndPoint = Mockito.mock(EndPoint.class);

    EndPoint result = handler.buildNodeEndPoint(row, null, localEndPoint);

    assertThat(result).isInstanceOf(ClientRoutesEndPoint.class);
    assertThat(((ClientRoutesEndPoint) result).getHostId()).isEqualTo(hostId);
  }

  // ---- Route removal tests --------------------------------------------------

  @Test
  public void should_remove_route_from_cache() throws UnknownHostException {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();

    handler.setRoutes(
        ImmutableMap.of(
            hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042),
            hostId2, new ClientRouteRecord(hostId2, "127.0.0.2", 9042)));

    assertThat(handler.resolve(hostId1)).isNotNull();
    assertThat(handler.resolve(hostId2)).isNotNull();

    handler.removeRouteForTest(hostId1);

    assertThat(handler.resolve(hostId1)).isNull();
    assertThat(handler.resolve(hostId2)).isNotNull();
  }

  @Test
  public void should_remove_stale_route_on_targeted_refresh() throws Exception {
    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();

    // Pre-populate cache with two routes
    handler.setRoutes(
        ImmutableMap.of(
            hostId1, new ClientRouteRecord(hostId1, "127.0.0.1", 9042),
            hostId2, new ClientRouteRecord(hostId2, "127.0.0.2", 9042)));
    assertThat(handler.getRoutes()).containsKeys(hostId1, hostId2);

    // Simulate a targeted refresh (CLIENT_ROUTES_CHANGE event) that mentions both host IDs,
    // but the query result only returns hostId1 (hostId2 was decommissioned server-side).
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.isNull("host_id")).thenReturn(false);
    when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId1);
    when(row.getString("address")).thenReturn("127.0.0.1");
    when(row.getInteger("port")).thenReturn(9042);
    when(row.contains("connection_id")).thenReturn(false);

    handler.setNextQueryResult(AdminResultTestHelper.mockResult(row));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));
    when(controlConnection.init(anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    handler.init();

    // Fire a targeted event mentioning both host IDs
    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connectionId),
            java.util.Arrays.asList(hostId1.toString(), hostId2.toString()));
    eventBus.fire(event);

    // hostId1 should still be present, hostId2 should have been removed
    assertThat(handler.getRoutes()).containsKey(hostId1);
    assertThat(handler.getRoutes()).doesNotContainKey(hostId2);
  }

  @Test
  public void should_resolve_to_fallback_when_no_route_for_host_id() {
    // Simulates a node that is not accessed via PrivateLink (no route in cache for its host_id).
    // resolve() must return the regular endpoint address (the fallback), not throw.
    UUID hostId = UUID.randomUUID();
    InetSocketAddress fallbackAddress = new InetSocketAddress("127.0.0.99", 9999);
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.contains("peer")).thenReturn(false);
    EndPoint localEndPoint = Mockito.mock(EndPoint.class);
    when(localEndPoint.resolve()).thenReturn(fallbackAddress);

    EndPoint endpoint = handler.buildNodeEndPoint(row, null, localEndPoint);
    assertThat(endpoint).isInstanceOf(ClientRoutesEndPoint.class);

    // Cache is empty (no PrivateLink route) → resolves to the regular endpoint address
    SocketAddress resolved = ((ClientRoutesEndPoint) endpoint).resolve();
    assertThat(resolved).isEqualTo(fallbackAddress);
    Mockito.verify(localEndPoint).resolve();
  }

  // ---- savePort() --------------------------------------------------------

  @Test
  public void port_should_be_set_from_config_in_constructor() {
    DriverChannel channel = Mockito.mock(DriverChannel.class);
    handler.savePort(channel);

    // Port is set from ClientRoutesConfig in the constructor (default 9042), savePort is a no-op.
    assertThat(handler.port).isEqualTo(9042);
  }

  @Test
  public void savePort_should_skip_when_port_already_set() {
    handler.port = 12345;

    DriverChannel channel = Mockito.mock(DriverChannel.class);
    handler.savePort(channel);

    // Port remains unchanged
    assertThat(handler.port).isEqualTo(12345);
  }

  // ---- Multi-endpoint tests -----------------------------------------------

  @Test
  public void should_use_in_clause_with_multiple_connection_ids() {
    String connId1 = "conn-1";
    String connId2 = "conn-2";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId1, "nlb1.example.com"))
            .addEndpoint(new ClientRouteProxy(connId2, "nlb2.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));
    when(controlConnection.init(anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    h.init();

    String query = h.lastCapturedQuery();
    assertThat(query)
        .contains("WHERE connection_id IN (")
        .contains("'" + connId1 + "'")
        .contains("'" + connId2 + "'")
        .contains("ALLOW FILTERING");
  }

  @Test
  public void should_apply_correct_override_per_connection_id_with_multiple_endpoints()
      throws Exception {
    String connId1 = "conn-1";
    String connId2 = "conn-2";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId1, "nlb1.example.com"))
            .addEndpoint(new ClientRouteProxy(connId2, "nlb2.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId1 = UUID.randomUUID();
    AdminRow row1 = Mockito.mock(AdminRow.class);
    when(row1.isNull("host_id")).thenReturn(false);
    // Address stubs lenient for the reason given in
    // should_apply_connection_addr_override_when_connection_id_matches.
    Mockito.lenient().when(row1.isNull("address")).thenReturn(false);
    when(row1.isNull("port")).thenReturn(false);
    when(row1.getUuid("host_id")).thenReturn(hostId1);
    Mockito.lenient().when(row1.getString("address")).thenReturn("10.0.0.1");
    when(row1.getInteger("port")).thenReturn(9042);
    when(row1.contains("connection_id")).thenReturn(true);
    when(row1.isNull("connection_id")).thenReturn(false);
    when(row1.getString("connection_id")).thenReturn(connId1);

    UUID hostId2 = UUID.randomUUID();
    AdminRow row2 = Mockito.mock(AdminRow.class);
    when(row2.isNull("host_id")).thenReturn(false);
    Mockito.lenient().when(row2.isNull("address")).thenReturn(false);
    when(row2.isNull("port")).thenReturn(false);
    when(row2.getUuid("host_id")).thenReturn(hostId2);
    Mockito.lenient().when(row2.getString("address")).thenReturn("10.0.0.2");
    when(row2.getInteger("port")).thenReturn(9042);
    when(row2.contains("connection_id")).thenReturn(true);
    when(row2.isNull("connection_id")).thenReturn(false);
    when(row2.getString("connection_id")).thenReturn(connId2);

    h.setNextQueryResult(AdminResultTestHelper.mockResult(row1, row2));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).hasSize(2);
    assertThat(h.getRoutes().get(hostId1).getHostname()).isEqualTo("nlb1.example.com");
    assertThat(h.getRoutes().get(hostId2).getHostname()).isEqualTo("nlb2.example.com");
  }

  @Test
  public void should_merge_routes_from_multiple_connection_ids_correctly() throws Exception {
    String connId1 = "conn-1";
    String connId2 = "conn-2";
    String connId3 = "conn-3";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId1, "nlb1.example.com"))
            .addEndpoint(new ClientRouteProxy(connId2, "nlb2.example.com"))
            .addEndpoint(new ClientRouteProxy(connId3, "nlb3.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();
    UUID hostId3 = UUID.randomUUID();

    AdminRow row1 = Mockito.mock(AdminRow.class);
    when(row1.isNull("host_id")).thenReturn(false);
    // Address stubs lenient for the reason given in
    // should_apply_connection_addr_override_when_connection_id_matches.
    Mockito.lenient().when(row1.isNull("address")).thenReturn(false);
    when(row1.isNull("port")).thenReturn(false);
    when(row1.getUuid("host_id")).thenReturn(hostId1);
    Mockito.lenient().when(row1.getString("address")).thenReturn("10.0.0.1");
    when(row1.getInteger("port")).thenReturn(9042);
    when(row1.contains("connection_id")).thenReturn(true);
    when(row1.isNull("connection_id")).thenReturn(false);
    when(row1.getString("connection_id")).thenReturn(connId1);

    AdminRow row2 = Mockito.mock(AdminRow.class);
    when(row2.isNull("host_id")).thenReturn(false);
    Mockito.lenient().when(row2.isNull("address")).thenReturn(false);
    when(row2.isNull("port")).thenReturn(false);
    when(row2.getUuid("host_id")).thenReturn(hostId2);
    Mockito.lenient().when(row2.getString("address")).thenReturn("10.0.0.2");
    when(row2.getInteger("port")).thenReturn(9043);
    when(row2.contains("connection_id")).thenReturn(true);
    when(row2.isNull("connection_id")).thenReturn(false);
    when(row2.getString("connection_id")).thenReturn(connId2);

    AdminRow row3 = Mockito.mock(AdminRow.class);
    when(row3.isNull("host_id")).thenReturn(false);
    Mockito.lenient().when(row3.isNull("address")).thenReturn(false);
    when(row3.isNull("port")).thenReturn(false);
    when(row3.getUuid("host_id")).thenReturn(hostId3);
    Mockito.lenient().when(row3.getString("address")).thenReturn("10.0.0.3");
    when(row3.getInteger("port")).thenReturn(9044);
    when(row3.contains("connection_id")).thenReturn(true);
    when(row3.isNull("connection_id")).thenReturn(false);
    when(row3.getString("connection_id")).thenReturn(connId3);

    h.setNextQueryResult(AdminResultTestHelper.mockResult(row1, row2, row3));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).hasSize(3);
    assertThat(h.getRoutes().get(hostId1).getHostname()).isEqualTo("nlb1.example.com");
    assertThat(h.getRoutes().get(hostId1).getPort()).isEqualTo(9042);
    assertThat(h.getRoutes().get(hostId2).getHostname()).isEqualTo("nlb2.example.com");
    assertThat(h.getRoutes().get(hostId2).getPort()).isEqualTo(9043);
    assertThat(h.getRoutes().get(hostId3).getHostname()).isEqualTo("nlb3.example.com");
    assertThat(h.getRoutes().get(hostId3).getPort()).isEqualTo(9044);

    // Verify query uses IN clause with all three connection IDs
    String query = h.lastCapturedQuery();
    assertThat(query)
        .contains("WHERE connection_id IN (")
        .contains("'" + connId1 + "'")
        .contains("'" + connId2 + "'")
        .contains("'" + connId3 + "'");
  }

  @Test
  public void should_not_keep_cached_route_for_unusable_row_with_several_connection_ids()
      throws Exception {
    // system.client_routes is keyed (connection_id, host_id) while the cache is keyed on host_id
    // alone, so with two connection IDs configured "the cached entry for this host" and "the row
    // for this host that failed" need not be the same route. Here conn-1's row is gone -- the
    // route the cache holds was deleted server-side -- and only conn-2's unusable row comes back.
    // Carrying the entry over would keep the deleted route for as long as conn-2's row stayed
    // unusable, and no non-empty refresh resets that, so nothing is carried over.
    // should_keep_cached_route_for_unusable_row_on_full_refresh is the one-connection-ID case,
    // where absence from the result really does mean the route is gone.
    String connId1 = "conn-1";
    String connId2 = "conn-2";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId1, "nlb1.example.com"))
            .addEndpoint(new ClientRouteProxy(connId2, "nlb2.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId = UUID.randomUUID();
    h.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "nlb1.example.com", 9042)));

    h.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(hostId, "10.0.0.1", "port", 0, connId2)));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).isEmpty();
  }

  @Test
  public void should_evict_targeted_host_with_unusable_row_with_several_connection_ids()
      throws Exception {
    // The same rule on the other cache writer. The removal sweep reads a host ID the event named
    // but the refresh cannot keep as a server-side delete, and with several connection IDs a row
    // that came back unusable is no longer a reason to keep the host: it may be a different
    // route's row than the one cached. should_keep_cached_route_when_targeted_refresh_returns
    // _unusable_row is the one-connection-ID counterweight.
    String connId1 = "conn-1";
    String connId2 = "conn-2";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId1, "nlb1.example.com"))
            .addEndpoint(new ClientRouteProxy(connId2, "nlb2.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    // init() before the seed: its own full refresh (against the default empty result) would
    // otherwise run after it, and the empty-result guard would be the thing under test.
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));
    when(controlConnection.init(anyBoolean(), anyBoolean(), anyBoolean()))
        .thenReturn(CompletableFuture.completedFuture(null));
    h.init();

    UUID hostId = UUID.randomUUID();
    h.setRoutes(ImmutableMap.of(hostId, new ClientRouteRecord(hostId, "nlb1.example.com", 9042)));
    h.setNextQueryResult(
        AdminResultTestHelper.mockResult(mockRouteRow(hostId, "10.0.0.1", "port", 0, connId2)));

    eventBus.fire(
        new ClientRoutesUpdateEvent(
            "UPDATED",
            Collections.singletonList(connId2),
            Collections.singletonList(hostId.toString())));

    assertThat(h.getRoutes()).isEmpty();
  }

  @Test
  public void should_not_keep_cached_routes_for_unreadable_host_id_with_several_connection_ids()
      throws Exception {
    // A row whose host_id the driver cannot read makes absence from the result stop being proof of
    // a delete, so with one connection ID the whole cache is kept -- see
    // should_keep_all_cached_routes_when_no_row_names_a_readable_host_id. That rests on the same
    // identity assumption as every other carry-over and goes the same way when it fails: keeping
    // routes the refresh cannot attribute would keep deleted ones among them, indefinitely.
    String connId1 = "conn-1";
    String connId2 = "conn-2";
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connId1, "nlb1.example.com"))
            .addEndpoint(new ClientRouteProxy(connId2, "nlb2.example.com"))
            .build();
    TestableClientRoutesTopologyMonitor h =
        new TestableClientRoutesTopologyMonitor(context, config);

    UUID hostId1 = UUID.randomUUID();
    UUID hostId2 = UUID.randomUUID();
    h.setRoutes(
        ImmutableMap.of(
            hostId1, new ClientRouteRecord(hostId1, "nlb1.example.com", 9042),
            hostId2, new ClientRouteRecord(hostId2, "nlb2.example.com", 9042)));

    AdminRow unreadableRow = Mockito.mock(AdminRow.class);
    when(unreadableRow.isNull("host_id")).thenReturn(true);

    h.setNextQueryResult(AdminResultTestHelper.mockResult(unreadableRow));
    when(controlConnection.channel()).thenReturn(Mockito.mock(DriverChannel.class));

    h.refresh().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertThat(h.getRoutes()).isEmpty();
  }
}
