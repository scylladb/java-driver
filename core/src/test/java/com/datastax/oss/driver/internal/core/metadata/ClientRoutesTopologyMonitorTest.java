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
          volatile boolean firstPostInitDone = false;

          @Override
          @NonNull
          protected CompletionStage<AdminResult> runAdminQuery(
              @NonNull DriverChannel channel,
              @NonNull String queryString,
              @NonNull Duration timeout) {
            capturedQueries.add(queryString);
            if (queryString.contains("system.client_routes") && !firstPostInitDone) {
              // First client_routes query is during init — return immediately
              firstPostInitDone = true;
              return CompletableFuture.completedFuture(emptyResult);
            }
            if (queryString.contains("system.client_routes")) {
              // Second client_routes query — block on delayedFuture
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
    when(row.isNull("address")).thenReturn(false);
    when(row.isNull("port")).thenReturn(false);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getString("address")).thenReturn("original.example.com");
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
    when(matchingRow.isNull("address")).thenReturn(false);
    when(matchingRow.isNull("port")).thenReturn(false);
    when(matchingRow.getUuid("host_id")).thenReturn(hostId1);
    when(matchingRow.getString("address")).thenReturn("original-1.example.com");
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
  public void savePort_should_use_route_port_when_routes_available() {
    UUID id1 = UUID.randomUUID();
    UUID id2 = UUID.randomUUID();
    handler.setRoutes(
        ImmutableMap.of(
            id1, new ClientRouteRecord(id1, "127.0.0.1", 19042),
            id2, new ClientRouteRecord(id2, "127.0.0.2", 19042)));

    DriverChannel channel = Mockito.mock(DriverChannel.class);
    handler.savePort(channel);

    assertThat(handler.port).isEqualTo(19042);
  }

  @Test
  public void savePort_should_fall_through_to_super_when_routes_empty() {
    // routes cache is empty by default
    DriverChannel channel = Mockito.mock(DriverChannel.class);
    EndPoint ep = Mockito.mock(EndPoint.class);
    when(ep.resolve()).thenReturn(new InetSocketAddress("127.0.0.1", 9042));
    when(channel.getEndPoint()).thenReturn(ep);

    handler.savePort(channel);

    assertThat(handler.port).isEqualTo(9042);
  }

  @Test
  public void savePort_should_skip_when_port_already_set() {
    handler.port = 12345;
    UUID id = UUID.randomUUID();
    handler.setRoutes(ImmutableMap.of(id, new ClientRouteRecord(id, "127.0.0.1", 19042)));

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
    when(row1.isNull("address")).thenReturn(false);
    when(row1.isNull("port")).thenReturn(false);
    when(row1.getUuid("host_id")).thenReturn(hostId1);
    when(row1.getString("address")).thenReturn("10.0.0.1");
    when(row1.getInteger("port")).thenReturn(9042);
    when(row1.contains("connection_id")).thenReturn(true);
    when(row1.isNull("connection_id")).thenReturn(false);
    when(row1.getString("connection_id")).thenReturn(connId1);

    UUID hostId2 = UUID.randomUUID();
    AdminRow row2 = Mockito.mock(AdminRow.class);
    when(row2.isNull("host_id")).thenReturn(false);
    when(row2.isNull("address")).thenReturn(false);
    when(row2.isNull("port")).thenReturn(false);
    when(row2.getUuid("host_id")).thenReturn(hostId2);
    when(row2.getString("address")).thenReturn("10.0.0.2");
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
    when(row1.isNull("address")).thenReturn(false);
    when(row1.isNull("port")).thenReturn(false);
    when(row1.getUuid("host_id")).thenReturn(hostId1);
    when(row1.getString("address")).thenReturn("10.0.0.1");
    when(row1.getInteger("port")).thenReturn(9042);
    when(row1.contains("connection_id")).thenReturn(true);
    when(row1.isNull("connection_id")).thenReturn(false);
    when(row1.getString("connection_id")).thenReturn(connId1);

    AdminRow row2 = Mockito.mock(AdminRow.class);
    when(row2.isNull("host_id")).thenReturn(false);
    when(row2.isNull("address")).thenReturn(false);
    when(row2.isNull("port")).thenReturn(false);
    when(row2.getUuid("host_id")).thenReturn(hostId2);
    when(row2.getString("address")).thenReturn("10.0.0.2");
    when(row2.getInteger("port")).thenReturn(9043);
    when(row2.contains("connection_id")).thenReturn(true);
    when(row2.isNull("connection_id")).thenReturn(false);
    when(row2.getString("connection_id")).thenReturn(connId2);

    AdminRow row3 = Mockito.mock(AdminRow.class);
    when(row3.isNull("host_id")).thenReturn(false);
    when(row3.isNull("address")).thenReturn(false);
    when(row3.isNull("port")).thenReturn(false);
    when(row3.getUuid("host_id")).thenReturn(hostId3);
    when(row3.getString("address")).thenReturn("10.0.0.3");
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
}
