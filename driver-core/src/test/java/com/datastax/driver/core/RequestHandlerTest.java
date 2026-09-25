/*
 * Copyright DataStax, Inc.
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

/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.scassandra.http.client.PrimingRequest.preparedStatementBuilder;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.unprepared;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.scassandra.Scassandra;
import org.scassandra.http.client.UnpreparedConfig;
import org.testng.annotations.Test;

public class RequestHandlerTest {

  @Test(groups = "short")
  public void should_try_next_host_when_borrow_connection_future_fails() throws Exception {
    ScassandraCluster scassandra = ScassandraCluster.builder().withNodes(2).build();
    Cluster cluster = null;

    try {
      scassandra.init();
      scassandra
          .node(2)
          .primingClient()
          .prime(
              queryBuilder()
                  .withQuery("mock query")
                  .withThen(then().withRows(row("result", "result2")))
                  .build());

      cluster = newCluster(scassandra);
      Session session = cluster.connect();
      Host host1 = TestUtils.findHost(cluster, 1);
      Host host2 = TestUtils.findHost(cluster, 2);

      failBorrowingFrom(session, host1, new ConnectionException(host1.getEndPoint(), "mock"));

      ResultSet rs = session.execute("mock query");

      assertThat(rs.one().getString("result")).isEqualTo("result2");
      assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(host2);
    } finally {
      if (cluster != null) cluster.close();
      scassandra.stop();
    }
  }

  @Test(groups = "short")
  public void should_report_no_host_when_unprepared_prepare_write_fails_synchronously()
      throws Exception {
    final Scassandra scassandra = TestUtils.createScassandraServer();
    Cluster cluster = null;

    try {
      scassandra.start();
      ScassandraCluster.primeSystemLocalRow(scassandra);
      String query = "SELECT v FROM mock_table";
      scassandra
          .primingClient()
          .prime(
              preparedStatementBuilder()
                  .withQuery(query)
                  .withThen(then().withRows(row("result", "result1")))
                  .build());

      cluster =
          Cluster.builder()
              .addContactPoint(TestUtils.ipOfNode(1))
              .withPort(scassandra.getBinaryPort())
              .withPoolingOptions(
                  new PoolingOptions()
                      .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                      .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                      .setHeartbeatIntervalSeconds(0))
              .build();
      Session session = cluster.connect();
      Host host = TestUtils.findHost(cluster, 1);
      PreparedStatement prepared = session.prepare(query);

      scassandra.primingClient().clearPreparedPrimes();
      scassandra
          .primingClient()
          .prime(
              preparedStatementBuilder()
                  .withQuery(query)
                  .withThen(
                      then()
                          .withResult(unprepared)
                          .withFixedDelay(200L)
                          .withConfig(
                              new UnpreparedConfig(
                                  prepared.getPreparedId().boundValuesMetadata.id.toString())))
                  .build());

      Connection connection = getSingleConnection(session);
      ResultSetFuture future = session.executeAsync(prepared.bind());
      waitForInFlight(connection);
      markDefunctWithoutClosing(connection);

      try {
        future.getUninterruptibly(5, TimeUnit.SECONDS);
        fail("expected a NoHostAvailableException");
      } catch (NoHostAvailableException e) {
        assertThat(e.getErrors()).containsKey(host.getEndPoint());
        assertThat(e.getErrors().get(host.getEndPoint()))
            .isInstanceOf(ConnectionException.class)
            .hasMessageContaining("Write attempt on defunct connection");
      }
    } finally {
      if (cluster != null) cluster.close();
      scassandra.stop();
    }
  }

  @Test(groups = "short")
  public void should_handle_race_between_response_and_cancellation() {
    final Scassandra scassandra = TestUtils.createScassandraServer();
    Cluster cluster = null;

    try {
      // Use a mock server that takes a constant time to reply
      scassandra.start();
      ScassandraCluster.primeSystemLocalRow(scassandra);
      List<Map<String, ?>> rows =
          Collections.<Map<String, ?>>singletonList(ImmutableMap.of("key", 1));
      scassandra
          .primingClient()
          .prime(
              queryBuilder()
                  .withQuery("mock query")
                  .withThen(then().withRows(rows).withFixedDelay(10L))
                  .build());

      cluster =
          Cluster.builder()
              .addContactPoint(TestUtils.ipOfNode(1))
              .withPort(scassandra.getBinaryPort())
              .withPoolingOptions(
                  new PoolingOptions()
                      .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                      .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                      .setHeartbeatIntervalSeconds(0))
              .build();

      Session session = cluster.connect();

      // To reproduce, we need to cancel the query exactly when the reply arrives.
      // Run a few queries to estimate how much that will take.
      int samples = 100;
      long start = System.currentTimeMillis();
      for (int i = 0; i < samples; i++) {
        session.execute("mock query");
      }
      long elapsed = System.currentTimeMillis() - start;
      long queryDuration = elapsed / samples;

      // Now run queries and cancel them after that estimated time
      for (int i = 0; i < 2000; i++) {
        ResultSetFuture future = session.executeAsync("mock query");
        try {
          future.getUninterruptibly(queryDuration, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          future.cancel(true);
        }
      }

      Connection connection = getSingleConnection(session);
      assertThat(connection.inFlight.get()).isEqualTo(0);
    } finally {
      if (cluster != null) cluster.close();
      scassandra.stop();
    }
  }

  private Connection getSingleConnection(Session session) {
    HostConnectionPool pool = ((SessionManager) session).pools.values().iterator().next();
    return pool.connections[0].get(0);
  }

  private Cluster newCluster(ScassandraCluster scassandra) {
    return Cluster.builder()
        .addContactPoints(scassandra.address(1).getAddress())
        .withPort(scassandra.getBinaryPort())
        .withLoadBalancingPolicy(new SortingLoadBalancingPolicy())
        .withPoolingOptions(
            new PoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                .setHeartbeatIntervalSeconds(0))
        .build();
  }

  private void failBorrowingFrom(Session session, Host host, ConnectionException failure)
      throws Exception {
    SessionManager manager = (SessionManager) session;
    HostConnectionPool pool = spy(manager.pools.get(host));
    manager.pools.put(host, pool);
    doReturn(Futures.<Connection>immediateFailedFuture(failure))
        .when(pool)
        .borrowConnection(
            anyLong(),
            (TimeUnit) any(),
            anyInt(),
            (Token.Factory) any(),
            (ByteBuffer) any(),
            (String) any(),
            (String) any());
  }

  private void waitForInFlight(Connection connection) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    boolean observedInFlight = false;
    while (System.nanoTime() < deadline) {
      if (connection.inFlight.get() > 0) {
        observedInFlight = true;
        break;
      }
      Thread.sleep(1);
    }
    assertThat(observedInFlight).isTrue();
  }

  private void markDefunctWithoutClosing(Connection connection) {
    // Keep the pending EXECUTE alive; only the following PREPARE write should see defunct.
    connection.markDefunctForTest();
  }

  private static List<Map<String, ?>> row(String key, String value) {
    return Collections.<Map<String, ?>>singletonList(ImmutableMap.of(key, value));
  }
}
