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

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.scassandra.http.client.PrimingRequest.then;

import com.datastax.driver.core.SystemColumnProjection.SystemTable;
import com.datastax.driver.core.policies.Policies;
import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.testng.annotations.Test;

/**
 * Covers the ScyllaDB-specific recovery paths of {@link ControlConnection}: the column-projection
 * cache reset on {@code InvalidQueryException} in the projected queries that {@link
 * SystemColumnProjection#hook} does not see, and the original-contacts reconnection plan.
 */
public class ControlConnectionRecoveryTest {

  private static final String SELECT_LOCAL_ALL = "SELECT * FROM system.local WHERE key='local'";
  private static final String SELECT_PEERS_ALL = "SELECT * FROM system.peers";

  @Test(groups = "short")
  public void should_reset_column_caches_when_projected_local_lookup_is_invalid() throws Exception {
    assertResetOnInvalidLocalQuery(
        new Refresh() {
          @Override
          public void run(ControlConnection controlConnection) {
            assertThat(controlConnection.refreshNodeInfo(controlConnection.connectedHost()))
                .isTrue();
          }
        });
  }

  @Test(groups = "short")
  public void should_reset_column_caches_when_projected_local_query_is_invalid() throws Exception {
    assertResetOnInvalidLocalQuery(
        new Refresh() {
          @Override
          public void run(ControlConnection controlConnection) {
            controlConnection.refreshNodeListAndTokenMap();
          }
        });
  }

  @Test(groups = "short")
  public void should_reconnect_to_original_contact_point_when_query_plan_is_empty()
      throws Exception {
    ScassandraCluster scassandras = ScassandraCluster.builder().withNodes(2).build();
    scassandras.init();
    ReconnectionTest.TogglabePolicy policy =
        new ReconnectionTest.TogglabePolicy(Policies.defaultLoadBalancingPolicy());
    Cluster cluster =
        newCluster(scassandras)
            .withLoadBalancingPolicy(policy)
            .withQueryOptions(new QueryOptions().setAddOriginalContactsToReconnectionPlan(true))
            .build();
    try {
      cluster.init();
      final ControlConnection controlConnection = cluster.manager.controlConnection;
      final Connection before = controlConnection.connectionRef.get();
      assertThat(before).isNotNull();

      // With no host from the load balancing policy, only the original contact points remain.
      policy.returnEmptyQueryPlan = true;
      controlConnection.triggerReconnect();

      ConditionChecker.check()
          .that(
              new Callable<Boolean>() {
                @Override
                public Boolean call() {
                  Connection current = controlConnection.connectionRef.get();
                  return current != null && current != before;
                }
              })
          .every(50, TimeUnit.MILLISECONDS)
          .before(10, TimeUnit.SECONDS)
          .becomesTrue();
      assertThat(controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(scassandras.address(1));
    } finally {
      cluster.close();
      scassandras.stop();
    }
  }

  private interface Refresh {
    void run(ControlConnection controlConnection);
  }

  /**
   * Fails the projected {@code system.local} query, which is not hooked on either refresh path, so
   * the {@code InvalidQueryException} reaches the refresh method's own reset.
   */
  private static void assertResetOnInvalidLocalQuery(Refresh refresh) throws Exception {
    ScassandraCluster scassandras = ScassandraCluster.builder().withNodes(2).build();
    scassandras.init();
    Cluster cluster = newCluster(scassandras).build();
    CountDownLatch release = new CountDownLatch(1);
    try {
      cluster.init();
      SystemColumnProjection projection = projection(cluster);
      assertWarm(projection);

      PrimingClient primingClient = scassandras.node(1).primingClient();
      primingClient.clearAllPrimes();
      primingClient.prime(
          PrimingRequest.queryBuilder()
              .withQuery(projection.query(SystemTable.LOCAL))
              .withThen(then().withResult(Result.invalid))
              .build());

      // The error also triggers a reconnect, whose tryConnect() resets the caches too. Hold the
      // reconnection threads so it can't race the assertion.
      blockReconnectionExecutor(cluster, release);

      refresh.run(cluster.manager.controlConnection);

      assertCold(projection);
    } finally {
      release.countDown();
      cluster.close();
      scassandras.stop();
    }
  }

  private static Cluster.Builder newCluster(ScassandraCluster scassandras) {
    return Cluster.builder()
        .addContactPoints(scassandras.address(1).getAddress())
        .withPort(scassandras.getBinaryPort())
        .withNettyOptions(nonQuietClusterCloseOptions);
  }

  private static SystemColumnProjection projection(Cluster cluster) throws Exception {
    Field field = ControlConnection.class.getDeclaredField("projection");
    field.setAccessible(true);
    return (SystemColumnProjection) field.get(cluster.manager.controlConnection);
  }

  private static void assertWarm(SystemColumnProjection projection) {
    assertThat(projection.query(SystemTable.LOCAL)).isNotEqualTo(SELECT_LOCAL_ALL);
    assertThat(projection.query(SystemTable.PEERS)).isNotEqualTo(SELECT_PEERS_ALL);
  }

  private static void assertCold(SystemColumnProjection projection) {
    assertThat(projection.query(SystemTable.LOCAL)).isEqualTo(SELECT_LOCAL_ALL);
    assertThat(projection.query(SystemTable.PEERS)).isEqualTo(SELECT_PEERS_ALL);
  }

  /** Occupies every reconnection thread until {@code release} is counted down. */
  private static void blockReconnectionExecutor(Cluster cluster, final CountDownLatch release)
      throws InterruptedException {
    int threads = ((ThreadPoolExecutor) cluster.manager.reconnectionExecutor).getCorePoolSize();
    final CountDownLatch started = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      cluster.manager.reconnectionExecutor.execute(
          new Runnable() {
            @Override
            public void run() {
              started.countDown();
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          });
    }
    assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
  }
}
