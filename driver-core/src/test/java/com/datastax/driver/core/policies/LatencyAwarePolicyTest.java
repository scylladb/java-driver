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
package com.datastax.driver.core.policies;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.read_request_timeout;
import static org.scassandra.http.client.Result.unavailable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.LatencyTracker;
import com.datastax.driver.core.ScassandraTestBase;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.google.common.collect.Lists;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import org.testng.annotations.Test;

public class LatencyAwarePolicyTest extends ScassandraTestBase {

  /**
   * A special latency tracker used to signal to the main thread that all trackers have finished
   * their jobs.
   */
  private class LatencyTrackerBarrier implements LatencyTracker {

    private final CountDownLatch latch;

    private LatencyTrackerBarrier(int numberOfQueries) {
      latch = new CountDownLatch(numberOfQueries);
    }

    @Override
    public void update(Host host, Statement statement, Exception exception, long newLatencyNanos) {
      latch.countDown();
    }

    public void await() throws InterruptedException {
      latch.await(10, SECONDS);
    }

    @Override
    public void onRegister(Cluster cluster) {}

    @Override
    public void onUnregister(Cluster cluster) {}
  }

  @Test(groups = "short")
  public void should_consider_latency_when_query_successful() throws Exception {
    // given
    String query = "SELECT foo FROM bar";
    primingClient.prime(queryBuilder().withQuery(query).build());
    LatencyAwarePolicy latencyAwarePolicy =
        LatencyAwarePolicy.builder(new RoundRobinPolicy()).withMininumMeasurements(1).build();
    Cluster.Builder builder = super.createClusterBuilder();
    builder.withLoadBalancingPolicy(latencyAwarePolicy);
    Cluster cluster = builder.build();
    try {
      cluster.init(); // force initialization of latency aware policy
      LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
      cluster.register(
          barrier); // add barrier to synchronize latency tracker threads with the current thread
      Session session = cluster.connect();
      // when
      session.execute(query);
      // then
      // wait until trackers have been notified
      barrier.await();
      // make sure the updater is called at least once
      latencyAwarePolicy.new Updater().run();
      LatencyAwarePolicy.Snapshot snapshot = latencyAwarePolicy.getScoresSnapshot();
      assertThat(snapshot.getAllStats()).hasSize(1);
      LatencyAwarePolicy.Snapshot.Stats stats = snapshot.getStats(retrieveSingleHost(cluster));
      assertThat(stats).isNotNull();
      assertThat(stats.getMeasurementsCount()).isEqualTo(1);
      assertThat(stats.getLatencyScore()).isNotEqualTo(-1);
    } finally {
      cluster.close();
    }
  }

  @Test(groups = "short")
  public void should_discard_latency_when_unavailable() throws Exception {
    // given
    String query = "SELECT foo FROM bar";
    primingClient.prime(
        queryBuilder().withQuery(query).withThen(then().withResult(unavailable)).build());
    LatencyAwarePolicy latencyAwarePolicy =
        LatencyAwarePolicy.builder(new RoundRobinPolicy()).withMininumMeasurements(1).build();
    Cluster.Builder builder = super.createClusterBuilder();
    builder.withLoadBalancingPolicy(latencyAwarePolicy);
    Cluster cluster = builder.build();
    try {
      cluster.init(); // force initialization of latency aware policy
      LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
      cluster.register(barrier);
      Session session = cluster.connect();
      // when
      try {
        session.execute(query);
        fail("Should have thrown NoHostAvailableException");
      } catch (NoHostAvailableException e) {
        // ok
        Throwable error = e.getErrors().get(hostEndPoint);
        assertThat(error).isNotNull();
        assertThat(error).isInstanceOf(UnavailableException.class);
      }
      // then
      // wait until trackers have been notified
      barrier.await();
      // make sure the updater is called at least once
      latencyAwarePolicy.new Updater().run();
      LatencyAwarePolicy.Snapshot snapshot = latencyAwarePolicy.getScoresSnapshot();
      assertThat(snapshot.getAllStats()).isEmpty();
      LatencyAwarePolicy.Snapshot.Stats stats = snapshot.getStats(retrieveSingleHost(cluster));
      assertThat(stats).isNull();
    } finally {
      cluster.close();
    }
  }

  @Test(groups = "short")
  public void should_consider_latency_when_read_timeout() throws Exception {
    String query = "SELECT foo FROM bar";
    primingClient.prime(
        queryBuilder().withQuery(query).withThen(then().withResult(read_request_timeout)).build());

    LatencyAwarePolicy latencyAwarePolicy =
        LatencyAwarePolicy.builder(new RoundRobinPolicy()).withMininumMeasurements(1).build();
    Cluster.Builder builder = super.createClusterBuilder();
    builder.withLoadBalancingPolicy(latencyAwarePolicy);
    builder.withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
    Cluster cluster = builder.build();
    try {
      cluster.init(); // force initialization of latency aware policy
      LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
      cluster.register(barrier);
      Session session = cluster.connect();
      // when
      try {
        session.execute(query);
        fail("Should have thrown ReadTimeoutException");
      } catch (ReadTimeoutException e) {
        // ok
      }
      // then
      // wait until trackers have been notified
      barrier.await();
      // make sure the updater is called at least once
      latencyAwarePolicy.new Updater().run();
      LatencyAwarePolicy.Snapshot snapshot = latencyAwarePolicy.getScoresSnapshot();
      assertThat(snapshot.getAllStats()).hasSize(1);
      LatencyAwarePolicy.Snapshot.Stats stats = snapshot.getStats(retrieveSingleHost(cluster));
      assertThat(stats).isNotNull();
      assertThat(stats.getMeasurementsCount()).isEqualTo(1);
      assertThat(stats.getLatencyScore()).isNotEqualTo(-1);
    } finally {
      cluster.close();
    }
  }

  @Test(groups = "short")
  public void should_not_reorder_query_plan_for_lwt_queries() throws Exception {
    // given
    String query = "SELECT foo FROM bar";
    primingClient.prime(queryBuilder().withQuery(query).build());

    LatencyAwarePolicy latencyAwarePolicy =
        LatencyAwarePolicy.builder(new RoundRobinPolicy()).withMininumMeasurements(1).build();

    Cluster.Builder builder = super.createClusterBuilder();
    builder.withLoadBalancingPolicy(latencyAwarePolicy);

    Cluster cluster = builder.build();
    try {
      cluster.init();

      // Create an LWT statement so latency-aware policy must preserve child ordering
      Statement lwtStatement =
          new SimpleStatement(query) {
            @Override
            public boolean isLWT() {
              return true;
            }
          };

      // Make a request to populate latency metrics
      LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
      cluster.register(barrier);
      Session session = cluster.connect();
      session.execute(query);
      barrier.await();
      latencyAwarePolicy.new Updater().run();

      // when
      Iterator<Host> plan1 = latencyAwarePolicy.newQueryPlan("ks", lwtStatement);
      Iterator<Host> plan2 = latencyAwarePolicy.newQueryPlan("ks", lwtStatement);

      // then
      Host host = retrieveSingleHost(cluster);
      assertThat(Lists.newArrayList(plan1)).containsExactly(host);
      assertThat(Lists.newArrayList(plan2)).containsExactly(host);
    } finally {
      cluster.close();
    }
  }

  @Test(groups = "short")
  public void should_not_reorder_query_plan_for_serial_consistency_queries() throws Exception {
    // given
    String query = "SELECT foo FROM bar";
    primingClient.prime(queryBuilder().withQuery(query).build());

    LatencyAwarePolicy latencyAwarePolicy =
        LatencyAwarePolicy.builder(new RoundRobinPolicy()).withMininumMeasurements(1).build();

    Cluster.Builder builder = super.createClusterBuilder();
    builder.withLoadBalancingPolicy(latencyAwarePolicy);

    Cluster cluster = builder.build();
    try {
      cluster.init();

      // Create a statement with LOCAL_SERIAL consistency (not isLWT)
      Statement serialStatement =
          new SimpleStatement(query)
              .setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);

      // Make a request to populate latency metrics
      LatencyTrackerBarrier barrier = new LatencyTrackerBarrier(1);
      cluster.register(barrier);
      Session session = cluster.connect();
      session.execute(query);
      barrier.await();
      latencyAwarePolicy.new Updater().run();

      // when
      Iterator<Host> plan1 = latencyAwarePolicy.newQueryPlan("ks", serialStatement);
      Iterator<Host> plan2 = latencyAwarePolicy.newQueryPlan("ks", serialStatement);

      // then: ordering is preserved (not reordered by latency)
      Host host = retrieveSingleHost(cluster);
      assertThat(Lists.newArrayList(plan1)).containsExactly(host);
      assertThat(Lists.newArrayList(plan2)).containsExactly(host);
    } finally {
      cluster.close();
    }
  }
}
