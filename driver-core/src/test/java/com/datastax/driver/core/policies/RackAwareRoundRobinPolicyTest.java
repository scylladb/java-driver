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
 * Copyright (C) 2021 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core.policies;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ScassandraCluster.datacenter;
import static com.datastax.driver.core.ScassandraCluster.rack;
import static com.datastax.driver.core.TestUtils.findHost;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static com.google.common.collect.Lists.newArrayList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.MockitoAnnotations.initMocks;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataProviders;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.MemoryAppender;
import com.datastax.driver.core.QueryTracker;
import com.datastax.driver.core.ScassandraCluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.Lists;
import java.util.Collection;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RackAwareRoundRobinPolicyTest {

  private final Logger policyLogger = Logger.getLogger(RackAwareRoundRobinPolicy.class);
  private Level originalLevel;
  private MemoryAppender logs;
  private QueryTracker queryTracker;

  @Captor private ArgumentCaptor<Collection<Host>> initHostsCaptor;

  @BeforeMethod(groups = "short")
  public void setUp() {
    initMocks(this);
    originalLevel = policyLogger.getLevel();
    policyLogger.setLevel(Level.WARN);
    logs = new MemoryAppender();
    policyLogger.addAppender(logs);
    queryTracker = new QueryTracker();
  }

  @AfterMethod(groups = "short", alwaysRun = true)
  public void tearDown() {
    policyLogger.setLevel(originalLevel);
    policyLogger.removeAppender(logs);
  }

  private Cluster.Builder builder() {
    return Cluster.builder()
        // Close cluster immediately to speed up tests.
        .withNettyOptions(nonQuietClusterCloseOptions);
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will round robin within hosts in the explicitly
   * specified local DC via {@link RackAwareRoundRobinPolicy.Builder#withLocalDc(String)} and local
   * rack via {@link RackAwareRoundRobinPolicy.Builder#withLocalRack(String)}
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_round_robin_within_local_rack() {
    // given: a 10 node 2 DC cluster (3 nodes in RACK1, 2 nodes in RACK2).
    ScassandraCluster sCluster =
        ScassandraCluster.builder()
            .withNodes(5, 5)
            .withRack(1, 1, rack(1))
            .withRack(1, 2, rack(2))
            .withRack(1, 3, rack(1))
            .withRack(1, 4, rack(2))
            .withRack(1, 5, rack(1))
            .withRack(2, 1, rack(2))
            .withRack(2, 2, rack(1))
            .withRack(2, 3, rack(2))
            .withRack(2, 4, rack(1))
            .withRack(2, 5, rack(2))
            .build();

    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder()
                    .withLocalDc(datacenter(1))
                    .withLocalRack(rack(1))
                    .build())
            .build();
    try {
      sCluster.init();

      Session session = cluster.connect();
      // when: a query is executed 15 times.
      queryTracker.query(session, 15);

      // then: each node in local DC and local rack should get an equal (5) number of requests.
      queryTracker.assertQueried(sCluster, 1, 1, 5);
      queryTracker.assertQueried(sCluster, 1, 3, 5);
      queryTracker.assertQueried(sCluster, 1, 5, 5);

      // then: no node in the remote DC or remote rack in local DC should get a request.
      for (int dc = 1; dc <= 2; dc++) {
        for (int node = 1; node <= 5; node++) {
          if (dc == 1 && (node == 1 || node == 3 || node == 5)) continue;
          queryTracker.assertQueried(sCluster, dc, node, 0);
        }
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} does not use remote hosts (remote rack or remote
   * DC) if replicas in local rack are UP.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_not_use_remote_hosts_if_some_nodes_are_up_in_local_rack() {
    // given: a 10 node 2 DC cluster (3 nodes in RACK1, 2 nodes in RACK2).
    ScassandraCluster sCluster =
        ScassandraCluster.builder()
            .withNodes(5, 5)
            .withRack(1, 1, rack(1))
            .withRack(1, 2, rack(2))
            .withRack(1, 3, rack(1))
            .withRack(1, 4, rack(2))
            .withRack(1, 5, rack(1))
            .withRack(2, 1, rack(2))
            .withRack(2, 2, rack(1))
            .withRack(2, 3, rack(2))
            .withRack(2, 4, rack(1))
            .withRack(2, 5, rack(2))
            .build();
    @SuppressWarnings("deprecation")
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder()
                    .withLocalDc(datacenter(1))
                    .withLocalRack(rack(1))
                    .withUsedHostsPerRemoteDc(2)
                    .build())
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      // when: a query is executed 20 times and some hosts are down in the local rack.
      sCluster.stop(cluster, 1, 5);
      assertThat(cluster).controlHost().isNotNull();
      queryTracker.query(session, 20);

      // then: all requests should be distributed to the remaining up nodes in local DC.
      queryTracker.assertQueried(sCluster, 1, 1, 10);
      queryTracker.assertQueried(sCluster, 1, 3, 10);

      // then: no nodes in the remote DC should have been queried.
      for (int i = 1; i <= 5; i++) {
        queryTracker.assertQueried(sCluster, 2, i, 0);
      }

      // then: no nodes in the remote rack should have been queried.
      queryTracker.assertQueried(sCluster, 1, 2, 0);
      queryTracker.assertQueried(sCluster, 1, 4, 0);
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} does not use remote DC if some replicas in local
   * DC and remote rack are UP.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_not_use_remote_hosts_if_some_nodes_are_up_in_remote_dc() {
    // given: a 10 node 2 DC cluster (3 nodes in RACK1, 2 nodes in RACK2).
    ScassandraCluster sCluster =
        ScassandraCluster.builder()
            .withNodes(5, 5)
            .withRack(1, 1, rack(1))
            .withRack(1, 2, rack(2))
            .withRack(1, 3, rack(1))
            .withRack(1, 4, rack(2))
            .withRack(1, 5, rack(1))
            .withRack(2, 1, rack(2))
            .withRack(2, 2, rack(1))
            .withRack(2, 3, rack(2))
            .withRack(2, 4, rack(1))
            .withRack(2, 5, rack(2))
            .build();
    @SuppressWarnings("deprecation")
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder()
                    .withLocalDc(datacenter(1))
                    .withLocalRack(rack(1))
                    .withUsedHostsPerRemoteDc(2)
                    .build())
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      // when: a query is executed 20 times and all hosts are down in the local rack.
      sCluster.stop(cluster, 1, 1);
      sCluster.stop(cluster, 1, 3);
      sCluster.stop(cluster, 1, 5);
      assertThat(cluster).controlHost().isNotNull();
      queryTracker.query(session, 20);

      // then: all requests should be distributed to the remaining up nodes in local DC, remote
      // rack.
      queryTracker.assertQueried(sCluster, 1, 2, 10);
      queryTracker.assertQueried(sCluster, 1, 4, 10);

      // then: no nodes in the remote DC should have been queried.
      for (int i = 1; i <= 5; i++) {
        queryTracker.assertQueried(sCluster, 2, i, 0);
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will determine it's local DC and local rack
   * based on the data center of the contact point(s).
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_use_local_dc_and_rack_from_contact_points_when_not_explicitly_specified() {
    // given: a 10 node 2 DC cluster (3 nodes in RACK1, 2 nodes in RACK2).
    RackAwareRoundRobinPolicy policy = spy(RackAwareRoundRobinPolicy.builder().build());
    ScassandraCluster sCluster =
        ScassandraCluster.builder()
            .withNodes(5, 5)
            .withRack(1, 1, rack(1))
            .withRack(1, 2, rack(2))
            .withRack(1, 3, rack(1))
            .withRack(1, 4, rack(2))
            .withRack(1, 5, rack(1))
            .withRack(2, 1, rack(2))
            .withRack(2, 2, rack(1))
            .withRack(2, 3, rack(2))
            .withRack(2, 4, rack(1))
            .withRack(2, 5, rack(2))
            .build();
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host1 = findHost(cluster, 1);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only the host given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsExactly(host1);
      // then: the local dc should match the contact points' datacenter.
      assertThat(policy.localDc).isEqualTo(host1.getDatacenter());
      // then: the local rack should match the contact points' rack.
      assertThat(policy.localRack).isEqualTo(host1.getRack());
      // then: should not indicate that contact points don't match the local datacenter.
      assertThat(logs.get()).doesNotContain("Some contact points don't match local datacenter");
      // then: should not indicate that contact points don't match the local rack.
      assertThat(logs.get()).doesNotContain("Some contact points don't match local rack");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will determine it's local rack based on the
   * contact point(s) and if contact points in different racks are detected that a log message is
   * generated indicating some contact points don't match the local rack.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_warn_if_contact_points_have_different_racks_when_not_explicitly_specified() {
    // given: a a 10 node 2 DC cluster (3 nodes in RACK1, 2 nodes in RACK2) with a Cluster instance
    // with contact points in different racks.
    RackAwareRoundRobinPolicy policy = spy(RackAwareRoundRobinPolicy.builder().build());
    ScassandraCluster sCluster =
        ScassandraCluster.builder()
            .withNodes(5, 5)
            .withRack(1, 1, rack(1))
            .withRack(1, 2, rack(2))
            .withRack(1, 3, rack(1))
            .withRack(1, 4, rack(2))
            .withRack(1, 5, rack(1))
            .withRack(2, 1, rack(2))
            .withRack(2, 2, rack(1))
            .withRack(2, 3, rack(2))
            .withRack(2, 4, rack(1))
            .withRack(2, 5, rack(2))
            .build();
    Cluster cluster =
        builder()
            .addContactPoints(
                sCluster.address(1, 1).getAddress(),
                sCluster.address(1, 3).getAddress(),
                sCluster.address(1, 2).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host1 = findHost(cluster, 1);
      Host host2 = findHost(cluster, 2);
      Host host3 = findHost(cluster, 3);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only two hosts given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsOnly(host1, host2, host3);
      // then: should indicate that some contact points don't match the local datacenter.
      assertThat(logs.get()).contains("Some contact points don't match local rack");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will not log a warning if all contact points
   * match the data center and rack provided in {@link
   * RackAwareRoundRobinPolicy.Builder#withLocalDc(String)}, {@link
   * RackAwareRoundRobinPolicy.Builder#withLocalRack(String)} and that the explicitly provided local
   * data center and local rack is used.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_use_provided_local_dc_rack_and_not_warn_if_contact_points_match() {
    // given: a a 10 node 2 DC cluster (3 nodes in RACK1, 2 nodes in RACK2) with a Cluster instance
    // with contact points in the same DCs and rack.
    ScassandraCluster sCluster =
        ScassandraCluster.builder()
            .withNodes(5, 5)
            .withRack(1, 1, rack(1))
            .withRack(1, 2, rack(2))
            .withRack(1, 3, rack(1))
            .withRack(1, 4, rack(2))
            .withRack(1, 5, rack(1))
            .withRack(2, 1, rack(2))
            .withRack(2, 2, rack(1))
            .withRack(2, 3, rack(2))
            .withRack(2, 4, rack(1))
            .withRack(2, 5, rack(2))
            .build();
    RackAwareRoundRobinPolicy policy =
        spy(
            RackAwareRoundRobinPolicy.builder()
                .withLocalDc(datacenter(1))
                .withLocalRack(rack(2))
                .build());
    Cluster cluster =
        builder()
            .addContactPoints(
                sCluster.address(1, 2).getAddress(), sCluster.address(1, 4).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host2 = findHost(cluster, 2);
      Host host4 = findHost(cluster, 4);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only two hosts given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsOnly(host2, host4);
      // then: the data center should appropriately be set to the one specified.
      assertThat(policy.localDc).isEqualTo(host2.getDatacenter());
      // then: the rack should appropriately be set to the one specified.
      assertThat(policy.localRack).isEqualTo(host2.getRack());
      // then: should not indicate that contact points don't match the local datacenter.
      assertThat(logs.get()).doesNotContain("Some contact points don't match local data center");
      // then: should not indicate that contact points don't match the local datacenter.
      assertThat(logs.get()).doesNotContain("Some contact points don't match local data rack");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  // The following tests are copied from {@link DCAwareRoundRobinPolicy}. If all
  // DCs are a single-rack DC, then the behavior of {@link RackAwareRoundRobinPolicy}
  // should be exactly the same.

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will round robin within hosts in the explicitly
   * specific local DC via {@link RackAwareRoundRobinPolicy.Builder#withLocalDc(String)}
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_round_robin_within_local_dc() {
    // given: a 10 node 2 DC cluster.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder().withLocalDc(datacenter(1)).build())
            .build();
    try {
      sCluster.init();

      Session session = cluster.connect();
      // when: a query is executed 25 times.
      queryTracker.query(session, 25);

      // then: each node in local DC should get an equal (5) number of requests.
      // then: no node in the remote DC should get a request.
      for (int i = 1; i <= 5; i++) {
        queryTracker.assertQueried(sCluster, 1, i, 5);
        queryTracker.assertQueried(sCluster, 2, i, 0);
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} does not use remote hosts if replicas in the
   * local DC are UP.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_not_use_remote_hosts_if_some_nodes_are_up_in_local_dc() {
    // given: a 10 node 2 DC cluster with DC policy with 2 remote hosts.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
    @SuppressWarnings("deprecation")
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder()
                    .withLocalDc(datacenter(1))
                    .withUsedHostsPerRemoteDc(2)
                    .build())
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      // when: a query is executed 20 times and some hosts are down in the local DC.
      sCluster.stop(cluster, 1, 5);
      sCluster.stop(cluster, 1, 3);
      sCluster.stop(cluster, 1, 1);
      assertThat(cluster).controlHost().isNotNull();
      queryTracker.query(session, 20);

      // then: all requests should be distributed to the remaining up nodes in local DC.
      queryTracker.assertQueried(sCluster, 1, 2, 10);
      queryTracker.assertQueried(sCluster, 1, 4, 10);

      // then: no nodes in the remote DC should have been queried.
      for (int i = 1; i <= 5; i++) {
        queryTracker.assertQueried(sCluster, 2, i, 0);
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will round robin on remote hosts but only if no
   * local replicas are available and only within the number of hosts configured by {@link
   * RackAwareRoundRobinPolicy.Builder#withUsedHostsPerRemoteDc(int)}
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_round_robin_on_remote_hosts_when_no_up_nodes_in_local_dc() {
    // given: a 10 node 2 DC cluster with DC policy with 2 remote hosts.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(5, 5).build();
    @SuppressWarnings("deprecation")
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder().withUsedHostsPerRemoteDc(2).build())
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      sCluster.stopDC(cluster, 1);

      // Wait for control connection to be re-established, needed as
      // control connection attempts increment LBP counter.
      assertThat(cluster).controlHost().isNotNull();

      // when: a query is executed 20 times and all hosts are down in local DC.
      queryTracker.query(session, 20);

      // then: only usedHostsPerRemoteDc nodes in the remote DC should get requests.
      Collection<Integer> queryCounts = newArrayList();
      for (int i = 1; i <= 5; i++) {
        queryCounts.add(queryTracker.queryCount(sCluster, 2, i));
      }
      assertThat(queryCounts).containsOnly(0, 0, 0, 10, 10);
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will by default only use remote hosts for non DC
   * local Consistency Levels. In the case that a DC local Consistency Level is provided a {@link
   * NoHostAvailableException} is raised.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(
      groups = "short",
      dataProvider = "consistencyLevels",
      dataProviderClass = DataProviders.class)
  public void should_only_use_remote_hosts_when_using_non_dc_local_cl(ConsistencyLevel cl) {
    // given: a 4 node 2 DC Cluster with a LB policy that specifies to not allow remote dcs for
    // a local consistency level.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2).build();
    @SuppressWarnings("deprecation")
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder().withUsedHostsPerRemoteDc(2).build())
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      sCluster.stopDC(cluster, 1);

      // Wait for control connection to be re-established, needed as
      // control connection attempts increment LBP counter.
      assertThat(cluster).controlHost().isNotNull();

      // when: a query is executed 20 times and all hosts are down in local DC.
      // then: expect a NHAE for a local CL since no local replicas available.
      Class<? extends Exception> expectedException =
          cl.isDCLocal() ? NoHostAvailableException.class : null;
      queryTracker.query(session, 20, cl, expectedException);

      int expectedQueryCount = cl.isDCLocal() ? 0 : 10;
      for (int i = 1; i <= 2; i++) {
        queryTracker.assertQueried(sCluster, 1, i, 0);
        // then: Remote hosts should only be queried for non local CLs.
        queryTracker.assertQueried(sCluster, 2, i, expectedQueryCount);
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will use remote hosts for non DC local
   * Consistency Levels if {@code
   * RackAwareRoundRobinPolicy.Builder#allowRemoteDCsForLocalConsistencyLevel} is used. In the case
   * that a DC local Consistency Level is provided a {@link NoHostAvailableException} is raised.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(
      groups = "short",
      dataProvider = "consistencyLevels",
      dataProviderClass = DataProviders.class)
  public void should_use_remote_hosts_for_local_cl_when_allowed(ConsistencyLevel cl) {
    // given: a 4 node 2 DC Cluster with a LB policy that specifies to allow remote dcs for
    // a local consistency level.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2).build();
    @SuppressWarnings("deprecation")
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(
                RackAwareRoundRobinPolicy.builder()
                    .allowRemoteDCsForLocalConsistencyLevel()
                    .withUsedHostsPerRemoteDc(2)
                    .build())
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      sCluster.stopDC(cluster, 1);

      // Wait for control connection to be re-established, needed as
      // control connection attempts increment LBP counter.
      assertThat(cluster).controlHost().isNotNull();

      // when: a query is executed 20 times and all hosts are down in local DC.
      queryTracker.query(session, 20, cl, null);

      for (int i = 1; i <= 2; i++) {
        queryTracker.assertQueried(sCluster, 1, i, 0);
        // then: Remote hosts should be queried.
        queryTracker.assertQueried(sCluster, 2, i, 10);
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that when {@link RackAwareRoundRobinPolicy} is wrapped with a {@link HostFilterPolicy}
   * that blacklists a data center that nodes in that datacenter are never queried.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_not_send_requests_to_blacklisted_dc_using_host_filter_policy() {
    // given: a 6 node 3 DC cluster with a RackAwareRoundRobinPolicy that is filtering hosts in DC2.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2, 2).build();
    @SuppressWarnings("deprecation")
    LoadBalancingPolicy loadBalancingPolicy =
        HostFilterPolicy.fromDCBlackList(
            RackAwareRoundRobinPolicy.builder().withUsedHostsPerRemoteDc(2).build(),
            Lists.newArrayList(datacenter(2)));
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      // when: A query is made and nodes for the local dc are available.
      queryTracker.query(session, 20);

      // then: only nodes in the local DC should have been queried.
      queryTracker.assertQueried(sCluster, 1, 1, 10);
      queryTracker.assertQueried(sCluster, 1, 2, 10);
      queryTracker.assertQueried(sCluster, 2, 1, 0);
      queryTracker.assertQueried(sCluster, 2, 2, 0);
      queryTracker.assertQueried(sCluster, 3, 1, 0);
      queryTracker.assertQueried(sCluster, 3, 1, 0);

      // when: A query is made and all nodes in the local dc are down.
      sCluster.stopDC(cluster, 1);
      assertThat(cluster).controlHost().isNotNull();
      queryTracker.reset();
      queryTracker.query(session, 20);

      // then: Only nodes in DC3 should have been queried, since DC2 is blacklisted and DC1 is down.
      queryTracker.assertQueried(sCluster, 1, 1, 0);
      queryTracker.assertQueried(sCluster, 1, 2, 0);
      queryTracker.assertQueried(sCluster, 2, 1, 0);
      queryTracker.assertQueried(sCluster, 2, 2, 0);
      queryTracker.assertQueried(sCluster, 3, 1, 10);
      queryTracker.assertQueried(sCluster, 3, 2, 10);
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that when {@link RackAwareRoundRobinPolicy} is wrapped with a {@link HostFilterPolicy}
   * that white lists data centers that only nodes in those data centers are queried.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_send_requests_to_whitelisted_dcs_using_host_filter_policy() {
    // given: a 6 node 3 DC cluster with a RackAwareRoundRobinPolicy that is whitelisting hosts in
    // DC1
    // and DC2.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2, 2).build();
    @SuppressWarnings("deprecation")
    LoadBalancingPolicy loadBalancingPolicy =
        HostFilterPolicy.fromDCWhiteList(
            RackAwareRoundRobinPolicy.builder().withUsedHostsPerRemoteDc(2).build(),
            Lists.newArrayList(datacenter(1), datacenter(2)));
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .build();

    try {
      sCluster.init();

      Session session = cluster.connect();

      // when: A query is made and nodes for the local dc are available.
      queryTracker.query(session, 20);

      // then: only nodes in the local DC should have been queried.
      queryTracker.assertQueried(sCluster, 1, 1, 10);
      queryTracker.assertQueried(sCluster, 1, 2, 10);
      queryTracker.assertQueried(sCluster, 2, 1, 0);
      queryTracker.assertQueried(sCluster, 2, 2, 0);
      queryTracker.assertQueried(sCluster, 3, 1, 0);
      queryTracker.assertQueried(sCluster, 3, 1, 0);

      // when: A query is made and all nodes in the local dc are down.
      sCluster.stopDC(cluster, 1);
      assertThat(cluster).controlHost().isNotNull();
      queryTracker.reset();
      queryTracker.query(session, 20);

      // then: Only nodes in DC2 should have been queried, since DC3 is not in the whitelist and DC1
      // is down.
      queryTracker.assertQueried(sCluster, 1, 1, 0);
      queryTracker.assertQueried(sCluster, 1, 2, 0);
      queryTracker.assertQueried(sCluster, 2, 1, 10);
      queryTracker.assertQueried(sCluster, 2, 2, 10);
      queryTracker.assertQueried(sCluster, 3, 1, 0);
      queryTracker.assertQueried(sCluster, 3, 1, 0);
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will determine it's local DC based on the data
   * center of the contact point(s).
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_use_local_dc_from_contact_points_when_not_explicitly_specified() {
    // given: a 4 node 2 DC cluster without a local DC specified.
    RackAwareRoundRobinPolicy policy = spy(RackAwareRoundRobinPolicy.builder().build());
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2).build();
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host1 = findHost(cluster, 1);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only the host given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsExactly(host1);
      // then: the local dc should match the contact points' datacenter.
      assertThat(policy.localDc).isEqualTo(host1.getDatacenter());
      // then: should not indicate that contact points don't match the local datacenter.
      assertThat(logs.get()).doesNotContain("Some contact points don't match local datacenter");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will determine it's local DC based on the data
   * center of the contact point(s) and if contact points in different DCs are detected that a log
   * message is generated indicating some contact points don't match the local data center.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_warn_if_contact_points_have_different_dcs_when_not_explicitly_specified() {
    // given: a 4 node 2 DC cluster with a Cluster instance with contact points in different DCs
    // and no contact point specified.
    RackAwareRoundRobinPolicy policy = spy(RackAwareRoundRobinPolicy.builder().build());
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2).build();
    Cluster cluster =
        builder()
            .addContactPoints(
                sCluster.address(1, 1).getAddress(), sCluster.address(2, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host1 = findHost(cluster, 1);
      Host host3 = findHost(cluster, 3);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only two hosts given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsOnly(host1, host3);
      // then: should indicate that some contact points don't match the local datacenter.
      assertThat(logs.get()).contains("Some contact points don't match local data center");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will not log a warning if all contact points
   * match the data center provided in {@link RackAwareRoundRobinPolicy.Builder#withLocalDc(String)}
   * and that the explicitly provided local data center is used.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_use_provided_local_dc_and_not_warn_if_contact_points_match() {
    // given: a 4 node 2 DC cluster with a Cluster instance with contact points in different DCs
    // and a local DC that doesn't match any contact points.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2).build();
    RackAwareRoundRobinPolicy policy =
        spy(RackAwareRoundRobinPolicy.builder().withLocalDc(datacenter(1)).build());
    Cluster cluster =
        builder()
            .addContactPoints(sCluster.address(1, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host1 = findHost(cluster, 1);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only two hosts given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsOnly(host1);
      // then: the data center should appropriately be set to the one specified.
      assertThat(policy.localDc).isEqualTo(host1.getDatacenter());
      // then: should not indicate that contact points don't match the local datacenter.
      assertThat(logs.get()).doesNotContain("Some contact points don't match local data center");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that {@link RackAwareRoundRobinPolicy} will log a warning if some contact points don't
   * match the data center provided in {@link RackAwareRoundRobinPolicy.Builder#withLocalDc(String)}
   * and that the explicitly provided local data center is used.
   *
   * @test_category load_balancing:rack_aware
   */
  @Test(groups = "short")
  public void should_use_provided_local_dc_and_warn_if_contact_points_dont_match() {
    // given: a 4 node 2 DC cluster with a Cluster instance with contact points in different DCs
    // and a local DC that doesn't match any contact points.
    ScassandraCluster sCluster = ScassandraCluster.builder().withNodes(2, 2).build();
    RackAwareRoundRobinPolicy policy =
        spy(RackAwareRoundRobinPolicy.builder().withLocalDc(datacenter(3)).build());
    Cluster cluster =
        builder()
            .addContactPoints(
                sCluster.address(1, 1).getAddress(), sCluster.address(2, 1).getAddress())
            .withPort(sCluster.getBinaryPort())
            .withLoadBalancingPolicy(policy)
            .build();

    try {
      sCluster.init();

      Host host1 = findHost(cluster, 1);
      Host host3 = findHost(cluster, 3);

      // when: the cluster is initialized.
      cluster.init();

      // then: should have been initialized with only two hosts given as the contact point.
      Mockito.verify(policy).init(any(Cluster.class), initHostsCaptor.capture());
      assertThat(initHostsCaptor.getValue()).containsOnly(host1, host3);
      // then: the data center should appropriately be set to the one specified.
      assertThat(policy.localDc).isEqualTo(datacenter(3));
      // then: should indicate that some contact points don't match the local datacenter.
      assertThat(logs.get()).contains("Some contact points don't match local data center");
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }
}
