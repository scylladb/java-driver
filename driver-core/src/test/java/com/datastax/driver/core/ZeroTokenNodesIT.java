package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.utils.ScyllaVersion;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ZeroTokenNodesIT {

  @DataProvider(name = "loadBalancingPolicies")
  public static Object[][] loadBalancingPolicies() {
    return new Object[][] {
      {DCAwareRoundRobinPolicy.builder().build(), true},
      {new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()), true},
      {new TokenAwarePolicy(new RoundRobinPolicy()), false}
    };
  }

  @Test(groups = "short")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_not_ignore_zero_token_peer_when_option_is_enabled() {
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;
    try {
      ccmBridge =
          CCMBridge.builder().withNodes(3).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setConsiderZeroTokenNodesValidPeers(true))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(1), 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(1));
      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings)
          .containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042", "/127.0.1.3:9042", "/127.0.1.4:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_not_discover_zero_token_DC_when_option_is_disabled() {
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;
    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2, 0).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(2, 3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.add(2, 4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(3);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setConsiderZeroTokenNodesValidPeers(false))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(1), 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(1));

      // Queries should not land on any of the zero-token DC nodes
      session.execute("DROP KEYSPACE IF EXISTS ZeroTokenNodesIT");
      session.execute(
          "CREATE KEYSPACE ZeroTokenNodesIT WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 0};");
      session.execute("CREATE TABLE ZeroTokenNodesIT.tab (id INT PRIMARY KEY)");
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute("INSERT INTO ZeroTokenNodesIT.tab (id) VALUES (" + i + ")");
        assertThat(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve())
            .isNotIn(ccmBridge.addressOfNode(3), ccmBridge.addressOfNode(4));
        assertThat(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve())
            .isIn(ccmBridge.addressOfNode(1), ccmBridge.addressOfNode(2));
      }

      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings).containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short", dataProvider = "loadBalancingPolicies")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_discover_zero_token_DC_when_option_is_enabled(
      LoadBalancingPolicy loadBalancingPolicy, boolean isDcAware) {
    // Makes sure that with QueryOptions.setConsiderZeroTokenNodesValidPeers(true)
    // the zero-token peers will be discovered and added to metadata
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;
    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2, 0).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(2, 3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.add(2, 4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(3);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setConsiderZeroTokenNodesValidPeers(true))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(1), 9042))
              .withPort(9042)
              .withLoadBalancingPolicy(loadBalancingPolicy)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(1));

      // Currently zero-token nodes are treated as normal nodes if allowed.
      // Later on we may want to adjust the LBP behavior to be more sophisticated.
      session.execute("DROP KEYSPACE IF EXISTS ZeroTokenNodesIT");
      session.execute(
          "CREATE KEYSPACE ZeroTokenNodesIT WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 0};");
      session.execute("CREATE TABLE ZeroTokenNodesIT.tab (id INT PRIMARY KEY)");
      PreparedStatement ps = session.prepare("INSERT INTO ZeroTokenNodesIT.tab (id) VALUES (?)");
      Set<InetSocketAddress> queriedNodes = new HashSet<>();
      for (int i = 0; i < 30; i++) {
        ResultSet rs = session.execute(ps.bind(i).setConsistencyLevel(ConsistencyLevel.TWO));
        queriedNodes.add(rs.getExecutionInfo().getQueriedHost().getEndPoint().resolve());
      }

      if (isDcAware) {
        assertThat(queriedNodes)
            .containsExactly(ccmBridge.addressOfNode(1), ccmBridge.addressOfNode(2));
      } else {
        assertThat(queriedNodes)
            .containsExactly(
                ccmBridge.addressOfNode(1),
                ccmBridge.addressOfNode(2),
                ccmBridge.addressOfNode(3),
                ccmBridge.addressOfNode(4));
      }

      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings)
          .containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042", "/127.0.1.3:9042", "/127.0.1.4:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_connect_to_zero_token_contact_point() {
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;

    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.waitForUp(3);

      cluster =
          Cluster.builder()
              .withQueryOptions(
                  new QueryOptions()
                      // Already implicitly false, but making sure
                      .setConsiderZeroTokenNodesValidPeers(false))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(3), 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(3));

      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings).containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042", "/127.0.1.3:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }

  @Test(groups = "short")
  @ScyllaVersion(
      minOSS = "6.2.0",
      minEnterprise = "2024.2.2",
      description = "Zero-token nodes introduced in scylladb/scylladb#19684")
  public void should_connect_to_zero_token_DC() {
    // This test is similar but not exactly the same as should_connect_to_zero_token_contact_point.
    // In the future we may want to have different behavior for arbiter DCs and adjust this test
    // method.
    Cluster cluster = null;
    Session session = null;
    CCMBridge ccmBridge = null;

    try {
      ccmBridge =
          CCMBridge.builder().withNodes(2, 0).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
      ccmBridge.start();
      ccmBridge.add(2, 3);
      ccmBridge.updateNodeConfig(3, "join_ring", false);
      ccmBridge.start(3);
      ccmBridge.add(2, 4);
      ccmBridge.updateNodeConfig(4, "join_ring", false);
      ccmBridge.start(4);
      ccmBridge.waitForUp(3);
      ccmBridge.waitForUp(4);

      cluster =
          Cluster.builder()
              .withQueryOptions(new QueryOptions().setConsiderZeroTokenNodesValidPeers(false))
              .addContactPointsWithPorts(new InetSocketAddress(ccmBridge.ipOfNode(3), 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .build();
      session = cluster.connect();

      assertThat(cluster.manager.controlConnection.connectedHost().getEndPoint().resolve())
          .isEqualTo(ccmBridge.addressOfNode(3));

      Set<Host> hosts = cluster.getMetadata().getAllHosts();
      Set<String> toStrings = hosts.stream().map(Host::toString).collect(Collectors.toSet());
      assertThat(toStrings)
          // node 3 will be discovered because it's a contact point. Node 4 will not because it's a
          // peer
          // and the setting is disabled
          .containsOnly("/127.0.1.1:9042", "/127.0.1.2:9042", "/127.0.1.3:9042");
    } finally {
      if (ccmBridge != null) ccmBridge.close();
      if (session != null) session.close();
      if (cluster != null) cluster.close();
    }
  }
}
