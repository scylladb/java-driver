package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class DnsEndpointTests {

  private static final Logger logger = LoggerFactory.getLogger(DnsEndpointTests.class);

  @Test(groups = "long")
  public void replace_cluster_test() {
    // Configure host resolution
    MappedHostResolverProvider.addResolverEntry("control.reconnect.test", "127.1.1.1");

    Cluster cluster = null;
    Session session = null;
    CCMBridge bridgeA = null;
    try {
      bridgeA =
          CCMBridge.builder()
              .withNodes(1)
              .withIpPrefix("127.1.1.")
              .withBinaryPort(9042)
              .withClusterName("same_name")
              .build();
      bridgeA.start();

      cluster =
          Cluster.builder()
              .addContactPointsWithPorts(
                  InetSocketAddress.createUnresolved("control.reconnect.test", 9042))
              .withPort(9042)
              .withoutAdvancedShardAwareness()
              .withQueryOptions(new QueryOptions().setAddOriginalContactsToReconnectionPlan(true))
              .build();
      session = cluster.connect();

      ResultSet rs = session.execute("select * from system.local");
      Row row = rs.one();
      String address = row.getInet("broadcast_address").toString();
      logger.info("Queried node has broadcast_address: {}}", address);
      System.out.flush();
    } finally {
      assert bridgeA != null;
      bridgeA.close();
    }

    CCMBridge bridgeB = null;
    // Overwrite host resolution
    MappedHostResolverProvider.removeResolverEntry("control.reconnect.test");
    MappedHostResolverProvider.addResolverEntry("control.reconnect.test", "127.2.2.1");
    try {
      bridgeB =
          CCMBridge.builder()
              .withNodes(1)
              .withIpPrefix("127.2.2.")
              .withBinaryPort(9042)
              .withClusterName("same_name")
              .build();
      bridgeB.start();
      Thread.sleep(1000 * 92);
      ResultSet rs = session.execute("select * from system.local");
      Row row = rs.one();
      String address = row.getInet("broadcast_address").toString();
      logger.info("Queried node has broadcast_address: {}}", address);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      assert bridgeB != null;
      bridgeB.close();
    }
  }

  @Test(groups = "long")
  public void should_connect_with_mocked_hostname() {
    MappedHostResolverProvider.addResolverEntry("mocked.hostname.test", "127.0.1.1");
    try (CCMBridge ccmBridge =
            CCMBridge.builder().withNodes(1).withIpPrefix("127.0.1.").withBinaryPort(9042).build();
        Cluster cluster =
            Cluster.builder()
                .addContactPointsWithPorts(
                    InetSocketAddress.createUnresolved("mocked.hostname.test", 9042))
                .withPort(9042)
                .withoutAdvancedShardAwareness()
                .build()) {
      ccmBridge.start();
      Session session = cluster.connect();
      ResultSet rs = session.execute("SELECT * FROM system.local");
      List<Row> rows = rs.all();
      assertThat(rows).hasSize(1);
      Row row = rows.get(0);
      assertThat(row.getInet("broadcast_address").toString()).contains("127.0.1.1");
      assertTrue(
          session.getCluster().getMetadata().getAllHosts().stream()
              .map(Host::toString)
              .anyMatch(hostString -> hostString.contains("mocked.hostname.test")));
    }
  }
}
