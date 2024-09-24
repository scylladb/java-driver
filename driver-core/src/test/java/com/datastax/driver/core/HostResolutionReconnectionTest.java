package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import org.burningwave.tools.net.DefaultHostResolver;
import org.burningwave.tools.net.HostResolutionRequestInterceptor;
import org.burningwave.tools.net.MappedHostResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class HostResolutionReconnectionTest {

  private static final Logger logger =
      LoggerFactory.getLogger(HostResolutionReconnectionTest.class);

  @Test(groups = "isolated")
  public void should_reconnect_to_different_cluster() {
    // Configure host resolution
    Map<String, String> hostAliasesA = new LinkedHashMap<>();
    hostAliasesA.put("control.reconnect.test", "127.1.1.1");
    HostResolutionRequestInterceptor.INSTANCE.install(
        new MappedHostResolver(hostAliasesA), DefaultHostResolver.INSTANCE);

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
    Map<String, String> hostAliasesB = new LinkedHashMap<>();
    hostAliasesB.put("control.reconnect.test", "127.2.2.1");
    HostResolutionRequestInterceptor.INSTANCE.install(
        new MappedHostResolver(hostAliasesB), DefaultHostResolver.INSTANCE);
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
}
