/*
 * Copyright (C) 2022 ScyllaDB
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.utils.ScyllaOnly;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.testng.annotations.Test;

@CreateCCM(CreateCCM.TestMode.PER_METHOD)
@ScyllaOnly
public class ScyllaSniProxyTest extends CCMTestsSupport {

  private void test_ccm_cluster(int testNodes) {
    ccm().setKeepLogs(true);
    Cluster c = cluster().init();
    Session s = c.connect();
    TestUtils.waitForUp(TestUtils.ipOfNode(1), c);

    Collection<Host> hosts = s.getState().getConnectedHosts();
    assertThat(hosts.size()).isEqualTo(testNodes);
    Set<UUID> uuidSet = new TreeSet<UUID>();
    for (Host host : hosts) {
      uuidSet.add(host.getHostId());
      assertThat(host.getEndPoint().resolve().getAddress()).isEqualTo(TestUtils.addressOfNode(1));
      assertThat(host.getEndPoint().resolve().getPort()).isEqualTo(ccm().getSniPort());
      assertThat(host.getEndPoint().toString()).doesNotContain("any.");
    }
    assertThat(uuidSet.size()).isEqualTo(testNodes);
    ((SessionManager) s).cluster.manager.controlConnection.triggerReconnect();

    SchemaChangeListener listener = mock(SchemaChangeListenerBase.class);
    final ArrayList<String> keyspaceMismatches = new ArrayList<String>();
    final ArrayList<String> tableMismatches = new ArrayList<String>();

    SchemaChangeListener assertingListener =
        new SchemaChangeListenerBase() {
          @Override
          public void onKeyspaceAdded(KeyspaceMetadata keyspace) {
            if (!keyspace.getName().equals("testks")) {
              keyspaceMismatches.add(keyspace.getName());
            }
          }

          @Override
          public void onTableAdded(TableMetadata table) {
            if (!table.getName().equals("testtab")) {
              tableMismatches.add(table.getName());
            }
          }
        };

    c.register(listener);
    c.register(assertingListener);

    s.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, "testks", testNodes));
    s.execute("CREATE TABLE testks.testtab (a int PRIMARY KEY, b int);");

    // Sometimes (probably due to reconnection) both events can be read twice
    // assertingListener ensures we deal with the same keyspace and table
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ignored) {
    }
    verify(listener, atLeast(1)).onTableAdded(any(TableMetadata.class));
    verify(listener, atMost(2)).onTableAdded(any(TableMetadata.class));
    verify(listener, atLeast(1)).onKeyspaceAdded(any(KeyspaceMetadata.class));
    verify(listener, atMost(2)).onKeyspaceAdded(any(KeyspaceMetadata.class));

    if (!keyspaceMismatches.isEmpty()) {
      StringBuilder ksNames = new StringBuilder();
      for (String str : keyspaceMismatches) {
        ksNames.append(", " + ksNames + str);
      }
      throw new RuntimeException(
          "assertingListener registered keyspace added event with keyspaces named: ["
              + ksNames.substring(2)
              + "] which is not \"testks\"");
    }
    if (!tableMismatches.isEmpty()) {
      StringBuilder tabNames = new StringBuilder();
      for (String str : tableMismatches) {
        tabNames.append(", " + tabNames + str);
      }
      throw new RuntimeException(
          "assertingListener registered table added event with tables named: ["
              + tabNames.substring(2)
              + "] which is not \"testtab\"");
    }

    s.close();
    c.close();
  }

  @Test(groups = "short")
  @CCMConfig(
      startSniProxy = true,
      numberOfNodes = 3,
      clusterProvider = "createClusterBuilderScyllaCloud",
      dirtiesContext = true)
  public void test_ccm_cluster_3node() throws InterruptedException {
    test_ccm_cluster(3);
  }

  @Test(groups = "short")
  @CCMConfig(
      startSniProxy = true,
      numberOfNodes = 1,
      clusterProvider = "createClusterBuilderScyllaCloud",
      dirtiesContext = true)
  public void test_ccm_cluster_1node() throws InterruptedException {
    test_ccm_cluster(1);
  }
}
