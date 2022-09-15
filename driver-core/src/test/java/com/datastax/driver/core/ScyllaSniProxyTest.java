/*
 * Copyright (C) 2022 ScyllaDB
 */
package com.datastax.driver.core;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import org.testng.annotations.Test;

@CreateCCM(CreateCCM.TestMode.PER_METHOD)
public class ScyllaSniProxyTest extends CCMTestsSupport {

  private void test_ccm_cluster(int testNodes) {
    Cluster c = cluster().init();
    Session s = c.connect();
    TestUtils.waitForUp(TestUtils.ipOfNode(1), c);

    Collection<Host> hosts = s.getState().getConnectedHosts();
    assert hosts.size() == testNodes;
    for (Host host : hosts) {
      assert (host.getListenAddress() == null
          || host.getListenAddress().equals(TestUtils.addressOfNode(1)));
      assert host.getEndPoint().resolve().getAddress().equals(TestUtils.addressOfNode(1));
      assert host.getEndPoint().resolve().getPort() == ccm().getSniPort();
      assert !host.getEndPoint().toString().contains("any.");
    }
    ((SessionManager) s).cluster.manager.controlConnection.triggerReconnect();

    SchemaChangeListener listener = mock(SchemaChangeListenerBase.class);
    c.register(listener);

    s.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, "testks", testNodes));
    s.execute("CREATE TABLE testks.testtab (a int PRIMARY KEY, b int);");

    verify(listener, times(1)).onTableAdded(any(TableMetadata.class));
    verify(listener, atLeast(1)).onKeyspaceAdded(any(KeyspaceMetadata.class));
    verify(listener, atMost(2)).onKeyspaceAdded(any(KeyspaceMetadata.class));

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
