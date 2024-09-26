package com.datastax.oss.driver.internal.core.loadbalancing.nodeset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.Test;

public class LazyCopyQueryPlanTest {
  @Test
  public void toString_returns_proper_results_after_poll() {
    Queue<Node> original = new ConcurrentLinkedQueue<Node>();
    original.add(mockNode("dc1"));
    original.add(mockNode("dc2"));
    Queue<Node> lc = new LazyCopyQueryPlan(original);
    assertThat(lc.toString())
        .isEqualTo(
            "java.util.concurrent.ConcurrentLinkedQueue(inQueue: ["
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc1, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0), "
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc2, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0)"
                + "], itemsPulled: [])");
    lc.poll();
    assertThat(lc.toString())
        .isEqualTo(
            "java.util.concurrent.ConcurrentLinkedQueue(inQueue: ["
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc2, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0)"
                + "], itemsPulled: ["
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc1, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0)"
                + "])");
    lc.poll();
    assertThat(lc.toString())
        .isEqualTo(
            "java.util.concurrent.ConcurrentLinkedQueue(inQueue: [], itemsPulled: ["
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc1, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0), "
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc2, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0)])");

    // Make sure that when queue is exhausted next poll does not break toString results
    lc.poll();
    assertThat(lc.toString())
        .isEqualTo(
            "java.util.concurrent.ConcurrentLinkedQueue(inQueue: [], itemsPulled: ["
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc1, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0), "
                + "DefaultNodeInfo(hostId: null, endPoint: null, datacenter: dc2, rack: null, distance: null, schemaVersion: null, broadcastRpcAddress: null, broadcastAddress: null, listenAddress: null, partitioner: null, isReconnecting: false, openConnections: 0)])");
  }

  private Node mockNode(String dc) {
    Node node = mock(Node.class);
    when(node.getDatacenter()).thenReturn(dc);
    return node;
  }
}
