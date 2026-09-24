/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2025 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.clientroutes;

import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simulates a Network Load Balancer (NLB) for PrivateLink testing.
 *
 * <p>Provides:
 *
 * <ul>
 *   <li>A <b>discovery port</b> that round-robins across all registered nodes (simulates the
 *       cluster-level NLB endpoint).
 *   <li>A <b>per-node port</b> for each registered node (simulates per-node NLB routing driven by
 *       system.client_routes).
 * </ul>
 *
 * <p>Nodes can be added and removed dynamically, mirroring real NLB reconfiguration during cluster
 * topology changes.
 */
public class NlbSimulator implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(NlbSimulator.class);

  private final CcmBridge ccmBridge;
  private final String bindAddress;
  private final int basePort;

  // Discovery port proxy: round-robins across all nodes
  private volatile RoundRobinProxy discoveryProxy;

  // Per-node proxies: nodeId -> TcpProxy
  private final Map<Integer, TcpProxy> nodeProxies =
      Collections.synchronizedMap(new LinkedHashMap<>());
  private final CopyOnWriteArrayList<Integer> activeNodes = new CopyOnWriteArrayList<>();

  /**
   * Creates an NLB simulator.
   *
   * @param ccmBridge the CCM bridge to get node addresses from
   * @param bindAddress the IP address to bind proxy listeners on (e.g. "127.254.254.254")
   * @param basePort the base port for NLB (discovery on basePort, per-node on basePort+nodeId)
   */
  public NlbSimulator(CcmBridge ccmBridge, String bindAddress, int basePort) {
    this.ccmBridge = ccmBridge;
    this.bindAddress = bindAddress;
    this.basePort = basePort;
  }

  /** Returns the bind address used by this NLB simulator. */
  public String getBindAddress() {
    return bindAddress;
  }

  /** Returns the discovery port (the "cluster endpoint" that round-robins to all nodes). */
  public int getDiscoveryPort() {
    return basePort;
  }

  /** Returns the per-node proxy port for a given CCM node ID. */
  public int getNodePort(int nodeId) {
    return basePort + nodeId;
  }

  /** Returns the list of currently active node IDs. */
  public List<Integer> getActiveNodes() {
    return new ArrayList<>(activeNodes);
  }

  /**
   * Adds a node to the NLB. Creates a per-node proxy and adds the node to the discovery round-robin
   * pool.
   */
  public synchronized void addNode(int nodeId) throws IOException {
    if (activeNodes.contains(nodeId)) {
      LOG.warn("Node {} already registered in NLB", nodeId);
      return;
    }

    String nodeIp = ccmBridge.getNodeIpAddress(nodeId);
    InetSocketAddress nodeAddr = new InetSocketAddress(nodeIp, 9042);

    int nodePort = getNodePort(nodeId);
    TcpProxy proxy = new TcpProxy(bindAddress, nodePort, nodeAddr);

    activeNodes.add(nodeId);
    nodeProxies.put(nodeId, proxy);

    if (discoveryProxy == null) {
      discoveryProxy = buildDiscoveryProxy(nodeAddr);
    } else {
      discoveryProxy.addTarget(nodeAddr);
    }

    LOG.info("NLB: added node{} ({}:{}) -> proxy port {}", nodeId, nodeIp, 9042, nodePort);
  }

  /** Removes a node from the NLB. Closes its per-node proxy and removes it from discovery. */
  public synchronized void removeNode(int nodeId) throws IOException {
    TcpProxy proxy = nodeProxies.remove(nodeId);
    if (proxy != null) {
      proxy.close();
    }
    activeNodes.remove(Integer.valueOf(nodeId));

    if (discoveryProxy != null) {
      String nodeIp = ccmBridge.getNodeIpAddress(nodeId);
      discoveryProxy.removeTarget(new InetSocketAddress(nodeIp, 9042));
      if (activeNodes.isEmpty()) {
        discoveryProxy.close();
        discoveryProxy = null;
      }
    }

    LOG.info("NLB: removed node{}", nodeId);
  }

  private RoundRobinProxy buildDiscoveryProxy(InetSocketAddress firstTarget) throws IOException {
    List<InetSocketAddress> targets = new ArrayList<>();
    targets.add(firstTarget);
    RoundRobinProxy proxy = new RoundRobinProxy(bindAddress, basePort, targets);
    LOG.info("NLB: discovery proxy on port {} -> {}", basePort, firstTarget);
    return proxy;
  }

  @Override
  public synchronized void close() {
    if (discoveryProxy != null) {
      discoveryProxy.close();
      discoveryProxy = null;
    }
    for (TcpProxy proxy : nodeProxies.values()) {
      proxy.close();
    }
    nodeProxies.clear();
    activeNodes.clear();
    LOG.info("NLB simulator closed");
  }
}
