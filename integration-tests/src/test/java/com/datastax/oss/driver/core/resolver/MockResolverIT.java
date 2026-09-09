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
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
public class MockResolverIT {

  private static final Logger LOG = LoggerFactory.getLogger(MockResolverIT.class);

  private static final int CLUSTER_WAIT_SECONDS =
      20; // Maximal wait time for cluster nodes to get up

  /**
   * Generous: a lookup racing the re-point can re-cache the dead addresses for one {@code
   * networkaddress.cache.ttl}, and each round first fails on the previous cluster's nodes.
   */
  private static final int RECOVERY_WAIT_SECONDS = 120;

  /** Bounds the driver noticing only: {@code decommission} already blocked on nodetool. */
  private static final int DECOMMISSION_WAIT_SECONDS = 120;

  /**
   * Resolver entries and the JVM's cache of them are process-global and shared by every test here,
   * so a name one test re-points must not be served to the next.
   */
  @Before
  @After
  public void clearResolverState() {
    MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
    MultimapHostResolverProvider.clearJvmCache();
  }

  private static void waitForAllNodesUp(CqlSession session, int expectedNodes) {
    Awaitility.await()
        .atMost(CLUSTER_WAIT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              Collection<Node> nodes = session.getMetadata().getNodes().values();
              long upCount = nodes.stream().filter(n -> n.getUpSinceMillis() > 0).count();
              return upCount == expectedNodes;
            });
  }

  @Test
  public void should_connect_with_mocked_hostname() {
    CcmBridge.Builder ccmBridgeBuilder = CcmBridge.builder().withNodes(1);
    try (CcmBridge ccmBridge = ccmBridgeBuilder.build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      ccmBridge.create();
      ccmBridge.start();

      DriverConfigLoader loader =
          new DefaultProgrammaticDriverConfigLoaderBuilder()
              .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
              .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
              .withStringList(
                  TypedDriverOption.CONTACT_POINTS.getRawOption(),
                  Collections.singletonList("test.cluster.fake:9042"))
              .build();

      CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
      try (CqlSession session = builder.build()) {
        ResultSet rs = session.execute("select * from system.local where key='local'");
        List<Row> rows = rs.all();
        assertThat(rows).hasSize(1);
        LOG.trace("system.local contents: {}", rows.get(0).getFormattedContents());
        Collection<Node> nodes = session.getMetadata().getNodes().values();
        for (Node node : nodes) {
          LOG.trace("Found metadata node: {}", node);
        }
        Set<Node> filteredNodes;
        filteredNodes =
            nodes.stream()
                .filter(x -> x.toString().contains("test.cluster.fake"))
                .collect(Collectors.toSet());
        assertThat(filteredNodes).hasSize(1);
        InetSocketAddress address =
            (InetSocketAddress) filteredNodes.iterator().next().getEndPoint().resolve();
        assertTrue(address.isUnresolved());
      }
    }
  }

  @Test
  public void replace_cluster_test() {
    final int numberOfNodes = 3;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;

    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      waitForAllNodesUp(session, numberOfNodes);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();
      Collection<Node> nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(numberOfNodes);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      Set<Node> filteredNodes;
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      ccmBridge.create();
      ccmBridge.start();
      waitForAllNodesUp(session, numberOfNodes);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();

      Collection<Node> nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(numberOfNodes);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      Set<Node> filteredNodes;
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      if (filteredNodes.size() == 0) {
        LOG.error(
            "No metadata node with \"test.cluster.fake\" substring. The unresolved endpoint socket was likely "
                + "replaced with resolved one.");
      } else if (filteredNodes.size() > 1) {
        fail(
            "Somehow there is more than 1 node in metadata with unresolved hostname. This should not ever happen.");
      }
    }
    session.close();
  }

  @Test
  public void should_recover_when_the_cluster_moves_to_new_addresses() {
    // replace_cluster_test brings the cluster back on the same addresses, so only the contact-point
    // fallback can show a session finding one that came back on *different* ones.
    // Four nodes, not three: Cassandra 4 refuses to decommission below system_distributed's
    // replication factor of 3. RemovedNodeIT sizes its cluster the same way.
    final int initialNodes = 4;
    final int movedNodes = 3;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            // Pinned like every other test here: the fallback only re-resolves a contact point
            // kept unresolved. Leaving the fallback's own default alone is deliberate.
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withDuration(
                TypedDriverOption.RECONNECTION_BASE_DELAY.getRawOption(), Duration.ofSeconds(1))
            .withDuration(
                TypedDriverOption.RECONNECTION_MAX_DELAY.getRawOption(), Duration.ofSeconds(1))
            .withDuration(
                TypedDriverOption.CONNECTION_CONNECT_TIMEOUT.getRawOption(), Duration.ofSeconds(2))
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();
    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session = null;

    try {
      try (CcmBridge ccmBridge =
          CcmBridge.builder().withNodes(initialNodes).withIpPrefix("127.0.1.").build()) {
        pointContactPointAt(ccmBridge, initialNodes);
        ccmBridge.create();
        ccmBridge.start();
        session = builder.build();
        waitForAllNodesUp(session, initialNodes);
        assertThat(nodesOnPrefix(session, "127.0.1.")).hasSize(initialNodes);
        // The loader leaves the fallback at its default on purpose; assert it, or a flipped
        // default would surface as a bare Awaitility timeout below.
        assertThat(
                session
                    .getContext()
                    .getConfig()
                    .getDefaultProfile()
                    .getBoolean(
                        TypedDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS
                            .getRawOption()))
            .isTrue();

        // The node reached through the contact point keeps re-resolving that name in its pool, so
        // left in place it would find the new cluster by itself and this test would pass with the
        // fallback off. Decommission rather than stop it: a stopped node stays in the metadata.
        Node hostnameNode = theNodeNamingTheContactPoint(session);
        ccmBridge.decommission(ccmNodeIndexOf(ccmBridge, hostnameNode, initialNodes));
        awaitRemovalOfContactPointNode(session, initialNodes - 1);
      }
      // Re-point the name and drop the JVM's cached answers before the new cluster exists, or the
      // fallback is served the dead addresses for one networkaddress.cache.ttl.
      try (CcmBridge ccmBridge =
          CcmBridge.builder().withNodes(movedNodes).withIpPrefix("127.0.2.").build()) {
        pointContactPointAt(ccmBridge, movedNodes);
        MultimapHostResolverProvider.clearJvmCache();
        ccmBridge.create();
        ccmBridge.start();
        awaitAllNodesUpOnPrefix(session, "127.0.2.", movedNodes);
        // The point of the test: the session followed the name and let go of the old cluster.
        Collection<Node> nodes = session.getMetadata().getNodes().values();
        assertThat(nodesOnPrefix(session, "127.0.1.")).isEmpty();
        // The fallback's signature: the node it reached is registered under the name, and only it.
        assertThat(nodesNamingTheContactPoint(nodes)).hasSize(1);
        ResultSet rs = session.execute("select * from system.local where key='local'");
        assertThat(rs.one()).isNotNull();
      }
    } finally {
      // A failure must not leave this session reconnecting against torn-down clusters.
      if (session != null) {
        session.close();
      }
    }
  }

  private static void pointContactPointAt(CcmBridge ccmBridge, int numberOfNodes) {
    MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
    for (int i = 1; i <= numberOfNodes; i++) {
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(i));
    }
  }

  /** The one metadata node registered under the contact point's name; fails if there is not one. */
  private static Node theNodeNamingTheContactPoint(CqlSession session) {
    Set<Node> named = nodesNamingTheContactPoint(session.getMetadata().getNodes().values());
    assertThat(named).hasSize(1);
    return named.iterator().next();
  }

  /**
   * The CCM index of the node {@code node} was identified as, by its broadcast RPC address: its own
   * endpoint is the contact point's unresolved name.
   */
  private static int ccmNodeIndexOf(CcmBridge ccmBridge, Node node, int numberOfNodes) {
    InetSocketAddress rpcAddress =
        node.getBroadcastRpcAddress()
            .orElseThrow(() -> new AssertionError("No broadcast RPC address recorded for " + node));
    String ip = rpcAddress.getAddress().getHostAddress();
    for (int i = 1; i <= numberOfNodes; i++) {
      if (ip.equals(ccmBridge.getNodeIpAddress(i))) {
        return i;
      }
    }
    throw new AssertionError(node + " (rpc " + ip + ") is not one of the CCM nodes");
  }

  /**
   * Waits until the decommissioned node has left the metadata and no remaining node is registered
   * under the contact point's name.
   */
  private static void awaitRemovalOfContactPointNode(CqlSession session, int expectedNodes) {
    Awaitility.await()
        .atMost(DECOMMISSION_WAIT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              Collection<Node> nodes = session.getMetadata().getNodes().values();
              assertThat(nodes).hasSize(expectedNodes);
              assertThat(nodesNamingTheContactPoint(nodes)).isEmpty();
            });
  }

  /** Waits until every node the session knows sits on {@code ipPrefix} and is up. */
  private static void awaitAllNodesUpOnPrefix(
      CqlSession session, String ipPrefix, int numberOfNodes) {
    Awaitility.await()
        .atMost(RECOVERY_WAIT_SECONDS, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        // untilAsserted so a timeout names the condition that failed. Up-ness is counted on the
        // prefix, the total across the metadata: the old cluster leaving is part of the wait.
        .untilAsserted(
            () -> {
              Set<Node> onPrefix = nodesOnPrefix(session, ipPrefix);
              assertThat(onPrefix).hasSize(numberOfNodes);
              assertThat(upNodes(onPrefix)).hasSize(numberOfNodes);
              assertThat(session.getMetadata().getNodes()).hasSize(numberOfNodes);
            });
  }

  /**
   * The metadata nodes sitting in {@code ipPrefix}. The node reached through the contact point
   * holds the unresolved name, so its broadcast RPC address says where it is.
   */
  private static Set<Node> nodesOnPrefix(CqlSession session, String ipPrefix) {
    return session.getMetadata().getNodes().values().stream()
        .filter(
            node -> {
              String ip = hostAddressOf(node);
              return ip != null && ip.startsWith(ipPrefix);
            })
        .collect(Collectors.toSet());
  }

  private static Set<Node> upNodes(Set<Node> nodes) {
    return nodes.stream().filter(n -> n.getUpSinceMillis() > 0).collect(Collectors.toSet());
  }

  /**
   * The IP a node sits at: its endpoint's address when resolved, else its broadcast RPC address,
   * else {@code null}.
   */
  private static String hostAddressOf(Node node) {
    SocketAddress resolved = node.getEndPoint().resolve();
    if (resolved instanceof InetSocketAddress && !((InetSocketAddress) resolved).isUnresolved()) {
      return ((InetSocketAddress) resolved).getAddress().getHostAddress();
    }
    return node.getBroadcastRpcAddress()
        .map(address -> address.getAddress().getHostAddress())
        .orElse(null);
  }

  /** The nodes whose endpoint carries the contact-point name as its host string. */
  private static Set<Node> nodesNamingTheContactPoint(Collection<Node> nodes) {
    return nodes.stream()
        .filter(
            node -> {
              SocketAddress resolved = node.getEndPoint().resolve();
              return resolved instanceof InetSocketAddress
                  && "test.cluster.fake".equals(((InetSocketAddress) resolved).getHostString());
            })
        .collect(Collectors.toSet());
  }

  @SuppressWarnings("unused")
  public void run_replace_test_20_times() {
    for (int i = 1; i <= 20; i++) {
      LOG.info(
          "Running ({}/20}) {}", i, MockResolverIT.class.toString() + "#replace_cluster_test()");
      replace_cluster_test();
    }
  }

  // This is too long to run during CI, but is useful for manual investigations.
  @SuppressWarnings("unused")
  public void cannot_reconnect_with_resolved_socket() {
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    CqlSession session;
    Collection<Node> nodes;
    Set<Node> filteredNodes;
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      ccmBridge.create();
      ccmBridge.start();
      session = builder.build();
      waitForAllNodesUp(session, 3);
      ResultSet rs = session.execute("select * from system.local where key='local'");
      assertThat(rs).isNotNull();
      Row row = rs.one();
      assertThat(row).isNotNull();
      nodes = session.getMetadata().getNodes().values();
      assertThat(nodes).hasSize(3);
      Iterator<Node> iterator = nodes.iterator();
      while (iterator.hasNext()) {
        LOG.trace("Metadata node: " + iterator.next().toString());
      }
      filteredNodes =
          nodes.stream()
              .filter(x -> x.toString().contains("test.cluster.fake"))
              .collect(Collectors.toSet());
      assertThat(filteredNodes).hasSize(1);
    }
    int counter = 0;
    while (filteredNodes.size() == 1) {
      counter++;
      if (counter == 255) {
        LOG.error("Completed 254 runs. Breaking.");
        break;
      }
      LOG.warn(
          "Launching another cluster until we lose resolved socket from metadata (run {}).",
          counter);
      try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
        MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
        MultimapHostResolverProvider.addResolverEntry(
            "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
        ccmBridge.create();
        ccmBridge.start();
        waitForAllNodesUp(session, 3);
        nodes = session.getMetadata().getNodes().values();
        assertThat(nodes).hasSize(3);
        Iterator<Node> iterator = nodes.iterator();
        while (iterator.hasNext()) {
          LOG.trace("Metadata node: " + iterator.next().toString());
        }
        filteredNodes =
            nodes.stream()
                .filter(x -> x.toString().contains("test.cluster.fake"))
                .collect(Collectors.toSet());
        if (filteredNodes.size() > 1) {
          fail(
              "Somehow there is more than 1 node in metadata with unresolved hostname. This should not ever happen.");
        }
      }
    }
    Iterator<Node> iterator = nodes.iterator();
    while (iterator.hasNext()) {
      InetSocketAddress address = (InetSocketAddress) iterator.next().getEndPoint().resolve();
      assertFalse(address.isUnresolved());
    }
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(3).build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(3));
      // Now the driver should fail to reconnect since unresolved hostname is gone.
      ccmBridge.create();
      ccmBridge.start();
      waitForAllNodesUp(session, 3);
      session.execute("select * from system.local where key='local'");
    }
    session.close();
  }
}
