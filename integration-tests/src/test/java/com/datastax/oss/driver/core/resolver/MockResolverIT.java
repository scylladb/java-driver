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
import static org.junit.Assert.fail;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
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
   * A loopback address no node is ever started on, outside the {@code 127.0.1.} prefix CCM hands
   * its nodes: a connection attempt that dials it is refused immediately.
   */
  private static final String DEAD_ADDRESS = "127.0.0.11";

  private static final ch.qos.logback.classic.Logger CHANNEL_FACTORY_LOGGER =
      (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ChannelFactory.class);

  private ListAppender<ILoggingEvent> channelFactoryAppender;
  private Level originalChannelFactoryLevel;

  @Before
  public void startCapturingChannelFactoryLogs() {
    // ChannelFactory reports a candidate it gave up on at DEBUG, which is the only externally
    // visible evidence that the multi-address fallback did any work.
    originalChannelFactoryLevel = CHANNEL_FACTORY_LOGGER.getLevel();
    CHANNEL_FACTORY_LOGGER.setLevel(Level.DEBUG);
    channelFactoryAppender = new ListAppender<>();
    // ChannelFactory logs from every I/O thread of the session under test, and the assertions read
    // the list while those threads are still winding down; ListAppender's own ArrayList is not safe
    // for that (appends are unsynchronized, and iterating one would throw).
    channelFactoryAppender.list = new CopyOnWriteArrayList<>();
    channelFactoryAppender.start();
    CHANNEL_FACTORY_LOGGER.addAppender(channelFactoryAppender);
  }

  @After
  public void stopCapturingChannelFactoryLogs() {
    CHANNEL_FACTORY_LOGGER.detachAppender(channelFactoryAppender);
    channelFactoryAppender.stop();
    CHANNEL_FACTORY_LOGGER.setLevel(originalChannelFactoryLevel);
  }

  /** The formatted messages {@code ChannelFactory} logged during the current test. */
  private List<String> channelFactoryLogMessages() {
    return channelFactoryAppender.list.stream()
        .map(ILoggingEvent::getFormattedMessage)
        .collect(Collectors.toList());
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
        // Selected by the address the connection reached, not by the contact-point name: the node
        // is
        // identified by its own address (see below), so the name no longer appears in toString().
        String nodeIp = ccmBridge.getNodeIpAddress(1);
        Set<Node> filteredNodes =
            nodes.stream()
                .filter(
                    x -> {
                      InetSocketAddress resolved = (InetSocketAddress) x.getEndPoint().resolve();
                      return !resolved.isUnresolved()
                          && nodeIp.equals(resolved.getAddress().getHostAddress());
                    })
                .collect(Collectors.toSet());
        assertThat(filteredNodes).hasSize(1);
        Node node = filteredNodes.iterator().next();
        InetSocketAddress address = (InetSocketAddress) node.getEndPoint().resolve();
        // ChannelFactory pins the control connection's endpoint to the address it actually reached,
        // and DefaultTopologyMonitor#buildNodeEndPoint keeps that address for the control node, so
        // resolution yields that concrete IP rather than the hostname.
        assertFalse(address.isUnresolved());
        assertThat(address.getAddress().getHostAddress()).isEqualTo(nodeIp);
        // ... while still carrying the configured name as its label, so the TLS peer host and the
        // Kerberos service name are what the operator wrote, with no reverse lookup.
        assertThat(address.getHostString()).isEqualTo("test.cluster.fake");
        assertThat(address.getAddress().getHostName()).isEqualTo("test.cluster.fake");
        // The node's *identity*, though, is its own address rather than the contact point's. The
        // reconnection fallback re-offers the contact points every round, so a name-derived prefix
        // would be acquired by each successive control node in turn, and two live nodes sharing one
        // prefix means the older one's clearMetrics() deletes the newcomer's series.
        assertThat(node.getEndPoint().asMetricPrefix())
            .isEqualTo(nodeIp.replace('.', '_') + ":9042");
      }
    }
  }

  @Test
  public void should_connect_when_first_dns_entry_is_non_responsive() {
    final int numberOfNodes = 2;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
            .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), true)
            .withStringList(
                TypedDriverOption.CONTACT_POINTS.getRawOption(),
                Collections.singletonList("test.cluster.fake:9042"))
            .build();

    CqlSessionBuilder builder = new CqlSessionBuilder().withConfigLoader(loader);
    try (CcmBridge ccmBridge =
        CcmBridge.builder().withNodes(numberOfNodes).withIpPrefix("127.0.1.").build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      // Nothing is ever started on DEAD_ADDRESS, so it is the dead record.
      MultimapHostResolverProvider.addResolverEntry("test.cluster.fake", DEAD_ADDRESS);
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      ccmBridge.create();
      ccmBridge.start();

      // ChannelFactory shuffles a name's expanded addresses per connection attempt, so whether the
      // dead record is dialed first is a coin toss (about 1 in 3 here). Every session must connect
      // either way; the loop hunts for an iteration that demonstrably dialed the dead record first
      // and fell through to a live one, which is the behavior this test exists to pin down.
      // Twenty misses in a row would have probability (2/3)^20, about 0.03%.
      boolean sawFallback = false;
      for (int attempt = 0; attempt < 20 && !sawFallback; attempt++) {
        try (CqlSession session = builder.build()) {
          waitForAllNodesUp(session, numberOfNodes);
          ResultSet rs = session.execute("select * from system.local where key='local'");
          assertThat(rs).isNotNull();
          List<Row> rows = rs.all();
          assertThat(rows).hasSize(1);
          Collection<Node> nodes = session.getMetadata().getNodes().values();
          assertThat(nodes).hasSize(numberOfNodes);
        }
        sawFallback =
            channelFactoryLogMessages().stream()
                .anyMatch(
                    message ->
                        message.contains(DEAD_ADDRESS) && message.contains("trying next address"));
      }

      // The connections above only survived a dead-first ordering because the candidate loop moved
      // past the dead record.
      assertThat(sawFallback)
          .as(
              "expected at least one connection attempt to dial %s first and fall through",
              DEAD_ADDRESS)
          .isTrue();
    }
  }

  @Test
  public void replace_cluster_test() {
    final int numberOfNodes = 3;
    DriverConfigLoader loader =
        new DefaultProgrammaticDriverConfigLoaderBuilder()
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
      // Exactly one node -- the one the control connection came up on -- still names the contact
      // point. It is the endpoint's *label* rather than its identity: the node is identified by the
      // address the connection reached (see should_connect_with_mocked_hostname), while the label
      // is
      // what TLS and Kerberos read. Re-resolution itself no longer depends on any node holding the
      // name: MetadataManager retains the contact points, and the control-connection reconnection
      // fallback re-offers them.
      assertThat(nodesNamingTheContactPoint(nodes)).hasSize(1);
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
      Set<Node> filteredNodes = nodesNamingTheContactPoint(nodes);
      if (filteredNodes.isEmpty()) {
        LOG.error(
            "No metadata node whose endpoint is labelled \"test.cluster.fake\". The label was likely "
                + "dropped when the endpoint was rebuilt.");
      } else if (filteredNodes.size() > 1) {
        fail(
            "Somehow there is more than 1 node in metadata labelled with the contact point. This should not ever happen.");
      }
    }
    session.close();
  }

  /**
   * The nodes whose endpoint carries the contact-point name as its host string.
   *
   * <p>Deliberately not {@code toString().contains(...)}: a node is identified by the address the
   * connection reached, so the name lives on as the endpoint's <i>label</i> -- what the SSL engine
   * and the Kerberos service name read -- rather than in its identity.
   */
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
      filteredNodes = nodesNamingTheContactPoint(nodes);
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
        filteredNodes = nodesNamingTheContactPoint(nodes);
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
