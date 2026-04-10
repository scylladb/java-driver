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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(IsolatedTests.class)
public class MockResolverIT {

  private static final Logger LOG = LoggerFactory.getLogger(MockResolverIT.class);

  private static final int CLUSTER_WAIT_SECONDS =
      20; // Maximal wait time for cluster nodes to get up

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

  @SuppressWarnings("unused")
  public void run_replace_test_20_times() {
    for (int i = 1; i <= 20; i++) {
      LOG.info(
          "Running ({}/20}) {}", i, MockResolverIT.class.toString() + "#replace_cluster_test()");
      replace_cluster_test();
    }
  }

  /**
   * Verifies that the driver can connect to a cluster when the first DNS entry for the contact
   * point hostname resolves to a non-responsive IP address (DRIVER-201).
   *
   * <p>With {@code RESOLVE_CONTACT_POINTS=false} (the default), the hostname is kept unresolved
   * until connection time. Previously, only the first IP returned by DNS was tried, causing an
   * {@code AllNodesFailedException} when that IP was unreachable. The fix expands the hostname to
   * all IPs at connection time and tries each one in sequence.
   */
  @Test
  public void should_connect_when_first_dns_entry_is_non_responsive() {
    // Use a 2-node cluster on 127.0.1.x.  Node 11 (127.0.1.11) does not exist and is therefore
    // non-responsive; nodes 1 and 2 are real.
    try (CcmBridge ccmBridge = CcmBridge.builder().withNodes(2).withIpPrefix("127.0.1.").build()) {
      MultimapHostResolverProvider.removeResolverEntries("test.cluster.fake");
      // First entry intentionally points to a non-existent/non-responsive address.
      MultimapHostResolverProvider.addResolverEntry("test.cluster.fake", "127.0.1.11");
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(1));
      MultimapHostResolverProvider.addResolverEntry(
          "test.cluster.fake", ccmBridge.getNodeIpAddress(2));
      ccmBridge.create();
      ccmBridge.start();

      DriverConfigLoader loader =
          new DefaultProgrammaticDriverConfigLoaderBuilder()
              .withBoolean(TypedDriverOption.RESOLVE_CONTACT_POINTS.getRawOption(), false)
              .withBoolean(TypedDriverOption.RECONNECT_ON_INIT.getRawOption(), false)
              .withStringList(
                  TypedDriverOption.CONTACT_POINTS.getRawOption(),
                  Collections.singletonList("test.cluster.fake:9042"))
              .build();

      // The session must open successfully despite the first DNS entry being unreachable.
      try (CqlSession session = new CqlSessionBuilder().withConfigLoader(loader).build()) {
        ResultSet rs = session.execute("select * from system.local where key='local'");
        assertThat(rs.one()).isNotNull();
        waitForAllNodesUp(session, 2);
      }
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
