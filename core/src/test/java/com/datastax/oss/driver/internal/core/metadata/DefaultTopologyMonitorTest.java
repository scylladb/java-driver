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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.filter;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.addresstranslation.PassThroughAddressTranslator;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.google.common.collect.Streams;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

@RunWith(DataProviderRunner.class)
public class DefaultTopologyMonitorTest {

  private static final InetSocketAddress ADDRESS2 = new InetSocketAddress("127.0.0.2", 9042);

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultConfig;
  @Mock private ControlConnection controlConnection;
  @Mock private DriverChannel channel;
  @Mock protected MetricsFactory metricsFactory;

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  @Mock private SslEngineFactory sslEngineFactory;

  private DefaultNode node1;
  private DefaultNode node2;

  private TestTopologyMonitor topologyMonitor;

  private Logger logger;
  private Level initialLogLevel;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);

    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(1));
    when(config.getDefaultProfile()).thenReturn(defaultConfig);
    when(context.getConfig()).thenReturn(config);

    AddressTranslator addressTranslator = spy(new PassThroughAddressTranslator(context));
    when(context.getAddressTranslator()).thenReturn(addressTranslator);

    when(channel.getEndPoint()).thenReturn(node1.getEndPoint());
    when(controlConnection.channel()).thenReturn(channel);
    when(context.getControlConnection()).thenReturn(controlConnection);

    topologyMonitor = new TestTopologyMonitor(context);

    logger = (Logger) LoggerFactory.getLogger(DefaultTopologyMonitor.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
    logger.setLevel(initialLogLevel);
  }

  @Test
  public void should_initialize_control_connection() {
    // When
    topologyMonitor.init();

    // Then
    verify(controlConnection).init(true, false, true);
  }

  @Test
  public void should_not_refresh_control_node() {
    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node1);

    // Then
    assertThatStage(futureInfo).isSuccess(maybeInfo -> assertThat(maybeInfo.isPresent()).isFalse());
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_present() {
    // Given
    node2.broadcastAddress = ADDRESS2;
    topologyMonitor.isSchemaV2 = false;
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers WHERE peer = :address",
            ImmutableMap.of("address", ADDRESS2.getAddress()),
            mockResult(mockPeersRow(2, node2.getHostId()))));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_present_v2() {
    // Given
    node2.broadcastAddress = ADDRESS2;
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers_v2 WHERE peer = :address and peer_port = :port",
            ImmutableMap.of("address", ADDRESS2.getAddress(), "port", 9042),
            mockResult(mockPeersV2Row(2, node2.getHostId()))));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
              assertThat(info.getBroadcastAddress().get().getPort()).isEqualTo(7002);
            });
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_not_present() {
    // Given
    topologyMonitor.isSchemaV2 = false;
    node2.broadcastAddress = null;
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The rpc_address in each row should have been tried, only the last row should have been
    // converted
    // Note: getUuid("host_id") is called once in findInPeers for comparison
    verify(peer3).getUuid("host_id");
    verify(peer3, never()).getString(anyString());

    verify(peer2, times(2)).getUuid("host_id");
    verify(peer2).getString("data_center");
    verify(peer2).getString("rack");
  }

  @Test
  public void should_refresh_node_from_peers_if_broadcast_address_is_not_present_V2() {
    // Given
    topologyMonitor.isSchemaV2 = true;
    node2.broadcastAddress = null;
    AdminRow peer3 = mockPeersV2Row(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersV2Row(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers_v2", mockResult(peer3, peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The host_id in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getUuid("host_id");
    verify(peer3, never()).getString(anyString());

    verify(peer2, times(2)).getUuid("host_id");
    verify(peer2).getString("data_center");
    verify(peer2).getString("rack");
  }

  @Test
  public void should_get_new_node_from_peers() {
    // Given
    AdminRow peer3 = mockPeersRow(4, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(3, node2.getHostId());
    AdminRow peer1 = mockPeersRow(2, node1.getHostId());
    topologyMonitor.isSchemaV2 = false;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.getNewNodeInfo(ADDRESS2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The rpc_address in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getInetAddress("rpc_address");
    verify(peer3, never()).getString(anyString());

    verify(peer2).getInetAddress("rpc_address");
    verify(peer2, never()).getString(anyString());

    verify(peer1).getInetAddress("rpc_address");
    verify(peer1).getString("data_center");
    verify(peer1).getString("rack");
  }

  @Test
  public void should_get_new_node_from_peers_v2() {
    // Given
    AdminRow peer3 = mockPeersV2Row(4, UUID.randomUUID());
    AdminRow peer2 = mockPeersV2Row(3, node2.getHostId());
    AdminRow peer1 = mockPeersV2Row(2, node1.getHostId());
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.peers_v2", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.getNewNodeInfo(ADDRESS2);

    // Then
    assertThatStage(futureInfo)
        .isSuccess(
            maybeInfo -> {
              assertThat(maybeInfo.isPresent()).isTrue();
              NodeInfo info = maybeInfo.get();
              assertThat(info.getDatacenter()).isEqualTo("dc2");
            });
    // The natove in each row should have been tried, only the last row should have been
    // converted
    verify(peer3).getInetAddress("native_address");
    verify(peer3, never()).getString(anyString());

    verify(peer2).getInetAddress("native_address");
    verify(peer2, never()).getString(anyString());

    verify(peer1).getInetAddress("native_address");
    verify(peer1).getString("data_center");
    verify(peer1).getString("rack");
  }

  @Test
  public void should_refresh_node_list_from_local_and_peers() {
    // Given
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult(local)),
        new StubbedQuery("SELECT * FROM system.peers_v2", Collections.emptyMap(), null, true),
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos -> {
              Iterator<NodeInfo> iterator = infos.iterator();
              NodeInfo info1 = iterator.next();
              assertThat(info1.getEndPoint()).isEqualTo(node1.getEndPoint());
              assertThat(info1.getDatacenter()).isEqualTo("dc1");
              NodeInfo info3 = iterator.next();
              assertThat(info3.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.3", 9042));
              assertThat(info3.getDatacenter()).isEqualTo("dc3");
              NodeInfo info2 = iterator.next();
              assertThat(info2.getEndPoint()).isEqualTo(node2.getEndPoint());
              assertThat(info2.getDatacenter()).isEqualTo("dc2");
            });
  }

  @Test
  @UseDataProvider("columnsToCheckV1")
  public void should_skip_invalid_peers_row(String columnToCheck) {
    // Given
    topologyMonitor.isSchemaV2 = false;
    node2.broadcastAddress = ADDRESS2;
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    when(peer2.isNull(columnToCheck)).thenReturn(true);
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers WHERE peer = :address",
            ImmutableMap.of("address", ADDRESS2.getAddress()),
            mockResult(peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo).isSuccess(maybeInfo -> assertThat(maybeInfo).isEmpty());
    assertThat(node2.broadcastAddress).isNotNull().isEqualTo(ADDRESS2);
    assertLog(
        Level.WARN,
        "[null] Found invalid row in system.peers for peer: /127.0.0.2. "
            + "This is likely a gossip or snitch issue, this node will be ignored.");
  }

  @Test
  @UseDataProvider("columnsToCheckV2")
  public void should_skip_invalid_peers_row_v2(String columnToCheck) {
    // Given
    topologyMonitor.isSchemaV2 = true;
    node2.broadcastAddress = ADDRESS2;
    AdminRow peer2 = mockPeersV2Row(2, node2.getHostId());
    when(peer2.isNull(columnToCheck)).thenReturn(true);
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.peers_v2 WHERE peer = :address and peer_port = :port",
            ImmutableMap.of("address", ADDRESS2.getAddress(), "port", 9042),
            mockResult(peer2)));

    // When
    CompletionStage<Optional<NodeInfo>> futureInfo = topologyMonitor.refreshNode(node2);

    // Then
    assertThatStage(futureInfo).isSuccess(maybeInfo -> assertThat(maybeInfo).isEmpty());
    assertThat(node2.broadcastAddress).isNotNull().isEqualTo(ADDRESS2);
    assertLog(
        Level.WARN,
        "[null] Found invalid row in system.peers_v2 for peer: /127.0.0.2. "
            + "This is likely a gossip or snitch issue, this node will be ignored.");
  }

  @Test
  public void should_fail_get_channel_node_info_if_local_result_is_empty() {
    // Given
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult()));

    // When
    CompletionStage<NodeInfo> futureNodeInfo = topologyMonitor.getChannelNodeInfo(channel);

    // Then
    assertThatStage(futureNodeInfo)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(IllegalStateException.class);
              assertThat(error.getMessage())
                  .contains("Expected a row in system.local for node info resolution");
            });
  }

  @Test
  public void should_query_system_local_for_channel_node_info() {
    // Given — a channel to identify (the control connection's connect hook is the caller).
    UUID hostId = UUID.randomUUID();
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'", mockResult(mockLocalRow(1, hostId))));

    // When
    CompletionStage<NodeInfo> futureNodeInfo = topologyMonitor.getChannelNodeInfo(channel);

    // Then
    assertThatStage(futureNodeInfo)
        .isSuccess(nodeInfo -> assertThat(nodeInfo.getHostId()).isEqualTo(hostId));
  }

  @Test
  public void should_identify_the_connected_node_by_the_address_it_reached_not_the_contact_point()
      throws Exception {
    // Given — a control channel that came up through a contact-point hostname. ChannelFactory binds
    // the endpoint it hands the channel to the one address that connection reached, and by
    // PinnableEndPoint's contract that copy is identified exactly like the unpinned original.
    //
    // The pinned address carries the queried *name*, which is what the connect path really
    // produces:
    // the JDK and Netty-DNS resolvers label their results with it, and ChannelFactory
    // #reattachHostname restores it when a custom resolver does not. A bare
    // new InetSocketAddress("127.0.0.1", 9042) would be a shape that path cannot yield, and would
    // let this test pass against an implementation that reads the host string back.
    EndPoint contactPoint =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("db.example.com", 9042));
    InetSocketAddress reached =
        new InetSocketAddress(
            InetAddress.getByAddress("db.example.com", new byte[] {127, 0, 0, 1}), 9042);
    EndPoint pinned = ((PinnableEndPoint) contactPoint).pinTo(reached);
    assertThat(pinned.asMetricPrefix()).isEqualTo("db_example_com:9042");
    assertThat(((InetSocketAddress) pinned.resolve()).getHostString()).isEqualTo("db.example.com");
    when(channel.getEndPoint()).thenReturn(pinned);
    UUID hostId = UUID.randomUUID();
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'", mockResult(mockLocalRow(1, hostId))));

    // When
    CompletionStage<NodeInfo> futureNodeInfo = topologyMonitor.getChannelNodeInfo(channel);

    // Then — the node is identified by its own address, not by the name it was reached through.
    // Keeping the contact point's identity would not even be exclusively this node's: the
    // reconnection fallback hands the contact points back every round, so each successive control
    // node would take the same metric prefix, and two live nodes sharing one prefix means the older
    // one's clearMetrics() deletes the newcomer's series.
    assertThatStage(futureNodeInfo)
        .isSuccess(
            nodeInfo -> {
              assertThat(nodeInfo.getEndPoint().asMetricPrefix()).isEqualTo("127_0_0_1:9042");
              // What the node connects to is unchanged, label included, so the TLS peer host and
              // the Kerberos service name stay the name the operator configured -- with no reverse
              // lookup.
              InetSocketAddress resolved = (InetSocketAddress) nodeInfo.getEndPoint().resolve();
              assertThat(resolved).isEqualTo(reached);
              assertThat(resolved.getHostString()).isEqualTo("db.example.com");
              assertThat(resolved.getAddress().getHostName()).isEqualTo("db.example.com");
            });
  }

  @Test
  public void should_not_let_a_cached_reverse_name_change_the_connected_nodes_identity()
      throws Exception {
    // An IP-literal contact point. Its pinned address starts out nameless, and begins reporting a
    // reverse-DNS name as soon as DefaultSslEngineFactory calls getHostName() on the shared
    // InetAddress -- which is what an already-labelled instance stands in for here. The identity
    // has
    // to come from the bytes, or it would depend on whether TLS is enabled and whether a PTR record
    // exists, and would move mid-session for the same node.
    EndPoint contactPoint =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("127.0.0.1", 9042));
    InetSocketAddress reachedWithPtrName =
        new InetSocketAddress(
            InetAddress.getByAddress("node1.internal.example.com", new byte[] {127, 0, 0, 1}),
            9042);
    EndPoint pinned = ((PinnableEndPoint) contactPoint).pinTo(reachedWithPtrName);
    when(channel.getEndPoint()).thenReturn(pinned);
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'",
            mockResult(mockLocalRow(1, UUID.randomUUID()))));

    CompletionStage<NodeInfo> futureNodeInfo = topologyMonitor.getChannelNodeInfo(channel);

    // The identity is already the literal, so there is nothing to rebuild and the instance is kept.
    assertThatStage(futureNodeInfo)
        .isSuccess(
            nodeInfo -> {
              assertThat(nodeInfo.getEndPoint()).isSameAs(pinned);
              assertThat(nodeInfo.getEndPoint().asMetricPrefix()).isEqualTo("127_0_0_1:9042");
            });
  }

  @Test
  public void should_keep_the_channels_own_endpoint_when_it_already_names_the_connected_address() {
    // The reconnection-to-a-known-node case, and every refresh after the first: nothing was pinned
    // because the endpoint already held the address, so the existing instance is kept -- which is
    // what keeps the control-node check in refreshNode() an identity comparison rather than an
    // endpoint equals() (and DefaultEndPoint's equals resolves names).
    EndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    when(channel.getEndPoint()).thenReturn(endPoint);
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'",
            mockResult(mockLocalRow(1, UUID.randomUUID()))));

    CompletionStage<NodeInfo> futureNodeInfo = topologyMonitor.getChannelNodeInfo(channel);

    assertThatStage(futureNodeInfo)
        .isSuccess(nodeInfo -> assertThat(nodeInfo.getEndPoint()).isSameAs(endPoint));
  }

  @Test
  public void should_stop_executing_queries_once_closed() {
    // Given
    topologyMonitor.close();

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isFailed(error -> assertThat(error).isInstanceOf(IllegalStateException.class));
  }

  @Test
  public void should_warn_when_control_host_found_in_system_peers() {
    // Given
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer1 = mockPeersRow(1, node2.getHostId()); // invalid
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult(local)),
        new StubbedQuery("SELECT * FROM system.peers_v2", Collections.emptyMap(), null, true),
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos ->
                assertThat(infos)
                    .hasSize(3)
                    .extractingResultOf("getEndPoint")
                    .containsOnlyOnce(node1.getEndPoint()));
    assertLogContains(
        Level.WARN,
        "[null] Control node /127.0.0.1:9042 has an entry for itself in system.peers: "
            + "this entry will be ignored. This is likely due to a misconfiguration; "
            + "please verify your rpc_address configuration in cassandra.yaml on "
            + "all nodes in your cluster.");
  }

  @Test
  public void should_warn_when_control_host_found_in_system_peers_v2() {
    // Given
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer3 = mockPeersRow(3, UUID.randomUUID());
    AdminRow peer2 = mockPeersRow(2, node2.getHostId());
    AdminRow peer1 = mockPeersRow(1, node2.getHostId()); // invalid
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult(local)),
        new StubbedQuery("SELECT * FROM system.peers_v2", mockResult(peer3, peer2, peer1)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos ->
                assertThat(infos)
                    .hasSize(3)
                    .extractingResultOf("getEndPoint")
                    .containsOnlyOnce(node1.getEndPoint()));
    assertLogContains(
        Level.WARN,
        "[null] Control node /127.0.0.1:9042 has an entry for itself in system.peers_v2: "
            + "this entry will be ignored. This is likely due to a misconfiguration; "
            + "please verify your rpc_address configuration in cassandra.yaml on "
            + "all nodes in your cluster.");
  }

  // Confirm the base case of extracting peer info from peers_v2, no SSL involved
  @Test
  public void should_get_peer_address_info_peers_v2() {
    // Given
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer2 = mockPeersV2Row(3, node2.getHostId());
    AdminRow peer1 = mockPeersV2Row(2, node1.getHostId());
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult(local)),
        new StubbedQuery("SELECT * FROM system.peers_v2", mockResult(peer2, peer1)));
    when(context.getSslEngineFactory()).thenReturn(Optional.empty());

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos -> {
              Iterator<NodeInfo> iterator = infos.iterator();
              // First NodeInfo is for local, skip past that
              iterator.next();
              NodeInfo peer2nodeInfo = iterator.next();
              assertThat(peer2nodeInfo.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.3", 9042));
              NodeInfo peer1nodeInfo = iterator.next();
              assertThat(peer1nodeInfo.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.2", 9042));
            });
  }

  // Confirm the base case of extracting peer info from DSE peers table, no SSL involved
  @Test
  public void should_get_peer_address_info_peers_dse() {
    // Given
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer2 = mockPeersRowDse(3, node2.getHostId());
    AdminRow peer1 = mockPeersRowDse(2, node1.getHostId());
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult(local)),
        new StubbedQuery("SELECT * FROM system.peers_v2", Maps.newHashMap(), null, true),
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer2, peer1)));
    when(context.getSslEngineFactory()).thenReturn(Optional.empty());

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos -> {
              Iterator<NodeInfo> iterator = infos.iterator();
              // First NodeInfo is for local, skip past that
              iterator.next();
              NodeInfo peer2nodeInfo = iterator.next();
              assertThat(peer2nodeInfo.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.3", 9042));
              NodeInfo peer1nodeInfo = iterator.next();
              assertThat(peer1nodeInfo.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.2", 9042));
            });
  }

  // Confirm the base case of extracting peer info from DSE peers table, this time with SSL
  @Test
  public void should_get_peer_address_info_peers_dse_with_ssl() {
    // Given
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer2 = mockPeersRowDseWithSsl(3, node2.getHostId());
    AdminRow peer1 = mockPeersRowDseWithSsl(2, node1.getHostId());
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", mockResult(local)),
        new StubbedQuery("SELECT * FROM system.peers_v2", Maps.newHashMap(), null, true),
        new StubbedQuery("SELECT * FROM system.peers", mockResult(peer2, peer1)));
    when(context.getSslEngineFactory()).thenReturn(Optional.of(sslEngineFactory));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos)
        .isSuccess(
            infos -> {
              Iterator<NodeInfo> iterator = infos.iterator();
              // First NodeInfo is for local, skip past that
              iterator.next();
              NodeInfo peer2nodeInfo = iterator.next();
              assertThat(peer2nodeInfo.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.3", 9043));
              NodeInfo peer1nodeInfo = iterator.next();
              assertThat(peer1nodeInfo.getEndPoint().resolve())
                  .isEqualTo(new InetSocketAddress("127.0.0.2", 9043));
            });
  }

  @Test
  public void should_use_projected_query_on_second_refresh_node_list_call() {
    // Given — first call uses SELECT * and teaches the monitor the available columns
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer2 = mockPeersV2Row(2, node2.getHostId());
    ImmutableList<String> localCols = ImmutableList.of("rpc_address", "data_center", "host_id");
    ImmutableList<String> peersCols = ImmutableList.of("native_address", "native_port", "host_id");
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        // First call — SELECT *
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(localCols, local)),
        new StubbedQuery(
            "SELECT * FROM system.peers_v2",
            AdminResultTestHelper.mockResultWithColumns(peersCols, peer2)));
    topologyMonitor.refreshNodeList().toCompletableFuture().join();

    // Second call — caches are warm, should use projected query with learned columns
    AdminRow local2 = mockLocalRow(1, node1.getHostId());
    AdminRow peer2b = mockPeersV2Row(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT " + String.join(", ", localCols) + " FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(localCols, local2)),
        new StubbedQuery(
            "SELECT " + String.join(", ", peersCols) + " FROM system.peers_v2",
            AdminResultTestHelper.mockResultWithColumns(peersCols, peer2b)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos).isSuccess(infos -> assertThat(infos).hasSize(2));
  }

  @Test
  public void should_relearn_the_local_projection_from_every_response() {
    // The control connection clears this cache before each candidate's identity read -- but that
    // orders the reads, not the answers. A candidate abandoned on the connect-hook timeout is
    // abandoned rather than cancelled, so both reads can be outstanding at once and the refused
    // one can answer first. A cache that filled only when empty would take its columns and then
    // decline the accepted candidate's, and since the projection is an intersection that can only
    // shrink, the node the driver keeps would stop reporting every column the refused one lacked --
    // dse_version here, and with it the driver's whole idea of what server it is talking to.
    UUID hostId = node1.getHostId();
    ImmutableList<String> narrow = ImmutableList.of("rpc_address", "data_center", "host_id");
    ImmutableList<String> wide =
        ImmutableList.of("rpc_address", "data_center", "host_id", "dse_version");
    CompletableFuture<AdminResult> refusedAnswer = new CompletableFuture<>();
    CompletableFuture<AdminResult> keptAnswer = new CompletableFuture<>();
    topologyMonitor.stubQueries(
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", refusedAnswer),
        new StubbedQuery("SELECT * FROM system.local WHERE key='local'", keptAnswer),
        new StubbedQuery(
            "SELECT " + String.join(", ", wide) + " FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(wide, mockLocalRow(1, hostId))));

    // When -- both reads go out with the cache cleared, as the hook does for two candidates, and
    // the one the loop refused is the first to answer.
    topologyMonitor.resetLocalColumnCache();
    CompletionStage<NodeInfo> refused = topologyMonitor.getChannelNodeInfo(channel);
    topologyMonitor.resetLocalColumnCache();
    CompletionStage<NodeInfo> kept = topologyMonitor.getChannelNodeInfo(channel);
    refusedAnswer.complete(
        AdminResultTestHelper.mockResultWithColumns(narrow, mockLocalRow(1, hostId)));
    keptAnswer.complete(AdminResultTestHelper.mockResultWithColumns(wide, mockLocalRow(1, hostId)));
    assertThatStage(refused).isSuccess();
    assertThatStage(kept).isSuccess();

    // Then -- a third read, with nothing cleared in front of it, projects the columns of the
    // response that came *last*, not of the one that came first. The stub is the assertion:
    // fill-only-when-empty would send `narrow` here and the query strings would not match.
    assertThatStage(topologyMonitor.getChannelNodeInfo(channel))
        .isSuccess(nodeInfo -> assertThat(nodeInfo.getHostId()).isEqualTo(hostId));
  }

  @Test
  public void should_revert_to_select_star_after_reset_column_caches() {
    // Given — warm the caches with a first call
    AdminRow local = mockLocalRow(1, node1.getHostId());
    AdminRow peer2 = mockPeersV2Row(2, node2.getHostId());
    ImmutableList<String> localCols = ImmutableList.of("rpc_address", "data_center", "host_id");
    ImmutableList<String> peersCols = ImmutableList.of("native_address", "native_port", "host_id");
    topologyMonitor.isSchemaV2 = true;
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(localCols, local)),
        new StubbedQuery(
            "SELECT * FROM system.peers_v2",
            AdminResultTestHelper.mockResultWithColumns(peersCols, peer2)));
    topologyMonitor.refreshNodeList().toCompletableFuture().join();

    // Reset the caches (as done on reconnect)
    topologyMonitor.resetColumnCaches();

    // Second call must revert to SELECT * since caches were cleared
    AdminRow local2 = mockLocalRow(1, node1.getHostId());
    AdminRow peer2b = mockPeersV2Row(2, node2.getHostId());
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(localCols, local2)),
        new StubbedQuery(
            "SELECT * FROM system.peers_v2",
            AdminResultTestHelper.mockResultWithColumns(peersCols, peer2b)));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then
    assertThatStage(futureInfos).isSuccess(infos -> assertThat(infos).hasSize(2));
  }

  @Test
  public void should_not_cache_empty_column_set_from_zero_row_peers_result() {
    // Given — single-node cluster where system.peers_v2 returns 0 rows and empty column metadata.
    // The driver must NOT cache the empty set; the next call must still use SELECT *.
    AdminRow local = mockLocalRow(1, node1.getHostId());
    topologyMonitor.isSchemaV2 = true;

    // First call: system.peers_v2 returns 0 rows with an empty column set (simulates a
    // single-node cluster whose server omits column metadata for empty result sets).
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT * FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(
                ImmutableList.of("rpc_address", "data_center", "host_id"), local)),
        new StubbedQuery(
            "SELECT * FROM system.peers_v2",
            AdminResultTestHelper.mockResultWithColumns(
                Collections.emptyList() /* empty column metadata */)));
    topologyMonitor.refreshNodeList().toCompletableFuture().join();

    // Second call must still use SELECT * for system.peers_v2 because the empty list was not
    // cached.
    AdminRow local2 = mockLocalRow(1, node1.getHostId());
    ImmutableList<String> localCols = ImmutableList.of("rpc_address", "data_center", "host_id");
    topologyMonitor.stubQueries(
        new StubbedQuery(
            "SELECT " + String.join(", ", localCols) + " FROM system.local WHERE key='local'",
            AdminResultTestHelper.mockResultWithColumns(localCols, local2)),
        new StubbedQuery(
            "SELECT * FROM system.peers_v2",
            AdminResultTestHelper.mockResultWithColumns(Collections.emptyList())));

    // When
    CompletionStage<Iterable<NodeInfo>> futureInfos = topologyMonitor.refreshNodeList();

    // Then — driver still starts up successfully even though peers are absent
    assertThatStage(futureInfos).isSuccess(infos -> assertThat(infos).hasSize(1));
  }

  @DataProvider
  public static Object[][] columnsToCheckV1() {
    return new Object[][] {{"rpc_address"}, {"host_id"}, {"data_center"}, {"rack"}, {"tokens"}};
  }

  @DataProvider
  public static Object[][] columnsToCheckV2() {
    return new Object[][] {
      {"native_address"}, {"native_port"}, {"host_id"}, {"data_center"}, {"rack"}, {"tokens"}
    };
  }

  /** Mocks the query execution logic. */
  private static class TestTopologyMonitor extends DefaultTopologyMonitor {

    private final Queue<StubbedQuery> queries = new ArrayDeque<>();

    private TestTopologyMonitor(InternalDriverContext context) {
      super(context);
      port = 9042;
    }

    private void stubQueries(StubbedQuery... queries) {
      this.queries.addAll(Arrays.asList(queries));
    }

    @Override
    protected CompletionStage<AdminResult> query(
        DriverChannel channel, String queryString, Map<String, Object> parameters) {
      StubbedQuery nextQuery = queries.poll();
      assertThat(nextQuery).isNotNull();
      assertThat(nextQuery.queryString).isEqualTo(queryString);
      assertThat(nextQuery.parameters).isEqualTo(parameters);
      if (nextQuery.error) {
        Message error =
            new Error(
                ProtocolConstants.ErrorCode.SERVER_ERROR,
                "Unknown keyspace/cf pair (system.peers_v2)");
        return CompletableFutures.failedFuture(new UnexpectedResponseException(queryString, error));
      }
      // A stub may hand back a future the test completes itself, which is the only way to have two
      // of these reads outstanding at once -- the shape the connect hook produces when a candidate
      // is abandoned on its timeout and answers anyway.
      return (nextQuery.pending != null)
          ? nextQuery.pending
          : CompletableFuture.completedFuture(nextQuery.result);
    }
  }

  private static class StubbedQuery {
    private final String queryString;
    private final Map<String, Object> parameters;
    private final AdminResult result;
    private final boolean error;
    private final CompletableFuture<AdminResult> pending;

    private StubbedQuery(
        String queryString,
        Map<String, Object> parameters,
        AdminResult result,
        boolean error,
        CompletableFuture<AdminResult> pending) {
      this.queryString = queryString;
      this.parameters = parameters;
      this.result = result;
      this.error = error;
      this.pending = pending;
    }

    private StubbedQuery(
        String queryString, Map<String, Object> parameters, AdminResult result, boolean error) {
      this(queryString, parameters, result, error, null);
    }

    /** A query whose response the test releases, so that reads can be left in flight. */
    private StubbedQuery(String queryString, CompletableFuture<AdminResult> pending) {
      this(queryString, Collections.emptyMap(), null, false, pending);
    }

    private StubbedQuery(String queryString, Map<String, Object> parameters, AdminResult result) {
      this(queryString, parameters, result, false);
    }

    private StubbedQuery(String queryString, AdminResult result) {
      this(queryString, Collections.emptyMap(), result);
    }
  }

  private AdminRow mockLocalRow(int i, UUID hostId) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.isNull("host_id")).thenReturn(hostId == null);
      when(row.getUuid("host_id")).thenReturn(hostId);
      when(row.getInetAddress("broadcast_address"))
          .thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("data_center")).thenReturn(false);
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.getInetAddress("listen_address")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("rack")).thenReturn(false);
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.getString("release_version")).thenReturn("release_version" + i);

      // The driver should not use this column for the local row, because it can contain the
      // non-broadcast RPC address. Simulate the bug to ensure it's handled correctly.
      when(row.isNull("rpc_address")).thenReturn(false);
      when(row.getInetAddress("rpc_address")).thenReturn(InetAddress.getByName("0.0.0.0"));

      when(row.isNull("tokens")).thenReturn(false);
      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      when(row.contains("peer")).thenReturn(false);
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminRow mockPeersRow(int i, UUID hostId) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.isNull("host_id")).thenReturn(hostId == null);
      when(row.getUuid("host_id")).thenReturn(hostId);
      when(row.getInetAddress("peer")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("data_center")).thenReturn(false);
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.isNull("rack")).thenReturn(false);
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.getString("release_version")).thenReturn("release_version" + i);
      when(row.isNull("rpc_address")).thenReturn(false);
      when(row.getInetAddress("rpc_address")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("tokens")).thenReturn(false);
      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      when(row.contains("peer")).thenReturn(true);

      when(row.isNull("native_address")).thenReturn(true);
      when(row.isNull("native_port")).thenReturn(true);

      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminRow mockPeersV2Row(int i, UUID hostId) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.isNull("host_id")).thenReturn(hostId == null);
      when(row.getUuid("host_id")).thenReturn(hostId);
      when(row.getInetAddress("peer")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.getInteger("peer_port")).thenReturn(7000 + i);
      when(row.isNull("data_center")).thenReturn(false);
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.isNull("rack")).thenReturn(false);
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.getString("release_version")).thenReturn("release_version" + i);
      when(row.isNull("native_address")).thenReturn(false);
      when(row.getInetAddress("native_address")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("native_port")).thenReturn(false);
      when(row.getInteger("native_port")).thenReturn(9042);
      when(row.isNull("tokens")).thenReturn(false);
      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      when(row.contains("peer")).thenReturn(true);
      when(row.contains("peer_port")).thenReturn(true);
      when(row.contains("native_port")).thenReturn(true);

      when(row.isNull("rpc_address")).thenReturn(true);
      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  // Mock row for DSE ~6.8
  private AdminRow mockPeersRowDse(int i, UUID hostId) {
    try {
      AdminRow row = mock(AdminRow.class);
      when(row.contains("peer")).thenReturn(true);
      when(row.isNull("data_center")).thenReturn(false);
      when(row.getString("data_center")).thenReturn("dc" + i);
      when(row.getString("dse_version")).thenReturn("6.8.30");
      when(row.contains("graph")).thenReturn(true);
      when(row.isNull("host_id")).thenReturn(hostId == null);
      when(row.getUuid("host_id")).thenReturn(hostId);
      when(row.getInetAddress("peer")).thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("rack")).thenReturn(false);
      when(row.getString("rack")).thenReturn("rack" + i);
      when(row.isNull("native_transport_address")).thenReturn(false);
      when(row.getInetAddress("native_transport_address"))
          .thenReturn(InetAddress.getByName("127.0.0." + i));
      when(row.isNull("native_transport_port")).thenReturn(false);
      when(row.getInteger("native_transport_port")).thenReturn(9042);
      when(row.isNull("tokens")).thenReturn(false);
      when(row.getSetOfString("tokens")).thenReturn(ImmutableSet.of("token" + i));
      when(row.isNull("rpc_address")).thenReturn(false);

      return row;
    } catch (UnknownHostException e) {
      fail("unexpected", e);
      return null;
    }
  }

  private AdminRow mockPeersRowDseWithSsl(int i, UUID hostId) {
    AdminRow row = mockPeersRowDse(i, hostId);
    when(row.isNull("native_transport_port_ssl")).thenReturn(false);
    when(row.getInteger("native_transport_port_ssl")).thenReturn(9043);
    return row;
  }

  private AdminResult mockResult(AdminRow... rows) {
    return AdminResultTestHelper.mockResult(rows);
  }

  private void assertLog(Level level, String message) {
    verify(appender, atLeast(1)).doAppend(loggingEventCaptor.capture());
    Iterable<ILoggingEvent> logs =
        filter(loggingEventCaptor.getAllValues()).with("level", level).get();
    assertThat(logs).hasSize(1);
    assertThat(logs.iterator().next().getFormattedMessage()).contains(message);
  }

  private void assertLogContains(Level level, String message) {
    verify(appender, atLeast(1)).doAppend(loggingEventCaptor.capture());
    Iterable<ILoggingEvent> logs =
        filter(loggingEventCaptor.getAllValues()).with("level", level).get();
    assertThat(
        Streams.stream(logs).map(ILoggingEvent::getFormattedMessage).anyMatch(message::contains));
  }
}
