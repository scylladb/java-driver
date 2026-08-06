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
package com.datastax.oss.driver.internal.core.channel;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.channel.local.LocalAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class ChannelFactoryProtocolNegotiationTest extends ChannelFactoryTestBase {

  /** A local address no server is bound to: connecting to it fails immediately. */
  private static final SocketAddress UNREACHABLE =
      new LocalAddress(
          ChannelFactoryProtocolNegotiationTest.class.getSimpleName() + "-unreachable");

  @Test
  public void should_succeed_if_version_specified_and_supported_by_server() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn("V4");
    when(protocolVersionRegistry.fromName("V4")).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    completeSimpleChannelInit();

    // Then
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V4);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_fail_if_version_specified_and_not_supported_by_server(int errorCode) {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn("V4");
    when(protocolVersionRegistry.fromName("V4")).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining("Host does not support protocol version V4");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V4);
            });
  }

  @Test
  public void should_fail_if_version_specified_and_considered_beta_by_server() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn("V5");
    when(protocolVersionRegistry.fromName("V5")).thenReturn(DefaultProtocolVersion.V5);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V5.getCode());
    // Server considers v5 beta, e.g. C* 3.10 or 3.11
    writeInboundFrame(
        requestFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Beta version of the protocol used (5/v5-beta), but USE_BETA flag is unset"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining("Host does not support protocol version V5");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V5);
            });
  }

  @Test
  public void should_succeed_if_version_not_specified_and_server_supports_latest_supported() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V4);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_negotiate_if_version_not_specified_and_server_supports_legacy(int errorCode) {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V4))
        .thenReturn(Optional.of(DefaultProtocolVersion.V3));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    // Factory should initialize a new connection, that retries with the lower version
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V3.getCode());
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V3);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_fail_if_negotiation_finds_no_matching_version(int errorCode) {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V4))
        .thenReturn(Optional.of(DefaultProtocolVersion.V3));
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V3)).thenReturn(Optional.empty());
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Client retries with v3
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V3.getCode());
    // Server does not support v3
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining(
                      "Protocol negotiation failed: could not find a common version "
                          + "(attempted: [V4, V3])");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V4, DefaultProtocolVersion.V3);
            });
  }

  @Test
  public void should_not_try_next_address_of_identified_node_when_negotiation_exhausts_versions() {
    // Given – an *identified* node (its host id is known, so every address its name expands to is
    // that same node) whose name expands to two candidates: the same live server twice, so
    // whichever the rotation picks first is irrelevant. The server rejects every protocol version.
    mockNegotiationLadderDownToV3();
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("test.cluster.fake", 9042)),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            true);

    exhaustNegotiationLadder();

    // Then – the second candidate must not be attempted: for a node we have already identified, a
    // protocol-version rejection is a property of the node, not of the address, so replaying the
    // negotiation ladder against the remaining IPs would buy nothing. Checked before the future
    // assertion so that on regression the stray frame is drained; leaving it unread would block the
    // server's exchanger and hang the whole suite in tearDown() instead of failing this test.
    assertThat(tryReadOutboundFrame(200))
        .as("second candidate must not be attempted after negotiation exhaustion")
        .isNull();
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e).isInstanceOf(UnsupportedProtocolVersionException.class);
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V4, DefaultProtocolVersion.V3);
              assertThat(e.getSuppressed())
                  .as("no other candidate should have been tried, so nothing to suppress")
                  .isEmpty();
            });
  }

  @Test
  public void
      should_try_next_address_of_unidentified_endpoint_when_negotiation_exhausts_versions() {
    // Given – the same setup, but for an endpoint the driver has not identified yet: a contact
    // point, before host ids have been read. Its name may well expand to addresses of *different*
    // nodes, so a version rejection by the first says nothing about the second.
    mockNegotiationLadderDownToV3();
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("test.cluster.fake", 9042)),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            false);

    // The first candidate exhausts the ladder...
    exhaustNegotiationLadder();
    // ...and the second is tried all the same, replaying the ladder from the top. Before this,
    // resolve-contact-points=true made each address a separate node and ControlConnection advanced
    // to the next one on exactly this error; collapsing a name into one node must not lose that.
    exhaustNegotiationLadder();

    // Then
    assertThat(tryReadOutboundFrame(200))
        .as("the name expands to two addresses, so there is no third attempt")
        .isNull();
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e).isInstanceOf(UnsupportedProtocolVersionException.class);
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .as("each candidate negotiates on its own, so this is the last one's ladder")
                  .containsExactly(DefaultProtocolVersion.V4, DefaultProtocolVersion.V3);
              assertThat(e.getSuppressed())
                  .as("the first candidate's failure must still be reported")
                  .hasSize(1);
              assertThat(e.getSuppressed()[0])
                  .isInstanceOf(UnsupportedProtocolVersionException.class);
            });
  }

  @Test
  public void should_surface_the_node_wide_rejection_when_an_earlier_address_failed_on_transport() {
    // Given – an identified node whose name expands to a dead address first and to the live server
    // second, the server rejecting every protocol version. Every address of an identified node is
    // that same node, so the rejection is a property of the node rather than of the address.
    mockNegotiationLadderDownToV3();
    installResolver(
        new TestAddressResolverGroup(Arrays.asList(UNREACHABLE, SERVER_ADDRESS.resolve())));
    ChannelFactory factory = newChannelFactory();
    // The order matters here, and only here: it is what makes the pass end with a *mixed* set of
    // failures instead of the version rejection on its own.
    factory.random = new KeepResolverOrder();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(InetSocketAddress.createUnresolved("test.cluster.fake", 9042)),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            true);

    // The dead address sends nothing, so the ladder below is the second candidate's.
    exhaustNegotiationLadder();

    // Then – the version rejection is what the caller must see, even though the pass also produced
    // a
    // transport failure that ChannelPool#handleError does not classify and would otherwise be
    // preferred (see ChannelFactory#surfacedFailure: a fatal failure needs every address to agree).
    // A node-wide failure is the exception to that rule: demoting it would turn the forced-down
    // node
    // that a single-address connect has always produced into a plain reconnect.
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e).isInstanceOf(UnsupportedProtocolVersionException.class);
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V4, DefaultProtocolVersion.V3);
              assertThat(e.getSuppressed())
                  .as("the earlier address's transport failure should still be attached")
                  .hasSize(1);
            });
  }

  /** Negotiation starts at V4 and has exactly one downgrade available, to V3. */
  private void mockNegotiationLadderDownToV3() {
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V4))
        .thenReturn(Optional.of(DefaultProtocolVersion.V3));
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V3)).thenReturn(Optional.empty());
  }

  /**
   * Plays the server side of a full negotiation ladder against one candidate address: V4 rejected,
   * downgrade retry with V3 rejected, i.e. no version left to try on that address.
   */
  private void exhaustNegotiationLadder() {
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    writeInboundFrame(
        requestFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR, "Invalid or unsupported protocol version"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V3.getCode());
    writeInboundFrame(
        requestFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR, "Invalid or unsupported protocol version"));
  }

  @Test
  public void should_fail_future_when_downgrade_lookup_throws_in_connect_listener() {
    // Given – a version registry that throws when the factory looks up the downgrade. The lookup
    // runs inside the Netty connect listener, which swallows throwables: without the blanket catch
    // in connectToAddress() the connect future would never complete and the attempt would hang.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    RuntimeException failure = new IllegalStateException("registry broken");
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V4)).thenThrow(failure);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4, which is what sends the factory to the downgrade lookup
    writeInboundFrame(
        requestFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR, "Invalid or unsupported protocol version"));

    // Then
    assertThatStage(channelFuture).isFailed(e -> assertThat(e).isSameAs(failure));
  }

  /**
   * Depending on the Cassandra version, an "unsupported protocol" response can use different error
   * codes, so we test all of them.
   */
  @DataProvider
  public static Object[][] unsupportedProtocolCodes() {
    return new Object[][] {
      new Object[] {ProtocolConstants.ErrorCode.PROTOCOL_ERROR},
      // C* 2.1 reports a server error instead of protocol error, see CASSANDRA-9451.
      new Object[] {ProtocolConstants.ErrorCode.SERVER_ERROR}
    };
  }
}
