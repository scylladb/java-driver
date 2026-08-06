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

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Ready;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.local.LocalAddress;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Test;

/**
 * Verifies how {@link ChannelFactory#connect} treats the several addresses a name expands to: they
 * are tried in sequence in a shuffled order, at most {@code
 * advanced.connection.max-candidate-addresses} of them, and failures are aggregated rather than
 * dropped.
 *
 * <p>The expansion itself is exercised in {@link ChannelFactoryNettyResolverTest}; here the
 * resolver is only the mechanism for producing more than one address from a single endpoint.
 */
public class ChannelFactoryMultiAddressTest extends ChannelFactoryTestBase {

  // Local addresses that no server is bound to: connecting to them fails immediately.
  private static final SocketAddress UNREACHABLE_1 =
      new LocalAddress(ChannelFactoryMultiAddressTest.class.getSimpleName() + "-unreachable-1");
  private static final SocketAddress UNREACHABLE_2 =
      new LocalAddress(ChannelFactoryMultiAddressTest.class.getSimpleName() + "-unreachable-2");
  private static final SocketAddress UNREACHABLE_3 =
      new LocalAddress(ChannelFactoryMultiAddressTest.class.getSimpleName() + "-unreachable-3");

  /** The name the endpoint reports, and that only the resolver knows how to expand. */
  private static final InetSocketAddress HOSTNAME =
      InetSocketAddress.createUnresolved("test.cluster.fake", 9042);

  @Test
  public void should_fail_with_suppressed_causes_when_all_addresses_are_unreachable() {
    // Given – a name that expands to two dead addresses.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    installResolver(new TestAddressResolverGroup(Arrays.asList(UNREACHABLE_1, UNREACHABLE_2)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then -- the future fails, and the earlier address's failure is preserved on the last one's
    // error rather than being silently dropped.
    //
    // The count of suppressed entries cannot say that, for the reason the max-candidates test below
    // spells out: a single dead candidate already contributes two entangled failures, the transport
    // refusal plus the init-write failure PromiseCombiner attaches to it. So getSuppressed() is
    // non-empty with one address dialled and nothing carried, and isNotEmpty() would hold with the
    // carrying removed outright. The set of addresses named anywhere in the aggregate is what
    // reflects what was carried.
    assertThatStage(channelFuture)
        .isFailed(
            e ->
                assertThat(mentionedUnreachableAddresses(e))
                    .as("both addresses' failures should be reachable from the surfaced error")
                    .hasSize(2));
  }

  @Test
  public void should_attach_each_earlier_failure_at_most_once() {
    // Given – three dead addresses, so there are earlier failures to carry.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    installResolver(
        new TestAddressResolverGroup(Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then – attaching to the error being reported mutates an object the driver does not own, so
    // every cause has to appear at most once and the error must never suppress itself. Nothing
    // stops two candidates from failing with the same instance -- a pipeline handler that throws a
    // stackless singleton, say -- and such an instance would otherwise grow a suppressed entry on
    // every connect for as long as the JVM lives.
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              List<Throwable> suppressed = Arrays.asList(e.getSuppressed());
              assertThat(suppressed).isNotEmpty();
              for (int i = 0; i < suppressed.size(); i++) {
                assertThat(suppressed.get(i)).isNotSameAs(e);
                for (int j = i + 1; j < suppressed.size(); j++) {
                  assertThat(suppressed.get(i)).isNotSameAs(suppressed.get(j));
                }
              }
            });
  }

  @Test
  public void should_try_next_address_when_authentication_fails_on_a_contact_point() {
    // Given – a name expanding to two addresses, both the same live server, which asks for
    // authentication the driver has no provider for. The endpoint is a bare contact point, so the
    // driver does not yet know which node -- or even which cluster -- any of these addresses is.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(context.getAuthProvider()).thenReturn(Optional.empty());
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Authenticate("mockAuthenticator"));

    // Then – the loop advances. Authentication completes before the cluster-name check
    // (ProtocolInitHandler runs STARTUP -> AUTH_RESPONSE -> GET_CLUSTER_NAME), so a stale record
    // pointing at a foreign cluster that wants different credentials fails here rather than at the
    // cluster-name mismatch that would have advanced. Writing off the whole name on this error
    // would therefore make that rule unreachable in exactly the multi-record case this loop is for.
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message)
        .as("the second candidate should have been attempted")
        .isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Authenticate("mockAuthenticator"));

    // And – once both are exhausted the failure is still an AuthenticationException, with the first
    // address's copy attached rather than dropped.
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e).isInstanceOf(AuthenticationException.class);
              assertThat(e.getSuppressed())
                  .as("the first candidate's failure should be attached as suppressed")
                  .hasSize(1);
            });
  }

  @Test
  public void should_surface_the_authentication_failure_when_another_address_fails_on_transport() {
    // Given – a name expanding to the live server (which asks for authentication the driver has no
    // provider for) and a dead address. The shuffled order does not matter: whichever is tried
    // first, the pass ends with one authentication failure and one transport failure.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(context.getAuthProvider()).thenReturn(Optional.empty());
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, UNREACHABLE_1)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Authenticate("mockAuthenticator"));

    // Then – even when the transport failure is the *last* error, propagating it would report a
    // connect failure for what is really a rejected password: callers branch on the type of what
    // they receive (ChannelPool#handleError, ControlConnection's auth-specific warning and its
    // errors.connection.auth metric), and with a shuffled multi-record name which address happens
    // to be tried last is arbitrary. The classified failure wins, and the transport one is still
    // attached.
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .as("an authentication failure must not be demoted by a later transport failure")
                  .isInstanceOf(AuthenticationException.class);
              assertThat(e.getSuppressed())
                  .as("the transport failure should still be attached")
                  .hasSize(1);
              assertThat(e.getSuppressed()[0]).isNotInstanceOf(AuthenticationException.class);
            });
  }

  @Test
  public void should_not_surface_a_cluster_name_mismatch_that_only_one_address_reported() {
    // Given – a factory that already knows the cluster name (from a first connection), then a name
    // expanding to the live server -- which now answers with a *different* cluster name -- and a
    // dead address. Whichever order the shuffle picks, the pass ends with one cluster-name mismatch
    // and one transport failure.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();
    CompletionStage<DriverChannel> firstChannel =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();
    assertThatStage(firstChannel).isSuccess();
    installResolver(
        new TestAddressResolverGroup(Arrays.asList(SERVER_ADDRESS.resolve(), UNREACHABLE_1)));

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // The dead address sends nothing, so this drives the live candidate whichever position it got.
    // The protocol version and the product type are known by now, hence no OPTIONS request.
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("wrongClusterName"));

    // Then – the mismatch must not be the failure that surfaces, not even as the last error of the
    // pass. ChannelPool#handleError turns it into TopologyEvent.forceDown and nothing in the driver
    // ever reverses one, while one address of a multi-record name fronting another cluster is a
    // stale record rather than a verdict about the node. It is still attached, so nothing is lost.
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .as("a mismatch from a single address must not be promoted over the others")
                  .isNotInstanceOf(ClusterNameMismatchException.class);
              assertThat(
                      Arrays.stream(e.getSuppressed())
                          .anyMatch(s -> s instanceof ClusterNameMismatchException))
                  .as("the mismatch should still be attached as a suppressed exception")
                  .isTrue();
            });
  }

  @Test
  public void should_try_next_address_when_authentication_fails_on_an_identified_node() {
    // Given – the same server and the same two addresses, but a node the driver has already
    // identified (nodeIsIdentified = true, i.e. its host id was read from system.local/peers).
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(context.getAuthProvider()).thenReturn(Optional.empty());
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            true);

    failAuthenticationOnNextCandidate();

    // Then – the loop advances, exactly as it does for a contact point: no single address's
    // failure writes off the endpoint, and the candidate cap -- not a node-wide classification --
    // is what bounds the cost of genuinely wrong credentials.
    failAuthenticationOnNextCandidate();

    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e).isInstanceOf(AuthenticationException.class);
              assertThat(e.getSuppressed())
                  .as("the first candidate's failure should be attached as suppressed")
                  .hasSize(1);
            });
  }

  /** Drives one candidate's handshake as far as the server's authentication challenge. */
  private void failAuthenticationOnNextCandidate() {
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Authenticate("mockAuthenticator"));
  }

  @Test
  public void should_stop_after_the_configured_number_of_addresses() {
    // Given – a name expanding to three addresses, but a cap of two. Every address tried is a full
    // connect plus handshake -- and, with wrong credentials, a rejected login -- and the
    // reconnection fallback re-appends the contact points to every round, so an unbounded walk
    // would repeat per contact point, per round, for as long as the session lives. The cap is what
    // bounds that.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(2);
    installResolver(
        new TestAddressResolverGroup(Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then – only two addresses were dialed. The count of suppressed entries is not what to assert
    // on: a single dead candidate can contribute two entangled failures (the transport refusal,
    // plus an init-write failure that PromiseCombiner attaches to it as suppressed). The set of
    // addresses named anywhere in the aggregate is what reflects the dials.
    assertThatStage(channelFuture)
        .isFailed(
            e ->
                assertThat(mentionedUnreachableAddresses(e))
                    .as("a cap of 2 means exactly two addresses dialed")
                    .hasSize(2));
  }

  private static final Pattern UNREACHABLE_NAME = Pattern.compile("unreachable-\\d");

  /** The distinct dead-address names mentioned anywhere in {@code error}'s suppressed tree. */
  private static Set<String> mentionedUnreachableAddresses(Throwable error) {
    Set<String> names = new HashSet<>();
    Deque<Throwable> toVisit = new ArrayDeque<>();
    toVisit.push(error);
    while (!toVisit.isEmpty()) {
      Throwable current = toVisit.pop();
      String message = current.getMessage();
      if (message != null) {
        Matcher matcher = UNREACHABLE_NAME.matcher(message);
        while (matcher.find()) {
          names.add(matcher.group());
        }
      }
      for (Throwable suppressed : current.getSuppressed()) {
        toVisit.push(suppressed);
      }
    }
    return names;
  }

  @Test
  public void should_shuffle_candidates_without_losing_any() {
    // The order is random per connect -- that is what spreads load across a name's records and
    // varies the starting address between successive attempts -- but every address must survive
    // the shuffle, since the loop's fallback walks this list.
    ChannelFactory factory = newChannelFactory();

    assertThat(
            factory.shuffleAndLimit(
                Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3), true))
        .containsExactlyInAnyOrder(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3);
  }

  @Test
  public void should_order_candidates_by_the_injected_random_source() {
    // The injection point for ordering-sensitive tests: a seeded Random produces the same
    // permutation on two factories, so a scenario that needs a particular order picks a seed
    // instead of depending on a sort the production code no longer performs.
    List<SocketAddress> addresses = Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3);
    ChannelFactory one = newChannelFactory();
    ChannelFactory other = newChannelFactory();
    one.random = new Random(42);
    other.random = new Random(42);

    assertThat(one.shuffleAndLimit(addresses, true))
        .containsExactlyElementsOf(other.shuffleAndLimit(addresses, true));
  }

  @Test
  public void should_leave_a_single_address_alone() {
    ChannelFactory factory = newChannelFactory();

    assertThat(factory.shuffleAndLimit(Collections.singletonList(UNREACHABLE_1), true))
        .containsExactly(UNREACHABLE_1);
  }

  @Test
  public void should_truncate_the_shuffled_list_to_the_cap() {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(2);
    ChannelFactory factory = newChannelFactory();
    List<SocketAddress> addresses = Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3);

    List<SocketAddress> capped = factory.shuffleAndLimit(addresses, true);

    assertThat(capped).hasSize(2);
    assertThat(addresses).containsAll(capped);
  }

  @Test
  public void should_clamp_the_cap_to_at_least_one_address() {
    // Zero or a negative value cannot mean "dial nothing" -- the attempt would fail without ever
    // trying an address. It degrades to the pre-multi-address behavior of one address per attempt.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(0);
    ChannelFactory factory = newChannelFactory();

    assertThat(factory.shuffleAndLimit(Arrays.asList(UNREACHABLE_1, UNREACHABLE_2), true))
        .hasSize(1);
  }

  @Test
  public void should_not_shuffle_when_the_addresses_are_not_interchangeable() {
    // A name that may denote different hosts -- what an AddressTranslator can hand back, and
    // SubnetAddressTranslator does by default -- must keep the resolver's order: a random one would
    // scatter a single Node's pool across hosts that routing, shard awareness and per-node metrics
    // all attribute to that one node. Keeping the order makes such a pool converge on one address,
    // as it did before multi-address support, while the rest of the list still serves as fallback.
    List<SocketAddress> addresses = Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3);
    ChannelFactory factory = newChannelFactory();
    // A seed that does permute this list, so the assertion below fails if the shuffle still runs.
    factory.random = new Random(42);
    assertThat(factory.shuffleAndLimit(addresses, true)).isNotEqualTo(addresses);

    assertThat(factory.shuffleAndLimit(addresses, false)).containsExactlyElementsOf(addresses);
  }

  @Test
  public void should_still_cap_the_candidates_when_the_order_is_kept() {
    // The cap is what bounds the cost of one attempt, and that applies whether or not the addresses
    // were shuffled.
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(2);
    ChannelFactory factory = newChannelFactory();

    assertThat(
            factory.shuffleAndLimit(
                Arrays.asList(UNREACHABLE_1, UNREACHABLE_2, UNREACHABLE_3), false))
        .containsExactly(UNREACHABLE_1, UNREACHABLE_2);
  }

  // ---- spreadAcrossAddresses() ----------------------------------------------

  /** The endpoints below only have to exist; nothing in this section connects to them. */
  private static final InetSocketAddress SOME_ADDRESS =
      InetSocketAddress.createUnresolved("node.example.com", 9042);

  @Test
  public void should_spread_across_the_addresses_of_a_contact_point() {
    // Nothing is known about a contact point's addresses -- they may be different nodes -- so there
    // is no node identity to preserve and every shape is spread.
    assertThat(ChannelFactory.spreadAcrossAddresses(new DefaultEndPoint(SOME_ADDRESS), false))
        .isTrue();
    assertThat(
            ChannelFactory.spreadAcrossAddresses(
                new SniEndPoint(SOME_ADDRESS, "server-name"), false))
        .isTrue();
  }

  @Test
  public void should_spread_across_the_addresses_of_an_identified_node_behind_a_proxy() {
    // An SNI proxy routes by server name, so every one of its A-records reaches this same node.
    // Spreading is what SniEndPoint#resolve() itself did, by rotating through the sorted records,
    // before resolution moved to the connection layer.
    assertThat(
            ChannelFactory.spreadAcrossAddresses(
                new SniEndPoint(SOME_ADDRESS, "server-name"), true))
        .isTrue();
  }

  @Test
  public void should_keep_the_order_for_an_identified_node_on_a_plain_endpoint() {
    // The case the interchangeability flag exists to exclude: a DefaultEndPoint holding a name an
    // AddressTranslator supplied carries no guarantee that its addresses are one node.
    assertThat(ChannelFactory.spreadAcrossAddresses(new DefaultEndPoint(SOME_ADDRESS), true))
        .isFalse();
  }

  @Test
  public void should_keep_the_order_for_an_identified_node_on_a_third_party_endpoint() {
    // An EndPoint that does not implement PinnableEndPoint cannot say, and the conservative reading
    // is the one that preserves node identity.
    EndPoint thirdParty = mock(EndPoint.class);
    when(thirdParty.resolve()).thenReturn(SOME_ADDRESS);

    assertThat(ChannelFactory.spreadAcrossAddresses(thirdParty, true)).isFalse();
  }

  // ---- reattachHostname() ---------------------------------------------------

  @Test
  public void should_reattach_queried_hostname_to_nameless_resolved_address() throws Exception {
    // A custom resolver may build its results from raw address bytes; the queried name must be
    // re-attached so TLS hostname validation checks the configured name (not the IP or a PTR
    // record) and reading the host name never triggers a reverse lookup on the event loop.
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9999);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(HOSTNAME, candidate);

    assertThat(result.isUnresolved()).isFalse();
    // getHostString() never looks anything up; getHostName() reverse-resolves a *nameless*
    // address, so it returning the queried name proves the name is embedded, not looked up.
    assertThat(result.getHostString()).isEqualTo("test.cluster.fake");
    assertThat(result.getHostName()).isEqualTo("test.cluster.fake");
    assertThat(result.getAddress().getHostAddress()).isEqualTo("10.0.0.1");
    // The candidate's port wins over the original's: a resolver may remap ports too.
    assertThat(result.getPort()).isEqualTo(9999);
    // Equality is unchanged (a resolved InetSocketAddress compares IP bytes + port only), so
    // pinning and the pin-equality shortcuts behave exactly as with the raw candidate.
    assertThat(result).isEqualTo(candidate);
  }

  @Test
  public void should_override_resolver_provided_hostname_with_queried_name() throws Exception {
    // A resolver may label its results with a canonical/CNAME name of its own. That name would end
    // up on the pinned endpoint and hence be the one TLS hostname verification checks the server
    // certificate against, so the name the user configured has to win over it.
    InetSocketAddress candidate =
        new InetSocketAddress(
            InetAddress.getByAddress("cname.example.fake", new byte[] {10, 0, 0, 1}), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(HOSTNAME, candidate);

    assertThat(result.getHostString()).isEqualTo("test.cluster.fake");
    assertThat(result.getAddress().getHostAddress()).isEqualTo("10.0.0.1");
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_pass_candidate_through_when_it_already_carries_the_queried_name()
      throws Exception {
    // The common case: the JDK and Netty-DNS resolvers attach the queried name themselves, so
    // there is nothing to rebuild.
    InetSocketAddress candidate =
        new InetSocketAddress(
            InetAddress.getByAddress("test.cluster.fake", new byte[] {10, 0, 0, 1}), 9042);

    assertThat(ChannelFactory.reattachHostname(HOSTNAME, candidate)).isSameAs(candidate);
  }

  @Test
  public void should_pass_non_inet_candidate_through() {
    // The local-transport addresses these unit tests connect over must never be touched.
    assertThat(ChannelFactory.reattachHostname(HOSTNAME, UNREACHABLE_1)).isSameAs(UNREACHABLE_1);
  }

  @Test
  public void should_pass_candidate_through_when_original_carries_no_name() throws Exception {
    // An original written as an IP literal has no name to carry over, and inventing one from the
    // literal would be worse than leaving the candidate alone: a resolver is free to redirect it to
    // a different IP, which would then be labelled with the literal form of a *different* address.
    InetSocketAddress original = new InetSocketAddress("127.0.0.1", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042);

    assertThat(ChannelFactory.reattachHostname(original, candidate)).isSameAs(candidate);
    assertThat(AddressUtils.carriesName(original)).isFalse();
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("10.0.0.1", 9042)))
        .isFalse();
  }

  @Test
  public void should_reattach_name_of_a_resolved_original() throws Exception {
    // A resolved original still reaches the resolver -- whether an address needs resolving is the
    // resolver's call, and a custom one may redirect it. Its name is the one the operator
    // configured, so it must survive onto whatever the resolver substitutes, exactly as it did when
    // Netty resolved the TCP destination and the channel kept the original endpoint for TLS.
    InetSocketAddress original = new InetSocketAddress("localhost", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    assertThat(AddressUtils.carriesName(original)).isTrue();
    assertThat(result.getHostString()).isEqualTo("localhost");
    assertThat(result.getAddress().getHostAddress()).isEqualTo("10.0.0.1");
  }

  @Test
  public void should_reattach_hostname_to_nameless_ipv6_address() throws Exception {
    byte[] loopback = new byte[16];
    loopback[15] = 1; // ::1
    InetSocketAddress candidate = new InetSocketAddress(InetAddress.getByAddress(loopback), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(HOSTNAME, candidate);

    assertThat(result.getHostString()).isEqualTo("test.cluster.fake");
    assertThat(result.getAddress()).isEqualTo(candidate.getAddress());
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_keep_the_scope_when_reattaching_to_a_scoped_ipv6_address() throws Exception {
    // A link-local address only points anywhere together with its zone, so the queried name has to
    // be re-attached without dropping the scope. InetAddress.getByAddress(host, bytes) cannot carry
    // one, but Inet6Address.getByAddress(host, bytes, scopeId) can.
    byte[] linkLocal = new byte[16];
    linkLocal[0] = (byte) 0xfe;
    linkLocal[1] = (byte) 0x80;
    linkLocal[15] = 1;
    InetSocketAddress candidate =
        new InetSocketAddress(Inet6Address.getByAddress(null, linkLocal, 3), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(HOSTNAME, candidate);

    assertThat(result.getHostString()).isEqualTo("test.cluster.fake");
    assertThat(result.getAddress()).isInstanceOf(Inet6Address.class);
    assertThat(((Inet6Address) result.getAddress()).getScopeId()).isEqualTo(3);
    assertThat(result.getAddress().getAddress()).isEqualTo(linkLocal);
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_keep_the_zone_of_an_interface_scoped_ipv6_address() throws Exception {
    // An address built from a NetworkInterface rather than from an index must keep pointing into
    // the
    // same zone. The numeric scope the JDK derived at construction is what the connect goes on, so
    // carrying that over is enough; only the interface name, a toString() detail, is not.
    Inet6Address linkLocal = firstInterfaceScopedIpv6Address();
    assumeThat(linkLocal).as("no interface-scoped IPv6 address on this host").isNotNull();
    InetSocketAddress candidate = new InetSocketAddress(linkLocal, 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(HOSTNAME, candidate);

    assertThat(result.getHostString()).isEqualTo("test.cluster.fake");
    assertThat(((Inet6Address) result.getAddress()).getScopeId()).isEqualTo(linkLocal.getScopeId());
    assertThat(result.getAddress().getAddress()).isEqualTo(linkLocal.getAddress());
  }

  /** An interface-scoped IPv6 address of this host, or null if it has none. */
  private static Inet6Address firstInterfaceScopedIpv6Address() throws Exception {
    for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
      for (InetAddress address : Collections.list(nif.getInetAddresses())) {
        if (address instanceof Inet6Address
            && ((Inet6Address) address).getScopedInterface() != null) {
          return (Inet6Address) address;
        }
      }
    }
    return null;
  }

  @Test
  public void should_fail_future_when_endpoint_resolve_throws() {
    // ChannelFactory calls EndPoint.resolve() directly on the caller thread, so a third-party
    // implementation that throws must surface as a failed future rather than an escaping exception.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();
    IllegalStateException failure = new IllegalStateException("resolve() blew up");

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new ThrowingEndPoint(failure),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    assertThatStage(channelFuture).isFailed(e -> assertThat(e).isSameAs(failure));
  }

  @Test
  public void should_fail_future_when_endpoint_resolve_returns_null() {
    // EndPoint.resolve() is contractually non-null, but a broken third-party implementation must
    // fail fast rather than NPE later inside an event-loop task, which would leave the future
    // hanging.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new NullResolvingEndPoint(),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    assertThatStage(channelFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("returned null"));
  }

  @Test
  public void should_fail_future_when_event_loop_group_is_rejecting_tasks()
      throws InterruptedException {
    // Resolution is dispatched to an I/O event loop; if the group is already shutting down, that
    // dispatch is rejected synchronously. The rejection must fail the future rather than escape to
    // the caller (connect() never used to throw) or leave the future hanging.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();
    clientGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    assertThatStage(channelFuture)
        .isFailed(e -> assertThat(e).isInstanceOf(RejectedExecutionException.class));
  }

  /** An endpoint whose {@link EndPoint#resolve()} throws, standing in for a broken third party. */
  private static class ThrowingEndPoint implements EndPoint {

    private final RuntimeException failure;

    ThrowingEndPoint(RuntimeException failure) {
      this.failure = failure;
    }

    @NonNull
    @Override
    public SocketAddress resolve() {
      throw failure;
    }

    @NonNull
    @Override
    public String asMetricPrefix() {
      return "test";
    }
  }

  /** A broken third-party endpoint that violates {@code resolve()}'s non-null contract. */
  private static class NullResolvingEndPoint implements EndPoint {

    @NonNull
    @Override
    @SuppressWarnings("NullAway") // deliberately broken, that is the point of the test
    public SocketAddress resolve() {
      return null;
    }

    @NonNull
    @Override
    public String asMetricPrefix() {
      return "test";
    }
  }
}
