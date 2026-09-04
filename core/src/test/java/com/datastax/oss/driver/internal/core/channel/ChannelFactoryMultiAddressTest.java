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
import com.datastax.oss.driver.internal.core.metadata.PinnableEndPoint;
import com.datastax.oss.driver.internal.core.metadata.SniEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Ready;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.channel.local.LocalAddress;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
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

  // ---- addressesAreInterchangeable() and the two booleans derived from it ----

  /** The endpoints below only have to exist; nothing in this section connects to them. */
  private static final InetSocketAddress SOME_ADDRESS =
      InetSocketAddress.createUnresolved("node.example.com", 9042);

  @Test
  public void should_report_a_proxy_endpoint_interchangeable() {
    // An SNI proxy routes by server name, so every one of its A-records reaches the same node.
    assertThat(
            ChannelFactory.addressesAreInterchangeable(
                new SniEndPoint(SOME_ADDRESS, "server-name"), SOME_ADDRESS))
        .isTrue();
  }

  @Test
  public void should_not_report_a_plain_endpoint_interchangeable() {
    // The case the flag exists to exclude: a DefaultEndPoint holding a name an AddressTranslator
    // supplied carries no guarantee that its addresses are one server.
    assertThat(
            ChannelFactory.addressesAreInterchangeable(
                new DefaultEndPoint(SOME_ADDRESS), SOME_ADDRESS))
        .isFalse();
  }

  @Test
  public void should_not_report_a_third_party_endpoint_interchangeable() {
    // An EndPoint that does not implement PinnableEndPoint cannot say, and the conservative reading
    // is the one that assumes nothing.
    EndPoint thirdParty = mock(EndPoint.class);
    when(thirdParty.resolve()).thenReturn(SOME_ADDRESS);

    assertThat(ChannelFactory.addressesAreInterchangeable(thirdParty, SOME_ADDRESS)).isFalse();
  }

  @Test
  public void should_spread_unless_an_identified_node_says_its_addresses_are_not_one_server() {
    // A contact point always spreads: nothing is known about its addresses -- they may be
    // different nodes -- so there is no node identity to preserve.
    assertThat(ChannelFactory.spreadAcrossAddresses(false, false)).isTrue();
    assertThat(ChannelFactory.spreadAcrossAddresses(false, true)).isTrue();
    // An identified node spreads only where its addresses are interchangeable.
    assertThat(ChannelFactory.spreadAcrossAddresses(true, true)).isTrue();
    assertThat(ChannelFactory.spreadAcrossAddresses(true, false)).isFalse();
  }

  @Test
  public void should_treat_one_server_as_answering_everywhere_only_on_identity_or_interchange() {
    // Not the negation of the above, and the difference is the whole of DRIVER-201's rolling-
    // upgrade case: an unidentified contact point on a plain multi-record name is spread across
    // its addresses *and* must not let one address's rejection speak for the others.
    assertThat(ChannelFactory.sameServerAtEveryAddress(false, false)).isFalse();
    assertThat(ChannelFactory.sameServerAtEveryAddress(false, true)).isTrue();
    assertThat(ChannelFactory.sameServerAtEveryAddress(true, false)).isTrue();
    assertThat(ChannelFactory.sameServerAtEveryAddress(true, true)).isTrue();
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
  public void should_pass_redirected_candidate_through_when_original_is_an_ip_literal()
      throws Exception {
    // An original written as an IP literal has no name to carry over, and inventing one from the
    // literal would be worse than leaving the candidate alone: a resolver is free to redirect it to
    // a different IP, which would then be labelled with the literal form of a *different* address.
    InetSocketAddress original = InetSocketAddress.createUnresolved("127.0.0.1", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042);

    assertThat(ChannelFactory.reattachHostname(original, candidate)).isSameAs(candidate);
    assertThat(AddressUtils.carriesName(original)).isFalse();
    assertThat(AddressUtils.carriesName(InetSocketAddress.createUnresolved("10.0.0.1", 9042)))
        .isFalse();
  }

  @Test
  public void should_reattach_the_literal_when_the_resolver_returns_the_same_address()
      throws Exception {
    // Not a no-op, even though the label says the same thing the bytes do: a *nameless* address is
    // what InetSocketAddress#getHostName() answers with a blocking reverse lookup, so leaving the
    // candidate unlabelled is what would send DefaultSslEngineFactory to a PTR record instead of
    // the
    // literal the operator configured. Before multi-address support the contact point stayed
    // unresolved and the literal came back with no lookup at all; labelling restores exactly that.
    InetSocketAddress original = InetSocketAddress.createUnresolved("127.0.0.1", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    assertThat(result.getHostString()).isEqualTo("127.0.0.1");
    // The point of the exercise: getHostName() is a field read answering the configured literal,
    // not a reverse lookup.
    assertThat(result.getAddress().getHostName()).isEqualTo("127.0.0.1");
    assertThat(result.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
    // And the labelled candidate still reports as a literal, so nothing downstream mistakes it for
    // a name.
    assertThat(AddressUtils.carriesName(result)).isFalse();
  }

  @Test
  public void should_match_a_non_canonical_ipv6_literal_against_the_candidate() throws Exception {
    // The literal is compared as an address, not as a string: "::1" and the candidate's
    // getHostAddress() ("0:0:0:0:0:0:0:1") never compare equal as text.
    InetSocketAddress original = InetSocketAddress.createUnresolved("::1", 9042);
    byte[] loopback = new byte[16];
    loopback[15] = 1;
    InetSocketAddress candidate = new InetSocketAddress(InetAddress.getByAddress(loopback), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    assertThat(result.getHostString()).isEqualTo("::1");
    assertThat(result.getAddress()).isEqualTo(candidate.getAddress());
  }

  // ---- materializeLiteral() --------------------------------------------------

  @Test
  public void should_materialize_an_unresolved_ipv4_literal() {
    // A literal needs no name service, so an endpoint holding one has no business failing where
    // resolution is unavailable -- and endpoints hold one routinely now that contact points are
    // kept unresolved whatever they were written as.
    InetSocketAddress literal = InetSocketAddress.createUnresolved("127.0.0.1", 9042);

    InetSocketAddress result = (InetSocketAddress) ChannelFactory.materializeLiteral(literal);

    assertThat(result.isUnresolved()).isFalse();
    assertThat(result.getAddress().getAddress()).isEqualTo(new byte[] {127, 0, 0, 1});
    assertThat(result.getPort()).isEqualTo(9042);
    // Labelled with the literal, not left nameless: getHostName() on a nameless address is a
    // blocking reverse lookup, and DefaultSslEngineFactory would validate the certificate against
    // whatever PTR record it returned instead of what the operator configured.
    assertThat(result.getHostName()).isEqualTo("127.0.0.1");
    assertThat(AddressUtils.carriesName(result)).isFalse();
  }

  @Test
  public void should_materialize_a_bracketed_ipv6_literal() {
    // The spelling AddressUtils#extract preserves: it splits a contact point on its last colon, so
    // "[::1]:9042" arrives with the brackets still on.
    InetSocketAddress literal = InetSocketAddress.createUnresolved("[::1]", 9042);

    InetSocketAddress result = (InetSocketAddress) ChannelFactory.materializeLiteral(literal);

    byte[] loopback = new byte[16];
    loopback[15] = 1;
    assertThat(result.isUnresolved()).isFalse();
    assertThat(result.getAddress().getAddress()).isEqualTo(loopback);
    // Without the brackets: InetAddress.getByAddress(String, byte[]) strips them from the label it
    // is handed. Still a literal, so getHostName() still answers without a reverse lookup, which is
    // the only property this label exists for.
    assertThat(result.getHostName()).isEqualTo("::1");
  }

  @Test
  public void should_materialize_a_zoned_ipv6_literal() throws Exception {
    // A named IPv6 zone is a literal to AddressUtils#carriesName, so it reaches here -- and unlike
    // the byte-matching in reattachHostname, which resolves the zone through Guava, the JDK turns
    // the name into a scope id itself. That costs a NetworkInterface syscall rather than a name
    // lookup, which is the one place materializeLiteral is not free.
    InetAddress linkLocal = aLinkLocalAddress();
    assumeThat(linkLocal)
        .as("requires a host with a link-local IPv6 address on some interface")
        .isNotNull();
    // Already the zoned spelling: getHostAddress() on a scoped address renders the zone as the
    // interface name, e.g. "fe80:0:0:0:...%eth0".
    String spelling = linkLocal.getHostAddress();
    assumeThat(spelling).as("expected a named zone").contains("%");

    InetSocketAddress result =
        (InetSocketAddress)
            ChannelFactory.materializeLiteral(InetSocketAddress.createUnresolved(spelling, 9042));

    assertThat(result).isNotNull();
    assertThat(result.isUnresolved()).isFalse();
    assertThat(result.getAddress().getAddress()).isEqualTo(linkLocal.getAddress());
    assertThat(result.getPort()).isEqualTo(9042);
  }

  /**
   * A link-local IPv6 address of some interface on this host, or {@code null} if it has none. Only
   * a zone that names an interface which actually carries an address in that scope survives {@code
   * InetAddress.getByName}, so the address has to be discovered rather than made up.
   */
  @Nullable
  private static InetAddress aLinkLocalAddress() {
    try {
      for (NetworkInterface nic : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        for (InetAddress address : Collections.list(nic.getInetAddresses())) {
          if (address instanceof Inet6Address && address.isLinkLocalAddress()) {
            return address;
          }
        }
      }
    } catch (SocketException unavailable) {
      // Treated as "this host has none".
    }
    return null;
  }

  @Test
  public void should_not_materialize_a_zone_this_host_does_not_have() {
    // The gate and the materializer disagree here: carriesName() calls it a literal, and
    // InetAddress.getByName() cannot turn a name it has no interface for into a scope id. Falling
    // through to the caller's diagnostic is the right outcome -- such an address could not have
    // been connected to either -- but its wording is about host names, which this is not. Recorded
    // so that the mismatch is a known one rather than a surprise.
    assertThat(
            ChannelFactory.materializeLiteral(
                InetSocketAddress.createUnresolved("fe80::1%no-such-interface", 9042)))
        .isNull();
  }

  @Test
  public void should_not_materialize_a_shorthand_ipv4_literal() {
    // "127.1" is /127.0.0.1 to InetAddress.getByName and to Netty's default resolver, but Guava's
    // parser requires four dotted parts, so carriesName() calls it a host name and this returns at
    // the first gate. Deliberate: getByName("1234") returns /0.0.4.210, so a test as lenient as
    // the JDK's would turn an all-digit host name into a packed IPv4 address. The cost is that
    // such a contact point works normally and fails only where no resolver runs.
    assertThat(ChannelFactory.materializeLiteral(InetSocketAddress.createUnresolved("127.1", 9042)))
        .isNull();
  }

  @Test
  public void should_not_materialize_a_hostname() {
    // The whole point of the diagnostic this sits in front of: a name genuinely needs a resolver,
    // and passing it through would fail later inside Netty with UnresolvedAddressException, naming
    // neither the address nor the reason.
    assertThat(
            ChannelFactory.materializeLiteral(
                InetSocketAddress.createUnresolved("node.example.com", 9042)))
        .isNull();
  }

  @Test
  public void should_not_materialize_an_already_resolved_address() throws Exception {
    // Nothing to do; the caller passes it through untouched.
    assertThat(
            ChannelFactory.materializeLiteral(
                new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042)))
        .isNull();
  }

  @Test
  public void should_reattach_the_name_of_an_already_resolved_original() throws Exception {
    // A resolved original reaches this only because a custom resolver reported it as unresolved in
    // order to redirect it, and its name is re-attached like any other. `new
    // InetSocketAddress(String, int)` resolves eagerly and keeps the name it was given, so this is
    // the shape an AddressTranslator or a third-party EndPoint hands over -- and leaving the
    // redirected candidate nameless is not neutral: DefaultSslEngineFactory would then take the TLS
    // peer host from a blocking reverse lookup and validate the certificate against a PTR record
    // instead of the configured DNS SAN, which is not what the pre-multi-address path did.
    InetSocketAddress original = new InetSocketAddress("localhost", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042);

    assertThat(original.isUnresolved()).isFalse();
    assertThat(AddressUtils.carriesName(original)).isTrue();

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    assertThat(result.getHostString()).isEqualTo("localhost");
    assertThat(result.getAddress().getAddress()).isEqualTo(new byte[] {10, 0, 0, 1});
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_leave_a_resolved_original_alone_when_it_carries_no_name() throws Exception {
    // The other half: a resolved original whose InetAddress has no cached hostName renders the IP
    // literal, so it takes the literal branch and only matches the address it denotes. A redirect
    // stays unlabelled rather than being given a name that resolves elsewhere.
    InetSocketAddress original =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 2}), 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 0, 0, 1}), 9042);

    assertThat(AddressUtils.carriesName(original)).isFalse();
    assertThat(ChannelFactory.reattachHostname(original, candidate)).isSameAs(candidate);
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
  public void should_label_a_candidate_from_a_bracketed_ipv6_literal_original() throws Exception {
    // A contact point written "[2001:db8::5]:9042" reaches here with its brackets on: extract()
    // splits on the last colon and keeps everything before it. carriesName() classifies that as a
    // literal, so this takes the IP-literal branch -- and the branch has to unwrap the brackets
    // before parsing, because InetAddresses.forString rejects the bracketed form outright.
    // Failing to parse would return the candidate unlabelled and hand getHostName() a reverse
    // lookup, which is the outcome the branch exists to prevent.
    byte[] bytes = InetAddress.getByName("2001:db8::5").getAddress();
    InetSocketAddress original = InetSocketAddress.createUnresolved("[2001:db8::5]", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(InetAddress.getByAddress(null, bytes), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    // Labelled with the literal, and with no lookup. The brackets are gone because
    // InetAddress.getByAddress(String, byte[]) strips a surrounding pair from the name it is
    // given -- which is the canonical outcome: getHostString() now answers a bare literal, so
    // carriesName() reports it as a literal on the way back too.
    assertThat(result.getHostString()).isEqualTo("2001:db8::5");
    assertThat(result.getAddress().getAddress()).isEqualTo(bytes);
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_not_label_a_redirected_candidate_from_a_bracketed_original() throws Exception {
    // The redirect guard has to survive the unwrapping: a candidate that is not the address the
    // bracketed literal denotes must come back unlabelled. Before brackets were recognised this
    // case took the name-wins branch instead, which relabels unconditionally.
    InetSocketAddress original = InetSocketAddress.createUnresolved("[2001:db8::5]", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(
            InetAddress.getByAddress(null, InetAddress.getByName("2001:db8::6").getAddress()),
            9042);

    assertThat(ChannelFactory.reattachHostname(original, candidate)).isSameAs(candidate);
  }

  @Test
  public void should_label_a_candidate_from_a_bracketed_and_zoned_ipv6_literal_original()
      throws Exception {
    // Brackets *and* a zone: the brackets have to come off first, or splitting on '%' leaves the
    // closing bracket inside the zone and the opening one inside the literal, and neither half
    // parses.
    byte[] linkLocal = new byte[16];
    linkLocal[0] = (byte) 0xfe;
    linkLocal[1] = (byte) 0x80;
    linkLocal[15] = 1;
    InetSocketAddress original = InetSocketAddress.createUnresolved("[fe80::1%eth0]", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(Inet6Address.getByAddress(null, linkLocal, 3), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    // Bare literal with the zone intact -- getByAddress() strips only the brackets.
    assertThat(result.getHostString()).isEqualTo("fe80::1%eth0");
    assertThat(result.getAddress().getAddress()).isEqualTo(linkLocal);
  }

  @Test
  public void should_label_a_candidate_from_a_zoned_ipv6_literal_original() throws Exception {
    // The original is a *literal* with a zone, which carriesName() reports as a literal (Guava's
    // isInetAddress accepts a zone suffix), so reattachHostname takes its IP-literal branch. That
    // branch cannot hand the string to InetAddresses.forString: Guava resolves the zone against the
    // local interfaces and throws when it does not name one -- it rejects even "%lo" on a host that
    // has an lo interface. Failing there would return the candidate unlabelled, and getHostName()
    // would then answer with a reverse lookup, which is precisely what this branch exists to stop.
    byte[] linkLocal = new byte[16];
    linkLocal[0] = (byte) 0xfe;
    linkLocal[1] = (byte) 0x80;
    linkLocal[15] = 1;
    InetSocketAddress original = InetSocketAddress.createUnresolved("fe80::1%eth0", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(Inet6Address.getByAddress(null, linkLocal, 3), 9042);

    InetSocketAddress result =
        (InetSocketAddress) ChannelFactory.reattachHostname(original, candidate);

    // Labelled with the literal exactly as configured, zone included, and with no lookup.
    assertThat(result.getHostString()).isEqualTo("fe80::1%eth0");
    assertThat(result.getAddress().getAddress()).isEqualTo(linkLocal);
    assertThat(result.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_not_label_a_candidate_that_is_a_different_address_from_a_zoned_original()
      throws Exception {
    // The redirect guard still has to hold on the zoned path: a candidate that is not the address
    // the literal denotes must come back unlabelled.
    byte[] other = new byte[16];
    other[0] = (byte) 0xfe;
    other[1] = (byte) 0x80;
    other[15] = 2;
    InetSocketAddress original = InetSocketAddress.createUnresolved("fe80::1%eth0", 9042);
    InetSocketAddress candidate =
        new InetSocketAddress(Inet6Address.getByAddress(null, other, 3), 9042);

    assertThat(ChannelFactory.reattachHostname(original, candidate)).isSameAs(candidate);
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
  public void should_fail_future_when_addresses_are_interchangeable_throws() {
    // The other implementation-supplied method connect() calls synchronously, and the one that is
    // easy to miss: it decides whether the resolved addresses may be shuffled. Escaping here would
    // be worse than escaping from resolve(), because ControlConnection#reconnect neither wraps its
    // connect() call nor catches inside the whenCompleteAsync callback that drives the recursive
    // ones -- the throwable would be swallowed and Reconnection left stuck ATTEMPT_IN_PROGRESS,
    // with no further attempts.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();
    IllegalStateException failure =
        new IllegalStateException("addressesAreInterchangeable() blew up");

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new ThrowingSpreadEndPoint(failure),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            // Either value would do: the endpoint is consulted on every connect, because both of
            // the booleans derived from its answer need it -- an unidentified contact point still
            // has to know whether one address's rejection speaks for the rest.
            false);

    assertThatStage(channelFuture).isFailed(e -> assertThat(e).isSameAs(failure));
  }

  @Test
  public void should_fail_future_when_endpoint_spread_check_throws_an_error() {
    // The guard catches Throwable, not Exception. An endpoint supplied by someone else can fail
    // with an Error just as readily as with an exception -- NoClassDefFoundError or
    // ExceptionInInitializerError out of lazy class initialization in a shaded or OSGi deployment,
    // AssertionError under -ea -- and the outcome of letting one escape is the same hang: nothing
    // upstream completes the future, so the attempt sits with Reconnection stuck in
    // ATTEMPT_IN_PROGRESS and no further attempt is ever scheduled.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();
    Error failure = new NoClassDefFoundError("com/example/CustomEndPointSupport");

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new ThrowingSpreadEndPoint(failure),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE,
            true);

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

  /**
   * A {@link PinnableEndPoint} whose {@code addressesAreInterchangeable()} throws, standing in for
   * any implementation-supplied override that can fail -- {@code ClientRoutesEndPoint}'s reaches
   * the topology monitor and catches only {@link IllegalStateException}.
   */
  private static class ThrowingSpreadEndPoint implements PinnableEndPoint {

    private final Throwable failure;

    ThrowingSpreadEndPoint(Throwable failure) {
      this.failure = failure;
    }

    @NonNull
    @Override
    public SocketAddress resolve() {
      return InetSocketAddress.createUnresolved("test.cluster.fake", 9042);
    }

    @Override
    public boolean addressesAreInterchangeable(@NonNull SocketAddress resolvedAddress) {
      if (failure instanceof Error) {
        throw (Error) failure;
      }
      throw (RuntimeException) failure;
    }

    @NonNull
    @Override
    public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
      return this;
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
