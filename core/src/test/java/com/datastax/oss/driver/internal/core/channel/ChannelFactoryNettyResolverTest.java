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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalAddress;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Verifies that {@link ChannelFactory} expands unresolved candidate addresses through Netty's
 * configured {@link AddressResolverGroup}, rather than doing its own JVM DNS lookup.
 *
 * <p>This is what keeps a custom resolver installed via {@link
 * com.datastax.oss.driver.internal.core.context.NettyOptions#afterBootstrapInitialized(Bootstrap)}
 * effective: before multi-address support, an unresolved address was handed straight to {@code
 * Bootstrap.connect()} and Netty's resolver expanded it, so resolving anywhere else would silently
 * bypass the user's configuration.
 */
public class ChannelFactoryNettyResolverTest extends ChannelFactoryTestBase {

  // A local address that no server is bound to: connecting to it fails immediately.
  private static final SocketAddress UNREACHABLE =
      new LocalAddress(ChannelFactoryNettyResolverTest.class.getSimpleName() + "-unreachable");

  /** The hostname the endpoint reports, and that only the custom resolver knows how to expand. */
  private static final InetSocketAddress HOSTNAME =
      InetSocketAddress.createUnresolved("test.cluster.fake", 9042);

  /** What a resolver must never hand back from {@code resolveAll}, but might. */
  private static final InetSocketAddress STILL_UNRESOLVED =
      InetSocketAddress.createUnresolved("still.unresolved.fake", 9042);

  @Test
  public void should_expand_unresolved_address_through_the_custom_netty_resolver() {
    // Given – a resolver that maps the hostname to an unreachable address followed by the running
    // local server, mimicking a DNS round-robin entry whose first record is dead.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    TestAddressResolverGroup resolverGroup =
        new TestAddressResolverGroup(Arrays.asList(UNREACHABLE, SERVER_ADDRESS.resolve()));
    installResolver(resolverGroup);
    ChannelFactory factory = newChannelFactory();

    // When – the endpoint itself performs no resolution at all; it just yields the hostname.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    // The handshake only happens once we fall back to the reachable second address.
    completeSimpleChannelInit();

    // Then – the custom resolver was consulted for the hostname, and *all* the addresses it
    // returned
    // were tried, so the connection survived the dead first record.
    assertThatStage(channelFuture).isSuccess();
    assertThat(resolverGroup.queried)
        .as("the custom Netty resolver must be the one expanding the hostname")
        .containsExactly(HOSTNAME);
  }

  @Test
  public void should_fail_when_the_custom_resolver_cannot_resolve_the_only_candidate() {
    // Given – a resolver that fails every lookup.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    TestAddressResolverGroup resolverGroup = new TestAddressResolverGroup(null);
    installResolver(resolverGroup);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then – no candidate survived resolution, so the connect fails with the resolver's own cause
    // rather than, say, an empty-candidate-list error.
    assertThatStage(channelFuture)
        .isFailed(e -> assertThat(e).hasMessageContaining("mock resolver failure"));
  }

  @Test
  public void should_fail_with_a_diagnosable_error_when_every_expanded_address_is_unresolved() {
    // Given – a resolver that "expands" the hostname to another unresolved address. A redirecting
    // resolver can do this by rewriting the host without resolving it, and nothing downstream will
    // resolve it either: connectToAddress() uses a bootstrap clone with disableResolver(), so Netty
    // would raise UnresolvedAddressException from inside doConnect, naming neither the address nor
    // the reason nothing resolved it -- for every connect of the whole session.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    installResolver(new TestAddressResolverGroup(Collections.singletonList(STILL_UNRESOLVED)));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then – the failure says which endpoint and which resolver produced it, as the pass-through
    // paths already did (see ChannelFactory#unusableWithoutResolution).
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e).isInstanceOf(IllegalStateException.class);
              assertThat(e.getMessage()).contains("test.cluster.fake");
              assertThat(e.getMessage()).contains("TestAddressResolverGroup");
              assertThat(e.getMessage()).contains("unresolved");
            });
  }

  @Test
  public void should_drop_an_unresolved_expanded_address_before_applying_the_cap() {
    // Given – the same resolver answering with one unusable address and one live one, and a cap of
    // a
    // single candidate. An address that cannot be connected to must not consume a slot in that cap:
    // dropped after the truncation instead, it would leave this connect with nothing to dial.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES))
        .thenReturn(1);
    installResolver(
        new TestAddressResolverGroup(Arrays.asList(STILL_UNRESOLVED, SERVER_ADDRESS.resolve())));
    ChannelFactory factory = newChannelFactory();
    // Keeping the resolver's order is what makes this an assertion about the cap rather than about
    // luck: the unusable address is the one the truncation would otherwise have kept.
    factory.random = new KeepResolverOrder();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    // Then
    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_not_resolve_at_all_when_the_user_disabled_the_resolver() {
    // Given – Bootstrap.disableResolver() means config().resolver() is null. ChannelFactory must
    // treat that as "pass the candidates through" instead of dereferencing the missing group.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    TestAddressResolverGroup resolverGroup =
        new TestAddressResolverGroup(Collections.singletonList(UNREACHABLE));
    doAnswer(
            invocation -> {
              Bootstrap bootstrap = invocation.getArgument(0);
              bootstrap.resolver(resolverGroup).disableResolver();
              return null;
            })
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    ChannelFactory factory = newChannelFactory();

    // When – the endpoint yields an already-usable address.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    // Then – connection succeeds and the resolver was never even instantiated, let alone consulted.
    assertThatStage(channelFuture).isSuccess();
    assertThat(resolverGroup.resolverRequested).isFalse();
    assertThat(resolverGroup.queried).isEmpty();
  }

  @Test
  public void should_pass_already_resolved_address_through_untouched() {
    // Given – an endpoint whose address is already resolved, which is the common case: metadata
    // nodes hold resolved addresses from the peers rows, so this is every pool refill and every
    // reconnect. A resolver with the usual semantics reports it as resolved and there is nothing
    // to expand.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    TestAddressResolverGroup resolverGroup =
        new TestAddressResolverGroup(Collections.singletonList(UNREACHABLE));
    installResolver(resolverGroup);
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

    // Then – no lookup was performed: had one been, it would have redirected us to UNREACHABLE and
    // the connection would have failed. The decision was the resolver's own, though -- see
    // should_let_the_resolver_redirect_an_already_resolved_address.
    assertThatStage(channelFuture).isSuccess();
    assertThat(resolverGroup.queried).isEmpty();
    assertThat(resolverGroup.resolverRequested)
        .as("whether an address needs resolving must be the resolver's decision")
        .isTrue();
  }

  @Test
  public void should_let_the_resolver_redirect_an_already_resolved_address() {
    // Given – a resolver that reports even an address carrying an IP as still needing resolution,
    // and redirects it. Netty consulted the resolver for every connect, resolved address or not
    // (Bootstrap#doResolveAndConnect0 calls isSupported()/isResolved() on it rather than testing
    // the address itself), so short-circuiting on InetSocketAddress#isUnresolved() here would take
    // that away for every connect to an already-resolved node -- which is nearly all of them.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    TestAddressResolverGroup resolverGroup =
        new TestAddressResolverGroup(
            Collections.singletonList(SERVER_ADDRESS.resolve()),
            /* claimNothingIsResolved = */ true);
    installResolver(resolverGroup);
    ChannelFactory factory = newChannelFactory();

    // When – the endpoint holds a resolved address that nothing is listening on.
    InetSocketAddress resolved = new InetSocketAddress("127.0.0.1", 9042);
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(resolved),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    // Then – the connect landed on the address the resolver substituted, which it could only do by
    // having been asked about an address that already carried an IP. Exactly one lookup: the
    // per-attempt bootstrap has the resolver disabled, so the substitute is connected to as-is
    // rather than being handed back to the resolver (see the next test for why that matters).
    assertThatStage(channelFuture).isSuccess();
    assertThat(resolverGroup.queried)
        .as("the resolver must get a say on an address that already carries an IP")
        .containsExactly(resolved);
  }

  @Test
  public void should_try_every_candidate_when_the_resolver_redirects() {
    // Given – the same redirecting resolver as above, but answering with more than one address:
    // a dead one first, then the running local server.
    //
    // The per-attempt bootstrap must not re-resolve. Bootstrap.clone() carries the resolver
    // configuration over, and Netty's own pass calls resolve() -- *singular* -- so with a resolver
    // that reports resolved addresses as unresolved, every candidate would be redirected again onto
    // the resolver's first answer: the dead address, N times over. Multi-address fallback would
    // silently do nothing, and the endpoint pinned onto the channel would name an address the
    // channel is not connected to -- which is what the SSL engine's peer host and
    // DefaultTopologyMonitor#savePort are then derived from.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    TestAddressResolverGroup resolverGroup =
        new TestAddressResolverGroup(
            Arrays.asList(UNREACHABLE, SERVER_ADDRESS.resolve()),
            /* claimNothingIsResolved = */ true);
    installResolver(resolverGroup);
    ChannelFactory factory = newChannelFactory();

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    // Then – the reachable address was actually reached. This holds whichever candidate rotate()
    // starts from, and it is precisely what fails when the clone re-resolves: the dead address is
    // the resolver's first answer, so both attempts would land there and the connect would fail.
    assertThatStage(channelFuture).isSuccess();
    assertThat(resolverGroup.queried)
        .as("the hostname is expanded once, by ChannelFactory; the candidates are not re-resolved")
        .containsExactly(HOSTNAME);
  }

  @Test
  public void should_resolve_and_connect_on_the_same_event_loop() throws InterruptedException {
    // Resolution and channel registration must share the loop picked once per connect. Taking one
    // loop for resolution and letting the registration pick another would advance the group's
    // round-robin chooser twice per connect, parking every channel on half the loops with the
    // default power-of-two chooser. The base's single-thread group would make this assertion
    // vacuous, so use two loops -- on which the split behavior was deterministic.
    DefaultEventLoopGroup twoLoops = new DefaultEventLoopGroup(2);
    try {
      when(nettyOptions.ioEventLoopGroup()).thenReturn(twoLoops);
      when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
      when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
      TestAddressResolverGroup resolverGroup =
          new TestAddressResolverGroup(Collections.singletonList(SERVER_ADDRESS.resolve()));
      installResolver(resolverGroup);
      ChannelFactory factory = newChannelFactory();

      CompletionStage<DriverChannel> channelFuture =
          factory.connect(
              new DefaultEndPoint(HOSTNAME),
              null,
              null,
              DriverChannelOptions.DEFAULT,
              NoopNodeMetricUpdater.INSTANCE);
      completeSimpleChannelInit();

      assertThatStage(channelFuture)
          .isSuccess(
              channel ->
                  assertThat((Object) channel.eventLoop())
                      .as("the channel must be registered on the loop resolution ran on")
                      .isSameAs(resolverGroup.resolverExecutor));
    } finally {
      twoLoops.shutdownGracefully(0, 100, TimeUnit.MILLISECONDS).sync();
    }
  }

  @Test
  public void should_fail_future_when_resolver_throws_synchronously() {
    // Given – a broken custom resolver that throws instead of returning a failed future. The throw
    // happens inside an event-loop task, where nothing else would ever complete the connect future:
    // nothing at this stage has a timeout, so before the blanket catch in resolveCandidates() this
    // hung the connect attempt (and with it control-connection init) forever.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    RuntimeException failure = new IllegalStateException("broken resolver");
    installResolver(new ThrowingAddressResolverGroup(failure));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then
    assertThatStage(channelFuture).isFailed(e -> assertThat(e).isSameAs(failure));
  }

  /** A resolver whose every method throws, standing in for a broken third-party implementation. */
  private static class ThrowingAddressResolverGroup extends AddressResolverGroup<SocketAddress> {

    private final RuntimeException failure;

    ThrowingAddressResolverGroup(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    protected AddressResolver<SocketAddress> newResolver(EventExecutor executor) {
      return new AddressResolver<SocketAddress>() {

        @Override
        public boolean isSupported(SocketAddress address) {
          throw failure;
        }

        @Override
        public boolean isResolved(SocketAddress address) {
          throw failure;
        }

        @Override
        public Future<SocketAddress> resolve(SocketAddress address) {
          throw failure;
        }

        @Override
        public Future<SocketAddress> resolve(
            SocketAddress address, Promise<SocketAddress> promise) {
          throw failure;
        }

        @Override
        public Future<List<SocketAddress>> resolveAll(SocketAddress address) {
          throw failure;
        }

        @Override
        public Future<List<SocketAddress>> resolveAll(
            SocketAddress address, Promise<List<SocketAddress>> promise) {
          throw failure;
        }

        @Override
        public void close() {
          // nothing to do
        }
      };
    }
  }
}
