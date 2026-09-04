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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.PinnableEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.local.LocalAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

/**
 * Verifies that a successfully connected {@link DriverChannel} carries an endpoint bound to the
 * address the connection actually used, not the multi-address original.
 *
 * <p>Without this, a hostname shared by several nodes would let a later reconnect land on a
 * different node while still being treated as the original {@code host_id}: {@code
 * DefaultTopologyMonitor#buildNodeEndPoint} stores the channel's endpoint for the control node, and
 * {@code ControlConnection} skips identity re-resolution for nodes that already have a host id. See
 * {@link PinnableEndPoint}.
 */
public class ChannelFactoryPinnedEndPointTest extends ChannelFactoryTestBase {

  // A local address that no server is bound to: connecting to it fails immediately.
  private static final SocketAddress UNREACHABLE =
      new LocalAddress(ChannelFactoryPinnedEndPointTest.class.getSimpleName() + "-unreachable");

  /** The name the endpoint reports, and that only the resolver knows how to expand. */
  private static final InetSocketAddress HOSTNAME =
      InetSocketAddress.createUnresolved("test.cluster.fake", 9042);

  @Test
  public void should_pin_channel_endpoint_to_the_address_that_connected() {
    // Given – an endpoint reporting a name, which the resolver expands to a dead address and the
    // running local server. Whichever of the two the connection ends up on, the channel must carry
    // that one.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    SocketAddress reachable = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(UNREACHABLE, reachable)));
    ChannelFactory factory = newChannelFactory();
    TestPinnableEndPoint endPoint = new TestPinnableEndPoint(HOSTNAME);

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            endPoint, null, null, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    // Then
    assertThatStage(channelFuture)
        .isSuccess(
            channel -> {
              EndPoint channelEndPoint = channel.getEndPoint();
              // The channel resolves to the address it is actually connected to -- the name it was
              // built from is gone from resolve(), which is what SSL engines and authenticators
              // need.
              assertThat(channelEndPoint.resolve()).isEqualTo(reachable);
              // ...while still denoting the same node, so node lookups and metric names are stable.
              assertThat(channelEndPoint).isEqualTo(endPoint);
              assertThat(channelEndPoint.asMetricPrefix()).isEqualTo(endPoint.asMetricPrefix());
            });
  }

  @Test
  public void should_leave_non_pinnable_endpoints_untouched() {
    // A third-party EndPoint that does not implement PinnableEndPoint must reach the channel
    // exactly
    // as it was given, so existing implementations keep working.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);
    completeSimpleChannelInit();

    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getEndPoint()).isSameAs(SERVER_ADDRESS));
  }

  @Test
  public void should_not_pin_to_an_unresolved_address() {
    // Given – a resolver that reports every address already resolved, which is what
    // NoopAddressResolverGroup does when something in the pipeline resolves the name instead.
    // resolveCandidates() honours the claim and hands the endpoint's own address straight back, so
    // for an endpoint that reports a name the candidate is still that name. This is the only path
    // that reaches pin() with an unresolved address: the other two pass-throughs materialize an IP
    // literal or fail, and resolveAll's results have the unresolved ones dropped.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    installResolver(TestAddressResolverGroup.claimingEverythingIsResolved());
    ChannelFactory factory = newChannelFactory();
    List<SocketAddress> pinnedTo = new ArrayList<>();
    TestPinnableEndPoint endPoint =
        new TestPinnableEndPoint(HOSTNAME) {
          @NonNull
          @Override
          public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
            pinnedTo.add(resolvedAddress);
            return super.pinTo(resolvedAddress);
          }
        };

    // When – the connect itself cannot succeed: these tests run over Netty's local transport, which
    // has no server bound to an InetSocketAddress. pin() runs before the attempt, so what it was
    // handed is settled either way.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            endPoint, null, null, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    // Then – pinTo() is documented to take an already-resolved address. Pinning a name would freeze
    // the endpoint on something that still re-expands on every connect, and for endpoints that stop
    // consulting their own source once pinned, silence that source for good.
    assertThatStage(channelFuture).isFailed();
    assertThat(pinnedTo).as("pinTo() must not be handed an unresolved address").isEmpty();
  }

  @Test
  public void should_fail_future_when_pin_to_throws() {
    // pinTo() runs in the continuation after resolution, whose exceptions CompletionStage
    // swallows; a throwing implementation must fail the connect future rather than hang it.
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    SocketAddress reachable = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Collections.singletonList(reachable)));
    ChannelFactory factory = newChannelFactory();
    RuntimeException failure = new IllegalStateException("pinTo blew up");
    TestPinnableEndPoint endPoint =
        new TestPinnableEndPoint(HOSTNAME) {
          @NonNull
          @Override
          public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
            throw failure;
          }
        };

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            endPoint, null, null, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    assertThatStage(channelFuture).isFailed(e -> assertThat(e).isSameAs(failure));
  }

  /**
   * A {@link PinnableEndPoint} that can hold a pin to any {@link SocketAddress}, including the
   * local-transport addresses these tests connect over (which {@code DefaultEndPoint} cannot).
   * Identity is the unpinned address, so a pinned copy stays equal to the original — the contract
   * {@link PinnableEndPoint} requires.
   */
  private static class TestPinnableEndPoint implements PinnableEndPoint {

    private final SocketAddress address;
    private final SocketAddress pinnedAddress;

    TestPinnableEndPoint(SocketAddress address) {
      this(address, null);
    }

    private TestPinnableEndPoint(SocketAddress address, SocketAddress pinnedAddress) {
      this.address = address;
      this.pinnedAddress = pinnedAddress;
    }

    @NonNull
    @Override
    public SocketAddress resolve() {
      return pinnedAddress != null ? pinnedAddress : address;
    }

    @NonNull
    @Override
    public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
      return new TestPinnableEndPoint(address, resolvedAddress);
    }

    @NonNull
    @Override
    public String asMetricPrefix() {
      return "test";
    }

    @Override
    public boolean equals(Object other) {
      return (other instanceof TestPinnableEndPoint)
          && address.equals(((TestPinnableEndPoint) other).address);
    }

    @Override
    public int hashCode() {
      return Objects.hash(address);
    }
  }
}
