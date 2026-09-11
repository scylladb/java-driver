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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.local.LocalAddress;
import io.netty.resolver.AddressResolverGroup;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * {@link ChannelFactory#resolveAll(SocketAddress)}: the resolver the bootstrap hook installed is
 * the one consulted, and Netty's own short-circuits are mirrored.
 */
public class ChannelFactoryResolveAllTest extends ChannelFactoryTestBase {

  private static final InetSocketAddress HOSTNAME =
      InetSocketAddress.createUnresolved("cluster.example.com", 9042);

  @Test
  public void should_return_every_address_a_custom_resolver_answers_with() {
    // Given
    List<SocketAddress> answer =
        ImmutableList.of(new LocalAddress("a"), new LocalAddress("b"), new LocalAddress("c"));
    TestAddressResolverGroup group = installResolver(new TestAddressResolverGroup(answer));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then
    assertThatStage(stage)
        .isSuccess(resolved -> assertThat(resolved).containsExactlyElementsOf(answer));
    assertThat(group.queried).containsExactly(HOSTNAME);
    assertThat(group.resolverExecutor).isNotNull();
    assertThat(group.resolverExecutor.parent()).isSameAs(clientGroup);
  }

  @Test
  public void should_resolve_on_an_io_loop_and_not_on_the_calling_thread() {
    // Given
    TestAddressResolverGroup group =
        installResolver(new TestAddressResolverGroup(ImmutableList.of(new LocalAddress("a"))));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then -- AddressResolver#resolveAll resolves inline and the default resolver blocks on the
    // JDK lookup, so the thread it runs on must not be the caller's: ControlConnection calls this
    // from the admin executor.
    assertThatStage(stage).isSuccess(resolved -> assertThat(resolved).hasSize(1));
    Thread resolvingThread = group.resolvingThread;
    assertThat(resolvingThread).isNotNull().isNotSameAs(Thread.currentThread());
    assertThat(group.resolverExecutor).isNotNull();
    assertThat(group.resolverExecutor.inEventLoop(resolvingThread)).isTrue();
  }

  @Test
  public void should_complete_the_stage_when_the_resolver_answers_later() {
    // Given -- a resolver that leaves its promise pending, as an asynchronous one does
    List<SocketAddress> answer = ImmutableList.of(new LocalAddress("a"), new LocalAddress("b"));
    installResolver(new TestAddressResolverGroup(answer).deferred());
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then -- the answer arrives through the listener
    assertThatStage(stage)
        .isSuccess(resolved -> assertThat(resolved).containsExactlyElementsOf(answer));
  }

  @Test
  public void should_return_an_empty_list_when_the_resolver_answers_with_nothing() {
    // Given
    installResolver(new TestAddressResolverGroup(ImmutableList.of()));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then -- not the input: what an empty answer means is the caller's to decide
    assertThatStage(stage).isSuccess(resolved -> assertThat(resolved).isEmpty());
  }

  @Test
  public void should_ask_the_resolver_even_about_an_address_that_carries_an_ip() {
    // Given -- a resolver that redirects everything, which Netty lets it do: doResolveAndConnect0
    // asks the resolver rather than testing the address itself
    List<SocketAddress> answer = ImmutableList.of(new LocalAddress("redirected"));
    TestAddressResolverGroup group = installResolver(new TestAddressResolverGroup(answer, true));
    InetSocketAddress resolvedInput = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(resolvedInput);

    // Then
    assertThatStage(stage)
        .isSuccess(resolved -> assertThat(resolved).containsExactlyElementsOf(answer));
    assertThat(group.queried).containsExactly(resolvedInput);
  }

  @Test
  public void should_run_the_bootstrap_hook_once_for_repeated_lookups() {
    // Given
    TestAddressResolverGroup group =
        installResolver(new TestAddressResolverGroup(ImmutableList.of(new LocalAddress("a"))));
    ChannelFactory factory = newChannelFactory();

    // When
    assertThatStage(factory.resolveAll(HOSTNAME)).isSuccess();
    assertThatStage(factory.resolveAll(HOSTNAME)).isSuccess();

    // Then -- the hook is user code and may build the group it installs; asking it per lookup
    // would mint a resolver, a socket and an empty DNS cache every time
    verify(nettyOptions, times(1)).afterBootstrapInitialized(any(Bootstrap.class));
    assertThat(group.queried).hasSize(2);
  }

  @Test
  public void should_fail_the_stage_when_the_io_loop_no_longer_accepts_tasks() throws Exception {
    // Given
    installResolver(new TestAddressResolverGroup(ImmutableList.of(new LocalAddress("a"))));
    ChannelFactory factory = newChannelFactory();
    clientGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();

    // When -- a session closing while a reconnection round is in flight
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then -- reported through the stage, never thrown at the caller
    assertThatStage(stage)
        .isFailed(error -> assertThat(error).isInstanceOf(RejectedExecutionException.class));
  }

  @Test
  public void should_return_the_address_as_is_when_the_resolver_is_disabled() {
    // Given
    doAnswer(
            invocation -> {
              invocation.<Bootstrap>getArgument(0).disableResolver();
              return null;
            })
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then -- unresolved, as Netty would hand it to connect()
    assertThatStage(stage).isSuccess(resolved -> assertThat(resolved).containsExactly(HOSTNAME));
  }

  @Test
  public void should_return_the_address_as_is_when_the_resolver_reports_it_resolved() {
    // Given
    TestAddressResolverGroup group =
        installResolver(new TestAddressResolverGroup(ImmutableList.of(new LocalAddress("unused"))));
    InetSocketAddress resolvedInput = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(resolvedInput);

    // Then -- the resolver was asked whether, not what
    assertThatStage(stage)
        .isSuccess(resolved -> assertThat(resolved).containsExactly(resolvedInput));
    assertThat(group.queried).isEmpty();
  }

  @Test
  public void should_return_the_address_as_is_when_the_resolver_does_not_support_it() {
    // Given -- Netty's default resolver handles InetSocketAddress only; a local-transport address
    // is not one
    LocalAddress local = new LocalAddress("local");
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(local);

    // Then
    assertThatStage(stage).isSuccess(resolved -> assertThat(resolved).containsExactly(local));
  }

  @Test
  public void should_resolve_a_hostname_through_the_default_resolver() {
    // Given -- no hook: Netty's default (JDK-backed) resolver
    InetSocketAddress localhost = InetSocketAddress.createUnresolved("localhost", 9042);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(localhost);

    // Then -- every answer is resolved and keeps the name it was resolved from
    assertThatStage(stage)
        .isSuccess(
            resolved -> {
              assertThat(resolved).isNotEmpty();
              for (SocketAddress address : resolved) {
                assertThat(address).isInstanceOf(InetSocketAddress.class);
                InetSocketAddress inet = (InetSocketAddress) address;
                assertThat(inet.isUnresolved()).isFalse();
                assertThat(inet.getHostString()).isEqualTo("localhost");
                assertThat(inet.getPort()).isEqualTo(9042);
              }
            });
  }

  @Test
  public void should_fail_the_stage_when_the_resolver_fails() {
    // Given
    installResolver(new TestAddressResolverGroup(null));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then
    assertThatStage(stage)
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("mock resolver failure"));
  }

  @Test
  public void should_fail_the_stage_rather_than_throw_when_the_bootstrap_hook_throws() {
    // Given -- the hook is user code
    doThrow(new IllegalStateException("hook failure"))
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<List<SocketAddress>> stage = factory.resolveAll(HOSTNAME);

    // Then
    assertThatStage(stage)
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("hook failure"));
  }

  /** Installs {@code group} the way a user would: through the bootstrap hook. */
  private <T extends AddressResolverGroup<?>> T installResolver(T group) {
    doAnswer(
            invocation -> {
              invocation.<Bootstrap>getArgument(0).resolver(group);
              return null;
            })
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    return group;
  }
}
