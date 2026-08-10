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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import io.netty.channel.local.LocalAddress;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

/**
 * Verifies the two steps {@link ChannelFactory} runs between protocol initialization and the
 * completion of a candidate attempt: the caller's {@link ConnectHook}, and the REGISTER request
 * that moved out of the init handshake so that a channel the hook is about to reject never
 * registers for events.
 */
public class ChannelFactoryConnectHookTest extends ChannelFactoryTestBase {

  private static final Duration HOOK_TIMEOUT = Duration.ofSeconds(5);

  /** The name the endpoint reports, and that only the resolver knows how to expand. */
  private static final InetSocketAddress HOSTNAME =
      InetSocketAddress.createUnresolved("test.cluster.fake", 9042);

  /** A local address that no server is bound to: connecting to it fails immediately. */
  private static final SocketAddress UNREACHABLE =
      new LocalAddress(ChannelFactoryConnectHookTest.class.getSimpleName() + "-unreachable");

  private void givenNegotiableProtocol() {
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
  }

  /**
   * Drives one candidate's handshake to successful completion. Only the factory's first channel
   * sends OPTIONS, so later candidates start straight at STARTUP.
   */
  private void completeInit() {
    Frame requestFrame = readOutboundFrame();
    if (requestFrame.message instanceof Options) {
      writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));
      requestFrame = readOutboundFrame();
    }
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));
  }

  private static DriverChannelOptions optionsWithHook(ConnectHook hook) {
    return DriverChannelOptions.builder().withConnectHook(hook, HOOK_TIMEOUT).build();
  }

  private static DriverChannelOptions optionsWithHookAndEvents(ConnectHook hook) {
    return DriverChannelOptions.builder()
        .withConnectHook(hook, HOOK_TIMEOUT)
        .withEvents(ImmutableList.of("foo", "bar"), mock(EventCallback.class))
        .build();
  }

  @Test
  public void should_complete_candidate_only_after_hook_accepts() {
    // Given — a hook whose completion the test controls.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    CompletableFuture<Void> gate = new CompletableFuture<>();
    List<DriverChannel> vettedChannels = new CopyOnWriteArrayList<>();
    ConnectHook hook =
        channel -> {
          vettedChannels.add(channel);
          return gate;
        };

    // When — init completes but the hook has not answered yet.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, null, null, optionsWithHook(hook), NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then — the attempt is not successful until the hook says so. (The hook runs on the event
    // loop after init succeeds, so wait for the invocation before asserting on the future.)
    await().atMost(java.time.Duration.ofSeconds(2)).until(() -> vettedChannels.size() == 1);
    assertThat(channelFuture.toCompletableFuture()).isNotDone();

    // When
    gate.complete(null);

    // Then — the vetted channel is the one handed to the caller.
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel).isSameAs(vettedChannels.get(0)));
  }

  @Test
  public void should_try_next_address_when_hook_rejects_candidate() {
    // Given — a name expanding to two addresses of the live server, and a hook that rejects the
    // first candidate and accepts the second: the caller's acceptance criteria are per address,
    // and a rejection must not write off the endpoint.
    givenNegotiableProtocol();
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();
    AtomicInteger invocations = new AtomicInteger();
    ConnectHook hook =
        channel -> {
          CompletableFuture<Void> result = new CompletableFuture<>();
          if (invocations.incrementAndGet() == 1) {
            result.completeExceptionally(new IllegalStateException("not this one"));
          } else {
            result.complete(null);
          }
          return result;
        };

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            optionsWithHook(hook),
            NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    completeInit();

    // Then
    assertThatStage(channelFuture).isSuccess();
    assertThat(invocations.get()).isEqualTo(2);
  }

  @Test
  public void should_fail_connect_when_hook_rejects_the_last_candidate() {
    // Given — a single address, so the rejection has nowhere to advance to.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    IllegalStateException rejection = new IllegalStateException("cannot identify itself");
    ConnectHook hook =
        channel -> {
          CompletableFuture<Void> result = new CompletableFuture<>();
          result.completeExceptionally(rejection);
          return result;
        };

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, null, null, optionsWithHook(hook), NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then — the rejection's cause is preserved for diagnosis.
    assertThatStage(channelFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(ConnectionInitException.class);
              assertThat(error.getCause()).isSameAs(rejection);
            });
  }

  @Test
  public void should_treat_synchronous_hook_throw_as_rejection() {
    // A hook is a caller-supplied callback running inside a Netty listener: a leaked throwable
    // would otherwise leave the attempt hanging forever.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    IllegalStateException thrown = new IllegalStateException("hook blew up");
    ConnectHook hook =
        channel -> {
          throw thrown;
        };

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, null, null, optionsWithHook(hook), NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    assertThatStage(channelFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(ConnectionInitException.class);
              assertThat(error.getCause()).isSameAs(thrown);
            });
  }

  @Test
  public void should_reject_candidate_when_hook_times_out() {
    // Given — a hook whose stage never completes; only the driver can bound that.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    ConnectHook hook = channel -> new CompletableFuture<>();
    DriverChannelOptions options =
        DriverChannelOptions.builder().withConnectHook(hook, Duration.ofMillis(100)).build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then
    assertThatStage(channelFuture)
        .isFailed(error -> assertThat(error).hasMessageContaining("timed out"));
  }

  @Test
  public void should_arm_the_hook_timeout_on_neither_thread_the_connect_uses() {
    // The wedge the backstop exists for, and the only one it can catch. A hook that hands back a
    // stage and never completes it is bounded by any timer at all; a hook that *blocks* is not, and
    // blocking is exactly what TopologyMonitor#getChannelNodeInfo's contract has to ask
    // implementations not to do, because nothing enforces it.
    //
    // So it must not be armed on either thread this connect already leans on. Not the channel's
    // event loop: the hook runs there -- finishCandidate is reached from a channel-promise
    // listener, which Netty notifies on it -- so a task armed on that loop is queued behind the
    // very block it was meant to interrupt. And not the admin group either: #connectToAddress
    // dispatches the blocking shard-aware port scan there, once per candidate address, on a
    // two-thread group one of whose threads is the control connection's own executor.
    //
    // Which leaves the driver's timer, and where the task goes is the whole property -- so that is
    // what this asserts, from both ends. Blocking a loop and racing the deadline instead was the
    // first version of this test, and it failed on a slow CI runner rather than on a regression:
    // the assertion budget is wall-clock, and a hook blocking a shared runner's core is the last
    // thing to add to that. The timeout here is long enough that it never fires.
    givenNegotiableProtocol();
    Timer recordingTimer = mock(Timer.class);
    when(recordingTimer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation ->
                timer.newTimeout(
                    invocation.getArgument(0),
                    (long) invocation.getArgument(1),
                    invocation.getArgument(2)));
    when(nettyOptions.getTimer()).thenReturn(recordingTimer);
    EventExecutor recordingExecutor = mock(EventExecutor.class);
    EventExecutorGroup recordingGroup = mock(EventExecutorGroup.class);
    when(recordingGroup.next()).thenReturn(recordingExecutor);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(recordingGroup);
    ChannelFactory factory = newChannelFactory();
    CompletableFuture<Void> gate = new CompletableFuture<>();
    ConnectHook hook = channel -> gate;
    DriverChannelOptions options =
        DriverChannelOptions.builder().withConnectHook(hook, Duration.ofMinutes(5)).build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then -- on the timer, not on the admin group, and the attempt is otherwise untouched.
    verify(recordingTimer, timeout(5000))
        .newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
    verify(recordingExecutor, never())
        .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    gate.complete(null);
    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_arm_the_hook_timeout_before_calling_the_hook() {
    // Where the backstop is armed only matters if it is armed at all, and a hook that blocks
    // inside onConnect never lets the arming statement run: the call does not return, so no timer
    // exists on any thread and the wedge the previous test describes is bounded by nothing. Which
    // thread the task lands on is only half the property; this is the other half.
    //
    // Asserted as an order rather than by blocking a loop and waiting for a deadline. The version
    // of this test that blocked a Netty loop for four seconds raced a wall-clock assertion budget
    // and failed on a slow CI runner instead of on a regression, so it was removed; the ordering
    // is the actual invariant and needs no clock at all.
    givenNegotiableProtocol();
    AtomicBoolean armed = new AtomicBoolean();
    AtomicBoolean armedBeforeTheHookRan = new AtomicBoolean();
    Timer recordingTimer = mock(Timer.class);
    when(recordingTimer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation -> {
              armed.set(true);
              return timer.newTimeout(
                  invocation.getArgument(0),
                  (long) invocation.getArgument(1),
                  invocation.getArgument(2));
            });
    when(nettyOptions.getTimer()).thenReturn(recordingTimer);
    ChannelFactory factory = newChannelFactory();
    ConnectHook hook =
        channel -> {
          armedBeforeTheHookRan.set(armed.get());
          return CompletableFuture.completedFuture(null);
        };
    DriverChannelOptions options =
        DriverChannelOptions.builder().withConnectHook(hook, Duration.ofMinutes(5)).build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then the hook saw a timeout already armed, and accepting still works.
    assertThatStage(channelFuture).isSuccess();
    assertThat(armedBeforeTheHookRan).isTrue();
  }

  @Test
  public void should_register_for_events_only_after_hook_accepts() {
    // Given — events requested and a gated hook.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    CompletableFuture<Void> gate = new CompletableFuture<>();
    ConnectHook hook = channel -> gate;

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            optionsWithHookAndEvents(hook),
            NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    // The hook has not accepted yet: were REGISTER part of init, it would already be on the wire.
    assertThat(tryReadOutboundFrame(200)).isNull();
    gate.complete(null);

    // Then — REGISTER goes out only now, and the attempt completes once it is acknowledged.
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    assertThat(((Register) registerFrame.message).eventTypes).containsExactly("foo", "bar");
    writeInboundFrame(registerFrame, new Ready());
    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_fail_candidate_when_the_step_after_the_hook_throws()
      throws InterruptedException {
    // Given — events requested, and a registration step that throws once the hook has accepted.
    // registerForEvents() runs inside the hook stage's whenComplete callback: nobody consumes the
    // stage that callback returns, and the hook timeout that would have failed the candidate has
    // just been cancelled, so without a blanket catch there the attempt would hang forever.
    //
    // The thrower is the event-type list rather than a config read, because the init-query timeout
    // is now captured beside the pipeline instead of being read here (see
    // ChannelFactory#bootstrapAndConnect). What this pins is the catch, not any one way of
    // reaching it: the list is consulted first thing in registerForEvents, on the callback's
    // thread, and it answers the builder's own emptiness check before that.
    givenNegotiableProtocol();
    @SuppressWarnings("unchecked")
    List<String> poisonedEventTypes = mock(List.class);
    when(poisonedEventTypes.isEmpty())
        .thenReturn(false)
        .thenThrow(new IllegalStateException("event types went away"));
    ChannelFactory factory = newChannelFactory();
    AtomicReference<DriverChannel> vettedChannel = new AtomicReference<>();
    ConnectHook hook =
        channel -> {
          vettedChannel.set(channel);
          return CompletableFuture.completedFuture(null);
        };
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withConnectHook(hook, Duration.ofMinutes(5))
            .withEvents(poisonedEventTypes, mock(EventCallback.class))
            .build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then — the attempt fails instead of hanging, REGISTER never goes out, and the channel it had
    // already opened is closed rather than left dangling with nothing holding it.
    assertThatStage(channelFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(ConnectionInitException.class);
              assertThat(error.getCause()).hasMessageContaining("event types went away");
            });
    assertThat(tryReadOutboundFrame(200)).isNull();
    assertThat(vettedChannel.get().closeFuture().await(500, TimeUnit.MILLISECONDS))
        .as("the abandoned candidate's channel should have been closed")
        .isTrue();
  }

  @Test
  public void should_register_for_events_after_init_when_no_hook_is_set() {
    // Given — events but no hook (REGISTER still has to happen even when there is nothing to vet).
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(ImmutableList.of("foo", "bar"), mock(EventCallback.class))
            .build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(registerFrame, new Ready());
    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_bound_registration_with_the_timeout_captured_for_the_attempt() {
    // advanced.connection.init-query-timeout is documented as taking effect for connections
    // created after the change, and every init step honours that by using the value
    // ProtocolInitHandler snapshotted when the pipeline was built. REGISTER is sent after init now,
    // so reading the option again at that point would hand a connection that already exists a
    // value configured after it was created.
    //
    // At zero that is not just a scope violation. The two request classes disagree about a
    // non-positive timeout -- AdminRequestHandler#onWriteComplete arms no timer, while the
    // ChannelHandlerRequest the init steps use arms one unconditionally -- so a reload to zero
    // landing inside a handshake leaves STARTUP bounded by the old value and REGISTER bounded by
    // nothing. Here the server accepts the connection and then never answers REGISTER, which is
    // the shape that hangs: no hook is armed either, so nothing else is watching the attempt.
    givenNegotiableProtocol();
    when(defaultProfile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .thenReturn(Duration.ofMillis(200));
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(ImmutableList.of("foo", "bar"), mock(EventCallback.class))
            .build();

    // When -- the option is disabled once the handshake is done, i.e. inside the connect.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    when(defaultProfile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .thenReturn(Duration.ZERO);

    // Then REGISTER goes out and is bounded by the 200ms this attempt captured, not by the zero
    // that is now configured. No response is written for it.
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    assertThatStage(channelFuture)
        .isFailed(error -> assertThat(error).hasMessageContaining("timed out"));
  }

  @Test
  public void should_try_next_address_when_registration_fails() {
    // Given — two addresses; the first candidate's REGISTER is refused. A registration failure is
    // a per-candidate failure, exactly as it was when REGISTER was an init step.
    givenNegotiableProtocol();
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(ImmutableList.of("foo", "bar"), mock(EventCallback.class))
            .build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME), null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(registerFrame, new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, "nope"));

    // Then — the loop advances; the second candidate registers successfully.
    completeInit();
    registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(registerFrame, new Ready());
    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_treat_a_zero_hook_timeout_as_unbounded() {
    // The hook timeout comes from advanced.control-connection.timeout, and every other consumer of
    // a
    // driver timeout option reads a non-positive duration as "no timeout" (see
    // AdminRequestHandler#onWriteComplete). Scheduled anyway, a zero delay fires on the next
    // event-loop turn -- before any round trip can complete -- and would abandon every candidate of
    // every contact point, so an operator who disabled that timeout could not initialize a session.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    CompletableFuture<Void> gate = new CompletableFuture<>();
    ConnectHook hook = channel -> gate;
    DriverChannelOptions options =
        DriverChannelOptions.builder().withConnectHook(hook, Duration.ZERO).build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then — the hook is given as long as it needs, and the candidate survives.
    assertThat(tryReadOutboundFrame(200)).isNull();
    assertThat(channelFuture.toCompletableFuture()).isNotDone();
    gate.complete(null);

    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_not_latch_negotiated_state_from_a_candidate_the_hook_rejects() {
    // The protocol version and cluster name used to be latched as soon as the transport connect and
    // init handshake succeeded, which was safe while init had the last word on a candidate. It no
    // longer does: this hook rejects one, and REGISTER (below) can too. A stale DNS record pointing
    // at a foreign cluster would otherwise leave that cluster's name latched here, and every later
    // connection -- to any node -- would fail its cluster-name check, which ChannelPool turns into
    // an irreversible forced-down node.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    ConnectHook hook =
        channel -> {
          CompletableFuture<Void> rejected = new CompletableFuture<>();
          rejected.completeExceptionally(new IllegalStateException("no host_id"));
          return rejected;
        };

    // When — the only candidate is rejected after a fully successful handshake.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, null, null, optionsWithHook(hook), NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    // Then
    assertThatStage(channelFuture)
        .isFailed(error -> assertThat(error).hasMessageContaining("hook"));
    assertThat(factory.protocolVersion).isNull();
    assertThat(factory.getClusterName()).isNull();
  }

  @Test
  public void should_not_latch_negotiated_state_from_a_candidate_whose_registration_fails() {
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(ImmutableList.of("foo", "bar"), mock(EventCallback.class))
            .build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(registerFrame, new Error(ProtocolConstants.ErrorCode.SERVER_ERROR, "nope"));

    // Then
    assertThatStage(channelFuture).isFailed();
    assertThat(factory.protocolVersion).isNull();
    assertThat(factory.getClusterName()).isNull();
  }

  @Test
  public void should_not_latch_negotiated_state_from_a_candidate_the_timeout_abandoned() {
    // The third way a candidate is thrown away, and the one no ordering alone protects against: the
    // hook timeout and the hook's own success race. cancel(false) can lose to a timeout task that
    // has already started running -- abandonCandidate documents exactly that -- and the candidate
    // then walks on through to completeCandidate with its future already failed.
    //
    // It must not latch on the way past. What decides that is the one-shot settle on the
    // candidate's
    // future, not the order of the two statements inside completeCandidate: the accepted candidate
    // has to latch *before* it publishes, since complete() releases callers on other threads
    // synchronously, so "latch only if we then win" is not available.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    CompletableFuture<Void> gate = new CompletableFuture<>();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withConnectHook(channel -> gate, Duration.ofMillis(100))
            .build();

    // When -- the handshake succeeds, then the timeout fires while the hook is still pending.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    assertThatStage(channelFuture)
        .isFailed(error -> assertThat(error).hasMessageContaining("timed out"));

    // And only now does the hook accept, sending an already-abandoned candidate on to
    // completeCandidate -- with no event types requested, straight there and with no REGISTER in
    // between, which is what makes this reachable rather than hypothetical.
    gate.complete(null);

    // Then -- nothing of that candidate's handshake survives it.
    assertThat(factory.protocolVersion).isNull();
    assertThat(factory.getClusterName()).isNull();
  }

  @Test
  public void should_fail_the_candidate_when_recording_the_negotiated_state_throws() {
    // Winning the settle makes completeCandidate the only call that can still complete the future:
    // every blanket catch downstream discharges that duty through abandonCandidate, and that is a
    // no-op once the candidate is settled. A throw out of onAccepted would therefore strand the
    // attempt -- never completed, channel never closed, Reconnection stuck in ATTEMPT_IN_PROGRESS,
    // and nothing left to time it out, since REGISTER is done and the hook timeout is cancelled.
    //
    // latchNegotiatedState is not throw-free: on the Cloud path it reaches
    // TypesafeDriverConfig#overrideDefaults, which re-parses the whole configuration.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    TypesafeDriverConfig typesafeConfig = mock(TypesafeDriverConfig.class);
    when(typesafeConfig.getDefaultProfile()).thenReturn(defaultProfile);
    doThrow(new IllegalArgumentException("bad reload"))
        .when(typesafeConfig)
        .overrideDefaults(anyMap());
    when(context.getConfig()).thenReturn(typesafeConfig);

    // A hook and no event types: completeCandidate is then reached from the hook stage's
    // whenComplete, whose catch calls abandonCandidate -- the path that would hang.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            optionsWithHook(channel -> CompletableFuture.completedFuture(null)),
            NoopNodeMetricUpdater.INSTANCE);

    // The server advertises the Cloud product type, which is what takes latchNegotiatedState into
    // the branch that throws.
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(
        requestFrame, TestResponses.supportedResponse("PRODUCT_TYPE", "DATASTAX_APOLLO"));
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then -- failed, which is the point: isFailed() waits two seconds and reports a timeout
    // rather than blocking, so a hang here shows up as a failure and not as a stuck build.
    assertThatStage(channelFuture).isFailed(error -> assertThat(error).isNotNull());
  }

  @Test
  public void should_latch_negotiated_state_once_a_candidate_is_accepted() {
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            optionsWithHook(channel -> CompletableFuture.completedFuture(null)),
            NoopNodeMetricUpdater.INSTANCE);
    completeInit();

    assertThatStage(channelFuture).isSuccess();
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V4);
    assertThat(factory.getClusterName()).isEqualTo("mockClusterName");
  }

  @Test
  public void should_stop_the_candidate_loop_when_the_event_type_rejection_speaks_for_them_all() {
    // Given — an identified node with two addresses, and a server that does not support the event
    // type being registered for. Every address of an identified node is that same node, so the
    // rejection describes all of them: replaying it can only fail the same way while paying a full
    // TCP connect plus the STARTUP/AUTH/cluster-name handshake for each. Stopping at the first
    // restores what this rejection cost while REGISTER was an init step, which was one failed
    // connect per node.
    givenNegotiableProtocol();
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(
                ImmutableList.of(ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE),
                mock(EventCallback.class))
            .build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME),
            null,
            null,
            options,
            NoopNodeMetricUpdater.INSTANCE,
            /* nodeIsIdentified = */ true);
    completeInit();
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(
        registerFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Unknown event type: " + ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE));

    // Then — the attempt is already failed, without the second address having been dialled. Compare
    // should_try_next_address_when_registration_fails, where the stage is still pending at this
    // point because the loop moved on.
    //
    // The frame check comes first, and it is the assertion that actually binds: surfacedFailure has
    // a rung of its own for an UnsupportedEventTypeException, so the type and the message below
    // come out whether the loop stopped or not. Draining before the stage assertion also matters on
    // regression -- an unread frame parks the server loop in Exchanger#exchange and hangs
    // tearDown()'s shutdownGracefully().sync() instead of failing here.
    assertThat(tryReadOutboundFrame(200))
        .as("second candidate must not be attempted after the event type was rejected")
        .isNull();
    assertThatStage(channelFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(ConnectionInitException.class);
              assertThat(error).hasMessageContaining("CLIENT_ROUTES_CHANGE");
              assertThat(error.getSuppressed())
                  .as("no other candidate should have been tried, so nothing to suppress")
                  .isEmpty();
            });
  }

  @Test
  public void should_try_next_address_when_only_the_first_server_lacks_the_event_type() {
    // The same rejection against an unidentified contact point on a plain multi-record name. Those
    // records may be distinct servers -- which is what a rolling upgrade looks like from the client
    // -- so the one that answered speaks only for itself, and writing the name off would skip the
    // upgraded node behind the second record. Only node identity, or an endpoint that says its
    // addresses are interchangeable, makes the rejection node-wide.
    givenNegotiableProtocol();
    SocketAddress serverAddress = SERVER_ADDRESS.resolve();
    installResolver(new TestAddressResolverGroup(Arrays.asList(serverAddress, serverAddress)));
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(
                ImmutableList.of(ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE),
                mock(EventCallback.class))
            .build();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME), null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(
        registerFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Unknown event type: " + ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE));

    // Then — the loop advances; the second address, running newer software, registers successfully.
    completeInit();
    registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(registerFrame, new Ready());
    assertThatStage(channelFuture).isSuccess();
  }

  @Test
  public void should_report_the_event_type_rejection_over_a_later_address_transport_failure() {
    // Having advanced past the rejection (the test above), the loop must still report it. The
    // address tried last is arbitrary -- one firewalled record and the failure the caller receives
    // is a bare connect timeout, with the only message that says what to do about the deployment
    // reachable through getSuppressed(). ClientRoutesTopologyMonitor#init() is the caller, and its
    // whole job is to say whether client routes are usable here.
    givenNegotiableProtocol();
    installResolver(
        new TestAddressResolverGroup(Arrays.asList(SERVER_ADDRESS.resolve(), UNREACHABLE)));
    ChannelFactory factory = newChannelFactory();
    // The order matters here, unlike in the test above, so the shuffle is pinned.
    factory.random = new KeepResolverOrder();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(
                ImmutableList.of(ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE),
                mock(EventCallback.class))
            .build();

    // When — the first address answers and rejects the event type, the second is unreachable.
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            new DefaultEndPoint(HOSTNAME), null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(
        registerFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Unknown event type: " + ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE));

    // Then — the rejection is what comes out, not the transport failure that happened to be last.
    assertThatStage(channelFuture)
        .isFailed(
            error -> {
              assertThat(error).hasMessageContaining("CLIENT_ROUTES_CHANGE");
              assertThat(error).hasMessageContaining("ScyllaDB Enterprise >= 2026.1");
            });
  }

  @Test
  public void should_translate_client_routes_register_rejection() {
    // The one REGISTER rejection with a known cause keeps its clear message, as it had when
    // REGISTER was an init step: the caller (ClientRoutesTopologyMonitor.init()) reports it
    // instead of silently degrading.
    givenNegotiableProtocol();
    ChannelFactory factory = newChannelFactory();
    DriverChannelOptions options =
        DriverChannelOptions.builder()
            .withEvents(
                ImmutableList.of(ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE),
                mock(EventCallback.class))
            .build();

    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, null, null, options, NoopNodeMetricUpdater.INSTANCE);
    completeInit();
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    writeInboundFrame(
        registerFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Unknown event type: " + ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE));

    assertThatStage(channelFuture)
        .isFailed(
            error -> {
              assertThat(error).isInstanceOf(ConnectionInitException.class);
              assertThat(error).hasMessageContaining("CLIENT_ROUTES_CHANGE");
              assertThat(error).hasMessageContaining("ScyllaDB Enterprise >= 2026.1");
            });
  }
}
