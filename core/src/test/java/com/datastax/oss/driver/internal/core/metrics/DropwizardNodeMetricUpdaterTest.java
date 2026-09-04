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
package com.datastax.oss.driver.internal.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

@RunWith(DataProviderRunner.class)
public class DropwizardNodeMetricUpdaterTest {

  @Test
  public void should_log_warning_when_provided_eviction_time_setting_is_too_low() {
    // given
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(AbstractMetricUpdater.class, Level.WARN);
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(DefaultNodeMetric.CQL_MESSAGES);
    Duration expireAfter = AbstractMetricUpdater.MIN_EXPIRE_AFTER.minusMinutes(1);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);

    DropwizardNodeMetricUpdater updater =
        new DropwizardNodeMetricUpdater(node, context, enabledMetrics, new MetricRegistry()) {
          @Override
          protected void initializeGauge(
              NodeMetric metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
            // do nothing
          }

          @Override
          protected void initializeCounter(NodeMetric metric, DriverExecutionProfile profile) {
            // do nothing
          }

          @Override
          protected void initializeHdrTimer(
              NodeMetric metric,
              DriverExecutionProfile profile,
              DriverOption highestLatency,
              DriverOption significantDigits,
              DriverOption interval) {
            // do nothing
          }
        };

    // then
    assertThat(updater.getExpireAfter()).isEqualTo(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
    assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
    assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
        .contains(
            String.format(
                "[prefix] Value too low for %s: %s. Forcing to %s instead.",
                DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER.getPath(),
                expireAfter,
                AbstractMetricUpdater.MIN_EXPIRE_AFTER));
  }

  @Test
  @UseDataProvider(value = "acceptableEvictionTimes")
  public void should_not_log_warning_when_provided_eviction_time_setting_is_acceptable(
      Duration expireAfter) {
    // given
    LoggerTest.LoggerSetup logger =
        LoggerTest.setupTestLogger(AbstractMetricUpdater.class, Level.WARN);
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    Set<NodeMetric> enabledMetrics = Collections.singleton(DefaultNodeMetric.CQL_MESSAGES);

    // when
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(expireAfter);

    DropwizardNodeMetricUpdater updater =
        new DropwizardNodeMetricUpdater(node, context, enabledMetrics, new MetricRegistry()) {
          @Override
          protected void initializeGauge(
              NodeMetric metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
            // do nothing
          }

          @Override
          protected void initializeCounter(NodeMetric metric, DriverExecutionProfile profile) {
            // do nothing
          }

          @Override
          protected void initializeHdrTimer(
              NodeMetric metric,
              DriverExecutionProfile profile,
              DriverOption highestLatency,
              DriverOption significantDigits,
              DriverOption interval) {
            // do nothing
          }
        };

    // then
    assertThat(updater.getExpireAfter()).isEqualTo(expireAfter);
    verify(logger.appender, timeout(500).times(0)).doAppend(logger.loggingEventCaptor.capture());
  }

  @Test
  public void should_adopt_a_pending_expiration_from_the_updater_it_replaces() {
    // given – two updaters for the same node, as DefaultNode.setEndPoint builds when an endpoint
    // change renames the metrics, and an expiration already armed on the one being replaced.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout pendingTimeout = mock(Timeout.class);
    Timeout adoptedTimeout = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(pendingTimeout, adoptedTimeout);
    // Still pending, i.e. the countdown had not run yet: that is what makes it worth carrying over.
    when(pendingTimeout.cancel()).thenReturn(true);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);
    previous.startMetricsExpirationTimeout();
    verify(timer).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));

    // when
    replacement.adoptExpirationFrom(previous);

    // then – the countdown is cancelled on the object nothing refers to any more and re-armed on
    // the
    // one whose metrics it will clear. Without this the expiration is simply lost: it is armed and
    // cancelled through node.getMetricUpdater(), so a node that is already down never produces
    // another DOWN event to arm the replacement, while the orphan timer still fires clearMetrics()
    // on names recomputed from whatever endpoint the node holds by then.
    verify(pendingTimeout).cancel();
    verify(timer, times(2)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));

    // and – there is nothing left to hand over a second time.
    replacement.adoptExpirationFrom(previous);
    verify(timer, times(2)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void should_re_arm_an_expiration_that_has_already_fired() throws Exception {
    // The countdown ran, so this node's metrics were cleared. If its endpoint then changes -- a new
    // address in the peers row, a client route published -- DefaultNode#setEndPoint rebuilds the
    // updater, and the new one's constructor eagerly re-registers the node's whole metric set. So
    // the series are back, for a node that is still down.
    //
    // Nothing else would arm a countdown for them: startMetricsExpirationTimeout()'s only other
    // caller is the metrics factory's DOWN/FORCED_DOWN/removed handler, and a node that is already
    // down produces no such event. Arming nothing here leaves the resurrected series to outlive the
    // node until it next comes up or is removed, which may be never.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout firedTimeout = mock(Timeout.class);
    Timeout adoptedTimeout = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(firedTimeout, adoptedTimeout);
    ArgumentCaptor<TimerTask> taskCaptor = ArgumentCaptor.forClass(TimerTask.class);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);
    previous.startMetricsExpirationTimeout();
    verify(timer).newTimeout(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
    // Run the countdown to completion, which is what leaves the updater with no pending timeout and
    // its metrics cleared -- the state the cancel() return value alone cannot distinguish from a
    // node that simply never had one armed.
    taskCaptor.getValue().run(firedTimeout);

    replacement.adoptExpirationFrom(previous);

    verify(timer, times(2)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void should_re_arm_an_expiration_that_is_still_running() throws Exception {
    // The gap between "fired" and "recorded as fired". Netty flips a HashedWheelTimeout to
    // ST_EXPIRED before it runs the task, and the task clears a whole metric set before it reaches
    // its compare-and-set -- so for that whole interval cancel() answers false while the reference
    // still holds the real timeout, exactly as it would for a timeout somebody else already
    // cancelled. Reading cancel() to decide whether there is a countdown to carry therefore drops
    // the one this method exists for, and the task's own compare-and-set then loses against the
    // getAndSet(null) the adoption just did -- so neither side arms anything, and a still-down
    // node keeps the metric set its replacement's constructor re-registered, for good.
    //
    // Non-null is the answer instead: the reference is cleared only when nothing is armed.
    //
    // This input used to be asserted the other way round, by a test reading a failing cancel() as
    // "already cancelled, nothing to carry". Netty returns false for ST_CANCELLED and ST_EXPIRED
    // alike, so cancel() cannot tell those apart -- and only one of them can be in the reference:
    // cancelMetricsExpirationTimeout() clears it *before* cancelling, and the arming accumulator
    // only ever cancels a candidate it declined to store. A non-null reference whose cancel() fails
    // is therefore an expiry in flight, never a spent one. The genuine "nothing to carry" case is a
    // cleared reference, which should_not_re_arm_an_expiration_on_a_node_that_came_back_up covers.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout firing = mock(Timeout.class);
    Timeout adopted = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(firing, adopted);
    // What Netty answers once the task is on its way: too late to cancel, not yet recorded.
    when(firing.cancel()).thenReturn(false);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);
    previous.startMetricsExpirationTimeout();

    replacement.adoptExpirationFrom(previous);

    verify(timer, times(2)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void should_not_cancel_a_timeout_from_inside_the_arming_accumulator() {
    // AtomicReference re-applies its accumulator when the compare-and-set loses, so the function
    // has to be pure. It used to cancel the candidate it declined, and the re-application is
    // precisely where that bites: a first pass that saw a live countdown, cancelled the newcomer
    // and kept the old one gets re-run against a reference the timer task has since set to EXPIRED
    // -- and now returns the newcomer it just cancelled. Storing that leaves a handle that never
    // fires and is neither null nor EXPIRED, so every later arm keeps it and cancels the fresh one
    // instead, and the node's metrics stop expiring for as long as it stays down.
    //
    // Asserted on the accumulator rather than through startMetricsExpirationTimeout(), because a
    // caller cannot show this without faking the contention: what is wrong is the shape of the
    // function, and that is what this pins.
    Timeout armed = mock(Timeout.class);
    Timeout candidate = mock(Timeout.class);

    assertThat(AbstractMetricUpdater.keepArmed(armed, candidate)).isSameAs(armed);
    assertThat(AbstractMetricUpdater.keepArmed(null, candidate)).isSameAs(candidate);
    assertThat(AbstractMetricUpdater.keepArmed(AbstractMetricUpdater.EXPIRED, candidate))
        .isSameAs(candidate);

    verify(candidate, never()).cancel();
    verify(armed, never()).cancel();
  }

  @Test
  public void should_not_report_an_expiry_a_cancel_won() throws Exception {
    // The interleaving that used to leave a healthy node's metrics on a death clock. The expiry
    // task was clearMetrics(), then cancelMetricsExpirationTimeout() -- which itself cleared the
    // "expired" flag -- then expired.set(true). A cancel from the metrics factory's UP handler
    // landing between the last two left the flag latched on a node that was live again, and the
    // next endpoint change then handed adoptExpirationFrom a countdown to re-arm: an expire-after
    // later, clearMetrics() wiped the whole series of a healthy node, with nothing to re-register
    // it.
    //
    // Both halves are now one compare-and-set on the timeout reference, so whichever of the two
    // arrives second finds the other already there. Asserted by driving the interleaving rather
    // than racing for it: a race cannot demonstrate the absence of a window.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout firedTimeout = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(firedTimeout);
    ArgumentCaptor<TimerTask> taskCaptor = ArgumentCaptor.forClass(TimerTask.class);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);

    // The node goes down and the countdown is armed.
    previous.startMetricsExpirationTimeout();
    verify(timer).newTimeout(taskCaptor.capture(), anyLong(), any(TimeUnit.class));

    // It fires, and the node comes back up while the task is between its two statements. Netty
    // marks a timeout ST_EXPIRED before invoking its task, so this cancel() is already failing.
    previous.cancelMetricsExpirationTimeout();
    taskCaptor.getValue().run(firedTimeout);

    // The node is live, so its endpoint changing must not arm anything.
    replacement.adoptExpirationFrom(previous);

    verify(timer, times(1)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void should_report_an_expiry_no_cancel_intervened_in() throws Exception {
    // The direction that must keep working: a node whose metrics really did expire while it was
    // down, and whose endpoint then changed. The replacement's constructor re-registers the whole
    // series, so without a countdown those resurrected metrics would outlive the node until it
    // next came up or was removed -- which may be never, since the factory's own
    // startMetricsExpirationTimeout() only runs on a DOWN event a node that is already down does
    // not produce.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout firedTimeout = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(firedTimeout);
    ArgumentCaptor<TimerTask> taskCaptor = ArgumentCaptor.forClass(TimerTask.class);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);

    previous.startMetricsExpirationTimeout();
    verify(timer).newTimeout(taskCaptor.capture(), anyLong(), any(TimeUnit.class));
    taskCaptor.getValue().run(firedTimeout);

    replacement.adoptExpirationFrom(previous);

    verify(timer, times(2)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void should_not_re_arm_an_expiration_on_a_node_that_came_back_up() {
    // The other side of the same coin: cancelMetricsExpirationTimeout() is what the metrics factory
    // calls on UP/UNKNOWN, so a node that is live again has nothing to hand over. Arming a
    // countdown
    // for it would clear a healthy node's metrics an expire-after later.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout cancelledTimeout = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(cancelledTimeout);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);
    previous.startMetricsExpirationTimeout();
    previous.cancelMetricsExpirationTimeout();

    replacement.adoptExpirationFrom(previous);

    verify(timer, times(1)).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Test
  public void should_not_fail_the_caller_when_the_timer_is_already_stopped() {
    // adoptExpirationFrom is reached from DefaultNode#setEndPoint, i.e. from NodesRefresh#copyInfos
    // inside MetadataManager's apply step, which neither catches nor contains throwables. And
    // HashedWheelTimer.newTimeout throws once the timer has been stopped (NettyOptions#onClose) or
    // its pending-task ceiling is reached. Letting that out would drop an entire metadata refresh
    // --
    // surfacing only as a DEBUG line -- over an hour-scale cleanup countdown.
    Node node = mock(Node.class);
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    NettyOptions nettyOptions = mock(NettyOptions.class);
    Timer timer = mock(Timer.class);
    Timeout pendingTimeout = mock(Timeout.class);
    when(context.getSessionName()).thenReturn("prefix");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.getTimer()).thenReturn(timer);
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenReturn(pendingTimeout);
    when(pendingTimeout.cancel()).thenReturn(true);

    DropwizardNodeMetricUpdater previous = newNodeUpdater(node, context);
    DropwizardNodeMetricUpdater replacement = newNodeUpdater(node, context);
    previous.startMetricsExpirationTimeout();

    // The session is closing: the timer refuses new tasks from here on.
    when(timer.newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class)))
        .thenThrow(new IllegalStateException("cannot be started once stopped"));

    // Then — the handover is abandoned quietly rather than propagating.
    replacement.adoptExpirationFrom(previous);
  }

  /** A node updater with metric registration stubbed out: only its expiration behavior matters. */
  private static DropwizardNodeMetricUpdater newNodeUpdater(
      Node node, InternalDriverContext context) {
    return new DropwizardNodeMetricUpdater(
        node,
        context,
        Collections.singleton(DefaultNodeMetric.CQL_MESSAGES),
        new MetricRegistry()) {
      @Override
      protected void initializeGauge(
          NodeMetric metric, DriverExecutionProfile profile, Supplier<Number> supplier) {
        // do nothing
      }

      @Override
      protected void initializeCounter(NodeMetric metric, DriverExecutionProfile profile) {
        // do nothing
      }

      @Override
      protected void initializeHdrTimer(
          NodeMetric metric,
          DriverExecutionProfile profile,
          DriverOption highestLatency,
          DriverOption significantDigits,
          DriverOption interval) {
        // do nothing
      }
    };
  }

  @DataProvider
  public static Object[][] acceptableEvictionTimes() {
    return new Object[][] {
      {AbstractMetricUpdater.MIN_EXPIRE_AFTER},
      {AbstractMetricUpdater.MIN_EXPIRE_AFTER.plusMinutes(1)}
    };
  }
}
