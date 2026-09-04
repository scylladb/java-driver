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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.throttling.ConcurrencyLimitingRequestThrottler;
import com.datastax.oss.driver.internal.core.session.throttling.RateLimitingRequestThrottler;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMetricUpdater<MetricT> implements MetricUpdater<MetricT> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricUpdater.class);

  // Not final for testing purposes
  public static Duration MIN_EXPIRE_AFTER = Duration.ofMinutes(5);

  protected final InternalDriverContext context;
  protected final Set<MetricT> enabledMetrics;

  /**
   * Stands in for "the expiration already ran" in {@link #metricsExpirationTimeoutRef}, which would
   * otherwise be unable to say so: {@code null} means both "nothing armed yet" and "the task has
   * cleared the metrics", and {@link #adoptExpirationFrom} has to tell a live node with no
   * countdown from a node whose metrics have already expired.
   *
   * <p>A sentinel rather than a second field, because the two have to be read and written as one.
   * The task's own hand-over used to be a cancel followed by a flag, and a cancel arriving from the
   * node's UP handler in between left the flag latched on a <b>live</b> node -- after which the
   * next endpoint change armed a fresh hour-long countdown that cleared a healthy node's whole
   * series, with nothing to re-register it. One reference makes both transitions a single
   * compare-and-set, so a cancel and an expiry can no longer both appear to have won.
   */
  @VisibleForTesting static final Timeout EXPIRED = new ExpiredSentinel();

  private final AtomicReference<Timeout> metricsExpirationTimeoutRef = new AtomicReference<>();

  private final Duration expireAfter;

  protected AbstractMetricUpdater(InternalDriverContext context, Set<MetricT> enabledMetrics) {
    this.context = context;
    this.enabledMetrics = enabledMetrics;
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    Duration expireAfter = config.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER);
    if (expireAfter.compareTo(MIN_EXPIRE_AFTER) < 0) {
      LOG.warn(
          "[{}] Value too low for {}: {}. Forcing to {} instead.",
          context.getSessionName(),
          DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER.getPath(),
          expireAfter,
          MIN_EXPIRE_AFTER);
      expireAfter = MIN_EXPIRE_AFTER;
    }
    this.expireAfter = expireAfter;
  }

  @Override
  public boolean isEnabled(MetricT metric, String profileName) {
    return enabledMetrics.contains(metric);
  }

  public Duration getExpireAfter() {
    return expireAfter;
  }

  protected int connectedNodes() {
    int count = 0;
    for (Node node : context.getMetadataManager().getMetadata().getNodes().values()) {
      if (node.getOpenConnections() > 0) {
        count++;
      }
    }
    return count;
  }

  protected int throttlingQueueSize() {
    RequestThrottler requestThrottler = context.getRequestThrottler();
    if (requestThrottler instanceof ConcurrencyLimitingRequestThrottler) {
      return ((ConcurrencyLimitingRequestThrottler) requestThrottler).getQueueSize();
    }
    if (requestThrottler instanceof RateLimitingRequestThrottler) {
      return ((RateLimitingRequestThrottler) requestThrottler).getQueueSize();
    }
    LOG.warn(
        "[{}] Metric {} does not support {}, it will always return 0",
        context.getSessionName(),
        DefaultSessionMetric.THROTTLING_QUEUE_SIZE.getPath(),
        requestThrottler.getClass().getName());
    return 0;
  }

  protected long preparedStatementCacheSize() {
    Cache<?, ?> cache = getPreparedStatementCache();
    if (cache == null) {
      LOG.warn(
          "[{}] Metric {} is enabled in the config, "
              + "but it looks like no CQL prepare processor is registered. "
              + "The gauge will always return 0",
          context.getSessionName(),
          DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath());
      return 0L;
    }
    return cache.size();
  }

  @Nullable
  protected Cache<?, ?> getPreparedStatementCache() {
    // By default, both the sync processor and the async ones are registered and they share the same
    // cache. But with a custom processor registry, there could be only one of the two present.
    for (RequestProcessor<?, ?> processor : context.getRequestProcessorRegistry().getProcessors()) {
      if (processor instanceof CqlPrepareAsyncProcessor) {
        return ((CqlPrepareAsyncProcessor) processor).getCache();
      } else if (processor instanceof CqlPrepareSyncProcessor) {
        return ((CqlPrepareSyncProcessor) processor).getCache();
      }
    }
    return null;
  }

  protected int availableStreamIds(Node node) {
    ChannelPool pool = context.getPoolManager().getPools().get(node);
    return (pool == null) ? 0 : pool.getAvailableIds();
  }

  protected int inFlightRequests(Node node) {
    ChannelPool pool = context.getPoolManager().getPools().get(node);
    return (pool == null) ? 0 : pool.getInFlight();
  }

  protected int orphanedStreamIds(Node node) {
    ChannelPool pool = context.getPoolManager().getPools().get(node);
    return (pool == null) ? 0 : pool.getOrphanedIds();
  }

  protected void startMetricsExpirationTimeout() {
    Timeout mine = newTimeout();
    // A spent expiration is not an armed one: re-arming over EXPIRED is what a node going down
    // again after its metrics expired needs, and what adoptExpirationFrom relies on.
    //
    // The accumulator has to stay a pure function of its two arguments. AtomicReference re-applies
    // it when its compare-and-set loses, and the losing read is exactly the one that changes which
    // branch it takes: a first pass that saw a live timeout, cancelled the new one and kept the old
    // is re-run against a reference the timer task has since set to EXPIRED (or an UP-triggered
    // cancel has cleared), and now returns the timeout it just cancelled. Storing that leaves a
    // handle that will never fire and is neither null nor EXPIRED, so every later arm preserves it
    // and cancels the fresh one instead -- the node's metrics stop expiring until it next comes up.
    // Deciding afterwards, from the value the loop settled on, cannot get that wrong.
    Timeout winner =
        metricsExpirationTimeoutRef.accumulateAndGet(mine, AbstractMetricUpdater::keepArmed);
    if (winner != mine) {
      mine.cancel();
    }
  }

  /**
   * The accumulator {@link #startMetricsExpirationTimeout()} hands to {@link AtomicReference}: keep
   * whatever countdown is already armed, and take the candidate only when none is.
   *
   * <p>Split out and named so that the property its caller cannot demonstrate has somewhere to be
   * asserted -- that it is a pure function of its two arguments, and in particular that it does not
   * cancel the candidate it declines. See the comment at the call site for what happens when it
   * does.
   */
  @VisibleForTesting
  static Timeout keepArmed(Timeout current, Timeout candidate) {
    return (current == null || current == EXPIRED) ? candidate : current;
  }

  protected void cancelMetricsExpirationTimeout() {
    // Called when the node comes back up, so whatever expiry happened is spent: there is nothing
    // left for a later adoptExpirationFrom() to carry over. Clearing the reference says both of
    // those at once, which is the point of the sentinel -- an expiry landing either side of this
    // is ordered against it rather than racing a separate flag.
    Timeout t = metricsExpirationTimeoutRef.getAndSet(null);
    if (t != null && t != EXPIRED) {
      t.cancel();
    }
  }

  /**
   * Moves a pending expiration from the updater being replaced onto this one. See {@link
   * NodeMetricUpdater#adoptExpirationFrom}.
   *
   * <p>Re-armed rather than handed over as-is, so the replacement's own {@code
   * startMetricsExpirationTimeout()} runs and the countdown belongs to the object whose metrics it
   * will clear. That restarts the clock; expiry is a coarse, hour-scale cleanup and the node has to
   * stay down for the whole period either way, so the reset is not worth carrying the original
   * deadline around for.
   *
   * <p>An expiration that has <b>already run</b> is carried over too, not just a pending one -- and
   * so is one that is running <i>right now</i>, which is neither, and which is why the test is that
   * the reference was non-null rather than anything {@code cancel()} reports. That is not
   * redundant: the replacement's constructor eagerly re-registers the node's whole metric set, so a
   * node that expired while down and then had its endpoint change comes back with every series
   * present again and, if nothing were armed here, no countdown to clear them. The only other
   * caller of {@code startMetricsExpirationTimeout()} is the metrics factory's
   * DOWN/FORCED_DOWN/removed handler, and a node that is already down produces no such event -- so
   * "nothing armed" would mean the resurrected series outlive the node until it next comes up or is
   * removed, which may be never.
   *
   * <p>A node that is merely <i>live</i> is not caught by that: {@code
   * cancelMetricsExpirationTimeout()}, which the same handler calls on UP, clears the reference, so
   * an endpoint change on a healthy node still arms nothing. That holds because the expiry task and
   * that cancel contend for one reference -- see {@link #EXPIRED}. It did not while they were a
   * cancel followed by a separate flag, and the direction that lost was this one: a UP-triggered
   * cancel landing inside the task left a live node looking expired, and this method then armed a
   * countdown that cleared a healthy node's metrics an hour later for good.
   *
   * <p>Re-arming is best effort. {@code HashedWheelTimer.newTimeout} throws once the timer has been
   * stopped ({@code NettyOptions#onClose}) or its pending-task ceiling is reached, and the only
   * caller is {@code DefaultNode#setEndPoint} -- reached from {@code NodesRefresh#copyInfos} inside
   * {@code MetadataManager}'s apply step, which neither catches nor contains throwables. Letting
   * one out would drop an entire metadata refresh, surfacing only as a DEBUG line, over an
   * hour-scale cleanup countdown. Losing the countdown costs at worst one node's metrics not
   * expiring.
   */
  public void adoptExpirationFrom(NodeMetricUpdater previous) {
    if (!(previous instanceof AbstractMetricUpdater)) {
      return;
    }
    AbstractMetricUpdater<?> replaced = (AbstractMetricUpdater<?>) previous;
    Timeout pending = replaced.metricsExpirationTimeoutRef.getAndSet(null);
    if (pending == null) {
      return;
    }
    // Any non-null value is a countdown to carry, whatever cancel() says about it. The reference is
    // null in exactly one situation -- nothing is armed, either because nothing ever was or because
    // cancelMetricsExpirationTimeout() cleared it when the node came up -- so non-null already
    // answers the question this method asks.
    //
    // Reading cancel()'s answer instead loses the one window the EXPIRED sentinel was added for.
    // Netty flips a HashedWheelTimeout to ST_EXPIRED *before* running its task, and the task then
    // clears a whole metric set before reaching its compare-and-set, so for that entire interval
    // cancel() returns false while the reference still holds the real timeout. A setEndPoint
    // landing there would conclude there was nothing to carry and arm nothing, and the task's own
    // compare-and-set then fails against the getAndSet above -- so neither side arms anything, and
    // the replacement's eagerly re-registered metric set is left with no countdown at all. That is
    // verbatim the outcome the paragraph above says the carry-over exists to prevent.
    if (pending != EXPIRED) {
      pending.cancel();
    }
    try {
      startMetricsExpirationTimeout();
    } catch (RuntimeException e) {
      LOG.debug("Could not re-arm the metrics expiration timeout, skipping it", e);
    }
  }

  protected Timeout newTimeout() {
    return context
        .getNettyOptions()
        .getTimer()
        .newTimeout(
            t -> {
              clearMetrics();
              // Conditional on this timeout still being the current one, which is what makes the
              // hand-over a single transition. Netty marks a timeout ST_EXPIRED before invoking its
              // task, so a concurrent cancel() -- from the node coming back up, or from
              // adoptExpirationFrom claiming the countdown -- can already be failing while this
              // task runs; the compare-and-set is how the two agree on which of them won. Losing
              // means the reference was cleared or re-armed underneath, and then the expiry is not
              // this object's to report. Note that neither of those callers decides anything from
              // that failing cancel(): it cannot distinguish "already cancelled" from "expiring as
              // we speak", and both of them go by the reference instead.
              //
              // Not routed through cancelMetricsExpirationTimeout() any more, even though
              // MicrometerNodeMetricUpdater and MicroProfileNodeMetricUpdater override it: that
              // method means "the node is back, drop the countdown", which is the opposite of what
              // happened here, and calling it is what reopened the window, by clearing the flag the
              // next statement then had to set. Both overrides are pure super-delegation today, so
              // nothing observable moves.
              metricsExpirationTimeoutRef.compareAndSet(t, EXPIRED);
            },
            expireAfter.toNanos(),
            TimeUnit.NANOSECONDS);
  }

  /**
   * The {@link #EXPIRED} marker. Never handed to a timer and never asked to do anything: it only
   * has to be distinguishable from a real {@link Timeout} by reference.
   */
  private static final class ExpiredSentinel implements Timeout {

    @Override
    public Timer timer() {
      throw new UnsupportedOperationException("Not a real timeout");
    }

    @Override
    public TimerTask task() {
      throw new UnsupportedOperationException("Not a real timeout");
    }

    @Override
    public boolean isExpired() {
      return true;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean cancel() {
      return false;
    }
  }
}
