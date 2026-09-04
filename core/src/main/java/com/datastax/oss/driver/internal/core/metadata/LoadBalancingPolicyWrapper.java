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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.collection.CompositeQueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps the user-provided LBPs for internal use. This serves multiple purposes:
 *
 * <ul>
 *   <li>help enforce the guarantee that init is called exactly once, and before any other method.
 *   <li>handle the early stages of initialization (before first actual connect), where the LBPs are
 *       not ready yet.
 *   <li>handle incoming node state events from the outside world and propagate them to the
 *       policies.
 *   <li>process distance decisions from the policies and propagate them to the outside world.
 * </ul>
 */
@ThreadSafe
public class LoadBalancingPolicyWrapper implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBalancingPolicyWrapper.class);

  private enum State {
    BEFORE_INIT,
    DURING_INIT,
    RUNNING,
    CLOSING
  }

  private final InternalDriverContext context;
  private final Set<LoadBalancingPolicy> policies;
  private final Map<String, LoadBalancingPolicy> policiesPerProfile;
  private final Map<LoadBalancingPolicy, SinglePolicyDistanceReporter> reporters;

  private final Lock distancesLock = new ReentrantLock();

  // Remember which distance each policy reported for each node. We assume that distance events will
  // be rare, so don't try to be too clever, a global lock should suffice.
  @GuardedBy("distancesLock")
  private final Map<Node, Map<LoadBalancingPolicy, NodeDistance>> distances;

  private final String logPrefix;
  private final ReplayingEventFilter<NodeStateEvent> eventFilter =
      new ReplayingEventFilter<>(this::processNodeStateEvent);
  private final AtomicReference<State> stateRef = new AtomicReference<>(State.BEFORE_INIT);

  public LoadBalancingPolicyWrapper(
      @NonNull InternalDriverContext context,
      @NonNull Map<String, LoadBalancingPolicy> policiesPerProfile) {
    this.context = context;

    this.policiesPerProfile = policiesPerProfile;
    ImmutableMap.Builder<LoadBalancingPolicy, SinglePolicyDistanceReporter> reportersBuilder =
        ImmutableMap.builder();
    // ImmutableMap.values does not remove duplicates, do it now so that we won't invoke a policy
    // more than once if it's associated with multiple profiles
    for (LoadBalancingPolicy policy : ImmutableSet.copyOf(policiesPerProfile.values())) {
      reportersBuilder.put(policy, new SinglePolicyDistanceReporter(policy));
    }
    this.reporters = reportersBuilder.build();
    // Just an alias to make the rest of the code more readable
    this.policies = reporters.keySet();

    this.distances = new WeakHashMap<>();

    this.logPrefix = context.getSessionName();
    context.getEventBus().register(NodeStateEvent.class, this::onNodeStateEvent);
  }

  public void init() {
    if (stateRef.compareAndSet(State.BEFORE_INIT, State.DURING_INIT)) {
      LOG.debug("[{}] Initializing policies", logPrefix);
      // State events can happen concurrently with init, so we must record them and replay once the
      // policy is initialized.
      eventFilter.start();
      MetadataManager metadataManager = context.getMetadataManager();
      Metadata metadata = metadataManager.getMetadata();
      for (LoadBalancingPolicy policy : policies) {
        policy.init(metadata.getNodes(), reporters.get(policy));
      }
      if (stateRef.compareAndSet(State.DURING_INIT, State.RUNNING)) {
        eventFilter.markReady();
      } else { // closed during init
        assert stateRef.get() == State.CLOSING;
        for (LoadBalancingPolicy policy : policies) {
          policy.close();
        }
      }
    }
  }

  /**
   * Note: we could infer the profile name from the request again in this method, but since that's
   * already done in request processors, pass the value directly.
   *
   * @see LoadBalancingPolicy#newQueryPlan(Request, Session)
   */
  @NonNull
  public Queue<Node> newQueryPlan(
      @Nullable Request request, @NonNull String executionProfileName, @Nullable Session session) {
    switch (stateRef.get()) {
      case BEFORE_INIT:
      case DURING_INIT:
        // The contact points are not stored in the metadata yet. Each unresolved hostname is
        // expanded to all its DNS IPs at connection time by ChannelFactory, so one entry per
        // contact point is enough here.
        List<Node> nodes = new ArrayList<>(context.getMetadataManager().getContactPoints());
        Collections.shuffle(nodes);
        return new ConcurrentLinkedQueue<>(nodes);
      case RUNNING:
        LoadBalancingPolicy policy = policiesPerProfile.get(executionProfileName);
        if (policy == null) {
          policy = policiesPerProfile.get(DriverExecutionProfile.DEFAULT_NAME);
        }
        return policy.newQueryPlan(request, session);
      default:
        return new ConcurrentLinkedQueue<>();
    }
  }

  @NonNull
  public Queue<Node> newControlReconnectionQueryPlan() {
    // Read the state once, before building the regular plan. State transitions are monotonic
    // (BEFORE_INIT -> DURING_INIT -> RUNNING -> ...), so this captured value is <= the value
    // newQueryPlan() reads internally; that guarantees we never both build the plan from the
    // contact points (pre-RUNNING branch of newQueryPlan) and append them again below.
    //
    // Note: this is still two separate reads of stateRef (this one, and newQueryPlan()'s own
    // internal read a moment later), so a transition landing exactly between them is possible: if
    // state flips BEFORE_INIT/DURING_INIT -> RUNNING in that window, newQueryPlan() takes the
    // RUNNING branch (a real LBP-built plan) while the state captured here is still pre-RUNNING,
    // so the contact-point fallback below is skipped for this one call even though
    // regularQueryPlan didn't come from the contact-point branch. This is benign: no crash, no
    // duplicate entries, and it self-corrects on the very next reconnection attempt.
    //
    // Monotonicity leaves the other direction open, and it is worth naming: a RUNNING -> CLOSING
    // flip in that same window makes newQueryPlan() take its default branch and return an empty
    // plan, which passes both the RUNNING check below and the empty-plan exemption from the
    // re-resolving-monitor rule, so the plan handed back is the contact points alone. Also benign:
    // ControlConnection abandons a reconnection attempt on closeWasCalled, and every node in that
    // plan is one it already had.
    State state = stateRef.get();
    Queue<Node> regularQueryPlan = newQueryPlan(null, DriverExecutionProfile.DEFAULT_NAME, null);

    // Only append the contact points as an explicit fallback once the LBP is RUNNING: before that
    // (BEFORE_INIT/DURING_INIT), newQueryPlan() above already built regularQueryPlan directly from
    // the contact points, so appending them again here would just duplicate every entry.
    //
    // Skipped when the topology monitor re-resolves node addresses on its own (e.g. proxy-based
    // monitors such as client routes or the cloud SNI proxy): those keep addresses fresh without
    // this fallback, and appending raw contact points could resurrect nodes the monitor has
    // authoritatively removed. The exception is an empty regular plan: with no live node to try,
    // reconnection cannot recover on its own, so the contact-point fallback is kept even for those
    // monitors.
    //
    // isEmpty() is asked of a plan a load balancing policy built, which QueryPlan's contract used
    // to say the driver never does -- so that contract now names this call, because it is what
    // makes the "size() and iterator() never throw" guarantee load-bearing rather than merely
    // documented. Nothing cheaper is available: isEmpty() is size() == 0 through
    // AbstractCollection, size() reads LazyQueryPlan#getNodes(), and so does poll(), so asking
    // through poll() and putting the node back would force the identical computation and allocate
    // a wrapper to do it.
    if (state == State.RUNNING
        && context
            .getConfig()
            .getDefaultProfile()
            .getBoolean(DefaultDriverOption.CONTROL_CONNECTION_RECONNECT_CONTACT_POINTS)
        && (!context.getTopologyMonitor().reresolvesNodeAddresses()
            || regularQueryPlan.isEmpty())) {
      // Append the original (unresolved) contact points so every IP their hostname resolves to is
      // tried as a fallback: ChannelFactory expands each one at connection time, instead of the
      // driver being stuck with whatever single IP a metadata node happens to hold.
      //
      // The retained instances, not fresh copies. MetadataManager holds the contact-point nodes for
      // the session's lifetime and the pre-RUNNING branch of newQueryPlan() already hands out these
      // very objects, so minting a copy per plan would give each reconnection round a distinct node
      // firing its own controlConnectionFailed event -- one set per round, for as long as
      // reconnection lasts. Shuffling a fresh list leaves the retained set itself untouched.
      //
      // Metrics are not a reason either way, and are worth stating because it looks as though they
      // should be: DefaultNode.newContactPoint installs NoopNodeMetricUpdater, so a contact-point
      // node records nothing, and a fresh copy would be no worse. What that costs is narrower than
      // it looks, and narrower than this comment used to claim: errors.connection.auth is written
      // in exactly two places, both in ChannelPool#handleError, and a contact-point node never
      // reaches a pool -- only this query plan -- so that counter was never written on this path,
      // before or after the flip. What is actually missing is every per-node metric for a
      // contact-point plan entry; the only thing a reconnect through one reports is
      // ChannelEvent.controlConnectionFailed, which NodeStateManager no-ops post-init. Giving these
      // nodes real updaters would register metrics under names for ephemeral objects that are
      // deliberately absent from metadata, so it is left as is.
      List<Node> contactNodes = new ArrayList<>(context.getMetadataManager().getContactPoints());
      Collections.shuffle(contactNodes);
      // Concatenate rather than mutate: the RUNNING-state regularQueryPlan is a built-in QueryPlan
      // whose add()/addAll() throw UnsupportedOperationException (poll() is its only mutator).
      // CompositeQueryPlan drains the regular plan first, then the contact-point fallback.
      return new CompositeQueryPlan(regularQueryPlan, new SimpleQueryPlan(contactNodes.toArray()));
    }

    return regularQueryPlan;
  }

  // when it comes in from the outside
  private void onNodeStateEvent(NodeStateEvent event) {
    eventFilter.accept(event);
  }

  // once it has gone through the filter
  private void processNodeStateEvent(NodeStateEvent event) {
    DefaultNode node = event.node;
    switch (stateRef.get()) {
      case BEFORE_INIT:
      case DURING_INIT:
        throw new AssertionError("Filter should not be marked ready until LBP init");
      case CLOSING:
        return; // ignore
      case RUNNING:
        for (LoadBalancingPolicy policy : policies) {
          if (event.newState == NodeState.UP) {
            policy.onUp(node);
          } else if (event.newState == NodeState.DOWN || event.newState == NodeState.FORCED_DOWN) {
            policy.onDown(node);
          } else if (event.newState == NodeState.UNKNOWN) {
            policy.onAdd(node);
          } else if (event.newState == null) {
            policy.onRemove(node);
          } else {
            LOG.warn("[{}] Unsupported event: {}", logPrefix, event);
          }
        }
        break;
    }
  }

  @Override
  public void close() {
    State old;
    while (true) {
      old = stateRef.get();
      if (old == State.CLOSING) {
        return; // already closed
      } else if (stateRef.compareAndSet(old, State.CLOSING)) {
        break;
      }
    }
    // If BEFORE_INIT, no need to close because they were never initialized
    // If DURING_INIT, this will be handled in init()
    if (old == State.RUNNING) {
      for (LoadBalancingPolicy policy : policies) {
        policy.close();
      }
    }
  }

  // An individual distance reporter for one of the policies. The results are aggregated across all
  // policies, the smallest distance for each node is used.
  private class SinglePolicyDistanceReporter implements LoadBalancingPolicy.DistanceReporter {

    private final LoadBalancingPolicy policy;

    private SinglePolicyDistanceReporter(LoadBalancingPolicy policy) {
      this.policy = policy;
    }

    @Override
    public void setDistance(@NonNull Node node, @NonNull NodeDistance suggestedDistance) {
      LOG.debug(
          "[{}] {} suggested {} to {}, checking what other policies said",
          logPrefix,
          policy,
          node,
          suggestedDistance);
      distancesLock.lock();
      try {
        Map<LoadBalancingPolicy, NodeDistance> distancesForNode =
            distances.computeIfAbsent(node, (n) -> new HashMap<>());
        distancesForNode.put(policy, suggestedDistance);
        NodeDistance newDistance = aggregate(distancesForNode);
        LOG.debug("[{}] Shortest distance across all policies is {}", logPrefix, newDistance);

        // There is a small race condition here (check-then-act on a volatile field). However this
        // would only happen if external code changes the distance, which is unlikely (and
        // dangerous).
        // The driver internals only ever set the distance here, and we're protected by the lock.
        NodeDistance oldDistance = node.getDistance();
        if (!oldDistance.equals(newDistance)) {
          LOG.debug("[{}] {} was {}, changing to {}", logPrefix, node, oldDistance, newDistance);
          DefaultNode defaultNode = (DefaultNode) node;
          defaultNode.distance = newDistance;
          context.getEventBus().fire(new DistanceEvent(newDistance, defaultNode));
        } else {
          LOG.debug("[{}] {} was already {}, ignoring", logPrefix, node, oldDistance);
        }
      } finally {
        distancesLock.unlock();
      }
    }

    private NodeDistance aggregate(Map<LoadBalancingPolicy, NodeDistance> distances) {
      NodeDistance minimum = NodeDistance.IGNORED;
      for (NodeDistance candidate : distances.values()) {
        if (candidate.compareTo(minimum) < 0) {
          minimum = candidate;
        }
      }
      return minimum;
    }
  }
}
