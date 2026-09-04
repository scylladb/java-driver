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
package com.datastax.oss.driver.internal.core.control;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.EventCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesUpdateEvent;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.PinnableEndPoint;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.event.ClientRoutesChangeEvent;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.event.TopologyChangeEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains a dedicated connection to a Cassandra node for administrative queries.
 *
 * <p>If the control node goes down, a reconnection is triggered. The control node is chosen
 * randomly among the contact points at startup, or according to the load balancing policy for later
 * reconnections.
 *
 * <p>The control connection is used by:
 *
 * <ul>
 *   <li>{@link DefaultTopologyMonitor} to determine cluster connectivity and retrieve node
 *       metadata;
 *   <li>{@link MetadataManager} to run schema metadata queries.
 * </ul>
 */
@ThreadSafe
public class ControlConnection implements EventCallback, AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ControlConnection.class);

  /**
   * How many removed host ids {@code SingleThreaded#removedHostIds} keeps before evicting the
   * oldest. Large enough that the set still covers the churn of a rolling restart many times over,
   * small enough that it cannot grow into a leak over a session's lifetime.
   */
  private static final int MAX_REMOVED_HOST_IDS = 256;

  /**
   * How many consecutive reconnection rounds may be refused entirely by exclusions before {@code
   * SingleThreaded#removedHostIds} is cleared anyway.
   *
   * <p>Small, because every one of those rounds is a round that reached a live server and then
   * threw the channel away: there is nothing to learn from repeating it, and the only thing still
   * refusing is a judgement the driver cannot re-check while the control connection is down. Not
   * one, because the refusal has to actually take effect -- see {@code #reconnect}.
   */
  private static final int MAX_ALL_EXCLUDED_ROUNDS = 3;

  private final InternalDriverContext context;
  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;

  // The single channel used by this connection. This field is accessed concurrently, but only
  // mutated on adminExecutor (by SingleThreaded methods)
  private volatile DriverChannel channel;

  public ControlConnection(InternalDriverContext context) {
    this.context = context;
    this.logPrefix = context.getSessionName();
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context);
  }

  /**
   * Initializes the control connection. If it is already initialized, this is a no-op and all
   * parameters are ignored.
   *
   * @param listenToClusterEvents whether to register for TOPOLOGY_CHANGE and STATUS_CHANGE events.
   *     If the control connection has already initialized with another value, this is ignored.
   *     SCHEMA_CHANGE events are always registered.
   * @param reconnectOnFailure whether to schedule a reconnection if the initial attempt fails (if
   *     true, the returned future will only complete once the reconnection has succeeded).
   * @param useInitialReconnectionSchedule if no node can be reached, the type of reconnection
   *     schedule to use. In other words, the value that will be passed to {@link
   *     ReconnectionPolicy#newControlConnectionSchedule(boolean)}. Note that this parameter is only
   *     relevant if {@code reconnectOnFailure} is true, otherwise it is not used.
   */
  public CompletionStage<Void> init(
      boolean listenToClusterEvents,
      boolean reconnectOnFailure,
      boolean useInitialReconnectionSchedule) {
    RunOrSchedule.on(
        adminExecutor,
        () ->
            singleThreaded.init(
                listenToClusterEvents, reconnectOnFailure, useInitialReconnectionSchedule));
    return singleThreaded.initFuture;
  }

  public CompletionStage<Void> initFuture() {
    return singleThreaded.initFuture;
  }

  public boolean isInit() {
    return singleThreaded.initFuture.isDone();
  }

  /**
   * The channel currently used by this control connection. This is modified concurrently in the
   * event of a reconnection, so it may occasionally return a closed channel (clients should be
   * ready to deal with that).
   */
  public DriverChannel channel() {
    return channel;
  }

  /**
   * The node currently associated with the control channel, or {@code null} if the control
   * connection is not established or the node has not been resolved yet.
   */
  public Node controlNode() {
    return singleThreaded.controlNodeState.current;
  }

  /**
   * Forces an immediate reconnect: if we were connected to a node, that connection will be closed;
   * if we were already reconnecting, the next attempt is started immediately, without waiting for
   * the next scheduled interval; in all cases, a new query plan is fetched from the load balancing
   * policy, and each node in it will be tried in sequence.
   */
  public void reconnectNow() {
    RunOrSchedule.on(adminExecutor, singleThreaded::reconnectNow);
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    // Control queries are never critical, so there is no graceful close.
    return forceCloseAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  @Override
  public void onEvent(Message eventMessage) {
    if (!(eventMessage instanceof Event)) {
      LOG.warn("[{}] Unsupported event class: {}", logPrefix, eventMessage.getClass().getName());
    } else {
      LOG.debug("[{}] Processing incoming event {}", logPrefix, eventMessage);
      Event event = (Event) eventMessage;
      switch (event.type) {
        case ProtocolConstants.EventType.TOPOLOGY_CHANGE:
          processTopologyChange(event);
          break;
        case ProtocolConstants.EventType.STATUS_CHANGE:
          processStatusChange(event);
          break;
        case ProtocolConstants.EventType.SCHEMA_CHANGE:
          processSchemaChange(event);
          break;
        case ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE:
          processClientRoutesChange(event);
          break;
        default:
          LOG.warn("[{}] Unsupported event type: {}", logPrefix, event.type);
      }
    }
  }

  private void processTopologyChange(Event event) {
    TopologyChangeEvent tce = (TopologyChangeEvent) event;
    switch (tce.changeType) {
      case ProtocolConstants.TopologyChangeType.NEW_NODE:
        context.getEventBus().fire(TopologyEvent.suggestAdded(tce.address));
        break;
      case ProtocolConstants.TopologyChangeType.REMOVED_NODE:
        context.getEventBus().fire(TopologyEvent.suggestRemoved(tce.address));
        break;
      default:
        LOG.warn("[{}] Unsupported topology change type: {}", logPrefix, tce.changeType);
    }
  }

  private void processStatusChange(Event event) {
    StatusChangeEvent sce = (StatusChangeEvent) event;
    switch (sce.changeType) {
      case ProtocolConstants.StatusChangeType.UP:
        context.getEventBus().fire(TopologyEvent.suggestUp(sce.address));
        break;
      case ProtocolConstants.StatusChangeType.DOWN:
        context.getEventBus().fire(TopologyEvent.suggestDown(sce.address));
        break;
      default:
        LOG.warn("[{}] Unsupported status change type: {}", logPrefix, sce.changeType);
    }
  }

  private void processSchemaChange(Event event) {
    SchemaChangeEvent sce = (SchemaChangeEvent) event;
    context
        .getMetadataManager()
        .refreshSchema(sce.keyspace, false, false)
        .whenComplete(
            (metadata, error) -> {
              if (error != null) {
                Loggers.warnWithException(
                    LOG,
                    "[{}] Unexpected error while refreshing schema for a SCHEMA_CHANGE event, "
                        + "keeping previous version",
                    logPrefix,
                    error);
              }
            });
  }

  private void processClientRoutesChange(Event event) {
    ClientRoutesChangeEvent crce = (ClientRoutesChangeEvent) event;
    LOG.debug("[{}] Received CLIENT_ROUTES_CHANGE event: {}", logPrefix, crce);
    context
        .getEventBus()
        .fire(new ClientRoutesUpdateEvent(crce.changeType, crce.connectionIds, crce.hostIds));
  }

  /**
   * Whether {@code error} records a node this connection was not allowed to use, rather than one it
   * tried and failed to reach.
   *
   * <p>Walks the cause chain rather than testing the top-level throwable, because a refusal is
   * wrapped once for every layer it travels through and the number of layers depends on where it
   * was raised. From the query plan it arrives bare. From the connect hook -- where a contact
   * point's host id is now settled, one candidate at a time -- it comes back through a failed stage
   * as a {@link CompletionException}, and {@code ChannelFactory#finishCandidate} then wraps that in
   * a {@code ConnectionInitException} before the candidate loop ever sees it. Matching on one fixed
   * shape would silently classify the deeper one as a connectivity failure, which is the opposite
   * of what it is.
   *
   * <p>Shared by every reader of a round's error list, which have to agree on what an exclusion
   * means -- {@code anyNodeUnreachable} must not count one as having reached something, {@code
   * anyNodeExcluded} decides whether the round spends any of the refusal budget, and {@code
   * isAuthFailure} must not let one veto the verdict.
   */
  private static boolean isExcluded(Throwable error) {
    // Bounded rather than unbounded: getCause() is overridable, so a cyclic chain is possible in
    // principle, and no legitimate one is anywhere near this deep.
    Throwable cause = error;
    for (int depth = 0; cause != null && depth < 16; depth++) {
      if (cause instanceof ExcludedNodeException) {
        return true;
      }
      Throwable next = cause.getCause();
      cause = (next == cause) ? null : next;
    }
    return false;
  }

  /**
   * Whether {@code error} and every failure attached to it are exclusions, i.e. whether "we were
   * not allowed to use it" is the whole story for the node that produced it.
   *
   * <p>The test to use wherever an exclusion must not be confused with a connection failure, for
   * the same reason {@link ChannelFactory#isAuthOnly} exists: one error no longer means one
   * address. A contact point expands to every address its name resolves to and reports a single
   * failure with the others attached as {@linkplain Throwable#getSuppressed() suppressed}, so a
   * name whose addresses went {@code [excluded, refused]} can surface either half depending on
   * which one {@code ChannelFactory#surfacedFailure} promotes. Only when no address was reached at
   * all is the node's failure genuinely an exclusion.
   */
  private static boolean isExclusionOnly(Throwable error) {
    if (!isExcluded(error)) {
      return false;
    }
    for (Throwable suppressed : error.getSuppressed()) {
      if (!isExcluded(suppressed)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether an exclusion appears anywhere in what {@code error} reports -- as the failure itself,
   * in its cause chain, or among the failures attached to it.
   *
   * <p>The weakest of the three, and the right one for a caller asking whether the round got as far
   * as refusing something rather than whether refusing is all it did. A contact point whose
   * addresses went {@code [excluded, refused]} surfaces one half or the other depending on {@code
   * ChannelFactory#surfacedFailure}, so neither {@link #isExcluded} nor {@link #isExclusionOnly}
   * answers that question without depending on which address happened to be tried last.
   */
  private static boolean mentionsExclusion(Throwable error) {
    if (isExcluded(error)) {
      return true;
    }
    for (Throwable suppressed : error.getSuppressed()) {
      if (isExcluded(suppressed)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static class ExcludedNodeException extends IllegalStateException {
    private static final long serialVersionUID = 1;

    ExcludedNodeException(String reason) {
      super(reason);
    }
  }

  private class SingleThreaded {
    private final InternalDriverContext context;
    private final DriverConfig config;
    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final ReconnectionPolicy reconnectionPolicy;
    private final Reconnection reconnection;
    // Computed once in init() and kept; the options themselves are built fresh for every connect
    // attempt (see buildChannelOptions), so this is the only part of them that lives here.
    private ImmutableList<String> eventTypes;
    private volatile ControlNodeState controlNodeState = ControlNodeState.NONE;
    // The last events received for each node
    private final Map<Node, NodeDistance> lastNodeDistance = new WeakHashMap<>();
    private final Map<Node, NodeState> lastNodeState = new WeakHashMap<>();

    /**
     * Host ids the topology monitor has removed, for as long as they stay removed.
     *
     * <p>The two maps above are keyed on the {@link Node} instance, which cannot answer for a node
     * the driver is about to re-create: {@code MetadataManager#registerNode} mints a fresh {@link
     * DefaultNode} for a host id absent from metadata, and {@code DefaultNode} overrides neither
     * {@code equals} nor {@code hashCode}, so the new instance is in neither map. Removal is
     * therefore also recorded by host id, which survives the re-creation -- see {@link
     * #exclusionReasonForHostId}.
     *
     * <p>An entry is dropped as soon as the host id reports any state, so a node that legitimately
     * comes back is not blocked -- but that path runs on node state events, which arrive from a
     * metadata refresh and therefore need a working control connection. It cannot un-refuse a node
     * while the control connection is down, which is the one moment this set can do harm, so a
     * reconnection round that fails against its whole plan clears it outright (see {@link
     * #reconnect}).
     *
     * <p>Neither of those bounds it on its own, which is why it is also capped at 256 entries,
     * evicting the oldest. Unlike {@code lastNodeDistance} and {@code lastNodeState} next to it --
     * {@link java.util.WeakHashMap}s, so a dead {@link Node} takes its entry with it -- this is
     * keyed on a {@link UUID} the driver holds strongly, and the state-event path only ever drops
     * the id of a node that came <i>back</i>. A host id that never returns, which is every
     * decommissioned or replaced node, would otherwise stay for the life of the session: under
     * rolling instance replacement each round mints new ids and none of the old ones are ever
     * removed. Evicting the oldest is the right way to lose them, since the risk this set guards
     * against -- a contact point whose DNS still lists a node the monitor removed -- fades as the
     * removal recedes; the cost of an eviction is at worst one connection attempt to a node that is
     * gone, which is the behaviour that predates the set entirely.
     */
    private final Set<UUID> removedHostIds =
        Collections.newSetFromMap(
            new LinkedHashMap<UUID, Boolean>() {
              @Override
              protected boolean removeEldestEntry(Map.Entry<UUID, Boolean> eldest) {
                return size() > MAX_REMOVED_HOST_IDS;
              }
            });

    /**
     * How many reconnection rounds in a row drained without reaching anything, every node in them
     * having been refused instead.
     *
     * <p>Counts only consecutive rounds: a round that reached something resets it, and so does a
     * successful reconnection. At {@link #MAX_ALL_EXCLUDED_ROUNDS} it clears {@link
     * #removedHostIds} and resets, which is the only exit from that set that does not require the
     * control connection this class is trying to restore. See {@code #reconnect}.
     *
     * <p>Admin-thread confined, like everything else in this class.
     */
    private int consecutiveAllExcludedRounds;

    private SingleThreaded(InternalDriverContext context) {
      this.context = context;
      this.config = context.getConfig();
      this.reconnectionPolicy = context.getReconnectionPolicy();
      this.reconnection =
          new Reconnection(
              logPrefix,
              adminExecutor,
              () -> reconnectionPolicy.newControlConnectionSchedule(false),
              this::reconnect);
      // In "reconnect-on-init" mode, handle cancellation of the initFuture by user code
      CompletableFutures.whenCancelled(
          this.initFuture,
          () -> {
            LOG.debug("[{}] Init future was cancelled, stopping reconnection", logPrefix);
            reconnection.stop();
          });

      context
          .getEventBus()
          .register(DistanceEvent.class, RunOrSchedule.on(adminExecutor, this::onDistanceEvent));
      context
          .getEventBus()
          .register(NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onStateEvent));
    }

    private void init(
        boolean listenToClusterEvents,
        boolean reconnectOnFailure,
        boolean useInitialReconnectionSchedule) {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      try {
        boolean listenClientRoutesEvents =
            context.getTopologyMonitor() instanceof ClientRoutesTopologyMonitor;
        this.eventTypes = buildEventTypes(listenToClusterEvents, listenClientRoutesEvents);
        LOG.debug("[{}] Initializing with event types {}", logPrefix, eventTypes);

        Queue<Node> nodes =
            context.getLoadBalancingPolicyWrapper().newControlReconnectionQueryPlan();

        connect(
            nodes,
            null,
            () -> initFuture.complete(null),
            error -> {
              if (isAuthFailure(error)) {
                LOG.warn(
                    "[{}] Authentication errors encountered on all contact points. Please check your authentication configuration.",
                    logPrefix);
              }
              if (reconnectOnFailure && !closeWasCalled) {
                reconnection.start(
                    reconnectionPolicy.newControlConnectionSchedule(
                        useInitialReconnectionSchedule));
              } else {
                // Special case for the initial connection: reword to a more user-friendly error
                // message
                if (error instanceof AllNodesFailedException) {
                  error =
                      ((AllNodesFailedException) error)
                          .reword(
                              "Could not reach any contact point, "
                                  + "make sure you've provided valid addresses");
                }
                initFuture.completeExceptionally(error);
              }
            });
      } catch (Throwable t) {
        initFuture.completeExceptionally(t);
      }
    }

    private CompletionStage<Boolean> reconnect() {
      assert adminExecutor.inEventLoop();
      Queue<Node> nodes = context.getLoadBalancingPolicyWrapper().newControlReconnectionQueryPlan();
      CompletableFuture<Boolean> result = new CompletableFuture<>();
      connect(
          nodes,
          null,
          () -> {
            result.complete(true);
            onSuccessfulReconnect();
          },
          error -> {
            // A round that reached nothing at all leaves this judgement unusable, so drop it.
            //
            // removedHostIds is only ever cleared by a NodeStateEvent, and those arrive from a
            // metadata refresh, which needs the very control connection this is trying to restore.
            // A host id recorded as removed on stale information -- a node transiently missing from
            // a peers table during a restart, say -- would therefore refuse the only reachable
            // address for the rest of the session, and the contact-point fallback that exists to
            // re-resolve names could never recover from it. That is a permanent deadlock; a
            // resurrection is not, since the ids are re-learned from the first successful refresh.
            //
            // Clearing here does not weaken the protection where it earns its keep. The case it
            // guards -- a contact point whose DNS still lists a node the monitor removed -- happens
            // while other nodes are reachable, so those rounds succeed and never reach this branch.
            // Only a round that failed against every node in its plan does, and at that point the
            // driver's view of who was removed is exactly as stale as its view of everything else.
            //
            // Only when something was genuinely unreachable, though. This branch is also reached by
            // a plan that drained without a single connectivity failure, every node in it having
            // been refused instead: excluded by distance or state, or turned away by host id --
            // which is this very set doing its job. Nothing is stale about the driver's view then,
            // and clearing on it would undo, on the round that just enforced it, the refusal that
            // the next round's contact-point fallback would immediately need again.
            //
            // But not forever. Enforcing a refusal is one thing; enforcing it for the life of the
            // session on evidence that can never be rechecked is another, and that is what an
            // unbounded version of this would do. Every other way out of the set needs something
            // this situation does not have: a NodeStateEvent arrives from a metadata refresh, which
            // needs the control connection being restored here, and the LRU cap only evicts after
            // MAX_REMOVED_HOST_IDS *further* removals, which likewise cannot be learned. So a round
            // that reached a live server and refused it, over and over, is a round that will keep
            // producing the identical outcome -- and if the refused host id is the only address the
            // plan has, the session never recovers. Give the refusal MAX_ALL_EXCLUDED_ROUNDS rounds
            // to matter, then clear and let the next round find out for itself. Being wrong that
            // way costs one connect to a node that is gone; being wrong the other way costs the
            // session.
            //
            // What the budget buys back is the removal set, and only that. The other two sources
            // #exclusionReason draws on -- a distance event that made the node IGNORED, a state
            // event that removed or forced it down -- are just as un-recheckable while the control
            // connection is down, and #exclusionReasonForHostId consults them first for any host id
            // still in metadata, so clearing here cannot lift them. That is deliberate: those two
            // say the driver was *told* not to use this node, where the removal set says the driver
            // inferred it and may be out of date. A plan every entry of which is refused on
            // distance or state therefore stays refused, which for a local-DC outage behind a
            // contact point resolving only to remote-DC nodes means the control connection does
            // not come up -- as it did not before this fallback existed, the plan then being
            // empty. Lifting those two as well would put the control connection back on a node an
            // operator forced down; see
            // https://github.com/scylladb/java-driver/issues/1010.
            //
            // Only a round that actually refused something counts against that budget. A plan that
            // was empty to begin with drains through here too, and it is neither of the two cases
            // above: it reached nothing and it turned nothing away, so it learned nothing either
            // way -- which is precisely why #anyNodeUnreachable declines to clear on it. Letting it
            // drive the budget would discard the set on the one kind of evidence both branches
            // agree confers no standing, and an empty plan is reachable: turn the contact-point
            // fallback off and let the load balancing policy's view go empty.
            if (anyNodeUnreachable(error)) {
              consecutiveAllExcludedRounds = 0;
              removedHostIds.clear();
            } else if (anyNodeExcluded(error)
                && ++consecutiveAllExcludedRounds >= MAX_ALL_EXCLUDED_ROUNDS) {
              LOG.debug(
                  "[{}] {} consecutive reconnection rounds were refused outright; "
                      + "discarding the set of removed host ids so the next round can retry them",
                  logPrefix,
                  consecutiveAllExcludedRounds);
              consecutiveAllExcludedRounds = 0;
              removedHostIds.clear();
            }
            result.complete(false);
          });
      return result;
    }

    /**
     * Whether a failed reconnection round actually failed to <b>reach</b> something, as opposed to
     * having had every node in its plan refused.
     *
     * <p>Drawn from the errors the round collected, since both outcomes arrive here as the same
     * {@link AllNodesFailedException}. A round with no errors at all -- a plan that was empty to
     * begin with -- counts as <b>not</b> unreachable: it never tried anything, so it learned
     * nothing about who is reachable and has no standing to discard the removal set.
     *
     * <p>{@link #mentionsExclusion}, and deliberately the weakest of the three tests. What clearing
     * needs is evidence that the driver's view is stale, and a round that refused a node <i>proves
     * the opposite</i>: it handshaked with a live server and read its host id. Neither {@link
     * #isExcluded} nor {@link #isExclusionOnly} can be that test, because a contact point reports
     * one failure for the whole name and {@code ChannelFactory#surfacedFailure} decides which of
     * its addresses speaks -- so a name whose addresses went {@code [excluded, refused]} would be
     * classified by whichever one happened to be tried last. Asking whether an exclusion is
     * mentioned at all is the only reading that does not turn on that.
     *
     * <p>The consequence is that one firewalled sibling record no longer discards the refusal its
     * neighbour just earned. Such a round still counts as excluded, so {@code
     * MAX_ALL_EXCLUDED_ROUNDS} engages and the set is discarded after a few of them -- the recovery
     * is delayed, not removed, and the deadlock the clearing exists to break stays broken.
     */
    private boolean anyNodeUnreachable(Throwable roundFailure) {
      if (!(roundFailure instanceof AllNodesFailedException)) {
        return false;
      }
      for (List<Throwable> nodeErrors :
          ((AllNodesFailedException) roundFailure).getAllErrors().values()) {
        for (Throwable nodeError : nodeErrors) {
          if (!mentionsExclusion(nodeError)) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Whether a failed reconnection round refused at least one node, as opposed to having had
     * nothing to try in the first place.
     *
     * <p>The complement of {@link #anyNodeUnreachable} on the branch that matters, not its
     * negation: a round can be neither, and an empty plan is exactly that -- so such a round
     * neither clears {@code removedHostIds} nor spends any of the budget that eventually will.
     *
     * <p>{@link #mentionsExclusion}, the same test its sibling uses, and that is what makes the two
     * complements. Reading one throwable deeper on one side than the other leaves a gap between
     * them, and a round can fall into it: an exclusion carried only among a node's {@linkplain
     * Throwable#getSuppressed() suppressed} failures is not "unreachable" to the sibling and would
     * not be "excluded" here either, so neither branch would run. That gap is not hypothetical --
     * {@code ChannelFactory#surfacedFailure} promotes an authentication failure over an exclusion,
     * so a contact point whose addresses went {@code [excluded, bad-credentials]} lands in it
     * deterministically, on every round, and both the clearing and the give-up counter stall for
     * the life of the session. Which is the deadlock {@code MAX_ALL_EXCLUDED_ROUNDS} exists to
     * break.
     */
    private boolean anyNodeExcluded(Throwable roundFailure) {
      if (!(roundFailure instanceof AllNodesFailedException)) {
        return false;
      }
      for (List<Throwable> nodeErrors :
          ((AllNodesFailedException) roundFailure).getAllErrors().values()) {
        for (Throwable nodeError : nodeErrors) {
          if (mentionsExclusion(nodeError)) {
            return true;
          }
        }
      }
      return false;
    }

    private void connect(
        Queue<Node> nodes,
        List<Entry<Node, Throwable>> errors,
        Runnable onSuccess,
        Consumer<Throwable> onFailure) {
      assert adminExecutor.inEventLoop();
      Node node = nodes.poll();
      if (node == null) {
        onFailure.accept(AllNodesFailedException.fromErrors(errors));
      } else {
        LOG.debug("[{}] Trying to establish a connection to {}", logPrefix, node);
        NodeInfoHolder capturedNodeInfo = new NodeInfoHolder();
        context
            .getChannelFactory()
            .connect(node, buildChannelOptions(node, capturedNodeInfo))
            .whenCompleteAsync(
                (channel, error) -> {
                  try {
                    String exclusion = exclusionReason(node);
                    if (error != null) {
                      if (closeWasCalled || initFuture.isCancelled()) {
                        onSuccess.run(); // abort, we don't really care about the result
                      } else if (isExclusionOnly(error)) {
                        // Every address of this contact point turned out to be a node this
                        // connection may not use -- the connect hook refused each one on its host
                        // id (see #readChannelNodeInfo). Reported exactly as the two exclusions
                        // that are decided on the admin thread are: at DEBUG, recorded so that
                        // exhausting the plan this way says why rather than surfacing a bare
                        // NoNodeAvailableException, and with no controlConnectionFailed event,
                        // because nothing failed to connect. Routing it through the branch below
                        // would warn the operator about a deployment that is in fact reachable,
                        // and would count a refusal as a connection failure in the metrics.
                        LOG.debug(
                            "[{}] Every address of {} belongs to a node this connection may not"
                                + " use, trying next node",
                            logPrefix,
                            node,
                            error);
                        List<Entry<Node, Throwable>> exclusionErrors =
                            (errors == null) ? new ArrayList<>() : errors;
                        exclusionErrors.add(new SimpleEntry<>(node, error));
                        connect(nodes, exclusionErrors, onSuccess, onFailure);
                      } else {
                        // isAuthOnly, not a bare instanceof: ChannelFactory reports one failure
                        // per contact point with the other addresses' failures attached as
                        // suppressed, and it deliberately surfaces an authentication failure over
                        // transport ones. A name whose records failed [refused, refused, auth] is
                        // not an authentication problem, and logging it as one would hide that two
                        // thirds of the deployment is unreachable.
                        if (ChannelFactory.isAuthOnly(error)) {
                          Loggers.warnWithException(
                              LOG, "[{}] Authentication error", logPrefix, error);
                        } else if (ChannelFactory.mentionsAuthentication(error)) {
                          // Mixed [refused, refused, auth]. Not an authentication problem alone --
                          // hence the wording -- but still warned unconditionally, as every
                          // AuthenticationException was before multi-address support.
                          // advanced.connection.warn-on-init-error mutes unreachable-node noise;
                          // it is not a switch for "your credentials are wrong". Folding this case
                          // into the gated branch below would log the only actionable half of the
                          // failure at DEBUG.
                          //
                          // mentionsAuthentication, not `error instanceof AuthenticationException`:
                          // which failure of the set arrives here is decided by
                          // ChannelFactory#surfacedFailure, and it ranks a node-wide failure and an
                          // invalid keyspace *above* an authentication one. So [auth,
                          // event-type-rejected] surfaces the rejection with the auth failure
                          // suppressed, and a test on the type of what arrived would send exactly
                          // the case this branch exists for down the gated path instead.
                          Loggers.warnWithException(
                              LOG,
                              "[{}] Error connecting to {} (authentication failed on some of its"
                                  + " addresses, others failed for other reasons), trying next node",
                              logPrefix,
                              node,
                              error);
                        } else {
                          if (config
                              .getDefaultProfile()
                              .getBoolean(DefaultDriverOption.CONNECTION_WARN_INIT_ERROR)) {
                            Loggers.warnWithException(
                                LOG,
                                "[{}] Error connecting to {}, trying next node",
                                logPrefix,
                                node,
                                error);
                          } else {
                            LOG.debug(
                                "[{}] Error connecting to {}, trying next node",
                                logPrefix,
                                node,
                                error);
                          }
                        }
                        List<Entry<Node, Throwable>> newErrors =
                            (errors == null) ? new ArrayList<>() : errors;
                        newErrors.add(new SimpleEntry<>(node, error));
                        // Contained for the same reason as the channelOpened fire further down,
                        // and it is the same hazard: EventBus.fire() has no try/catch of its own
                        // and RunOrSchedule.on(adminExecutor, ..) runs listeners inline when
                        // already on the admin loop, so a listener that throws would escape into
                        // the outer catch (Exception) -- which only logs -- and skip the connect()
                        // below. The round would then never advance and never complete: initFuture
                        // stays pending (SessionBuilder.build() blocks) or the Reconnection is
                        // parked in ATTEMPT_IN_PROGRESS for good. Whether the round advances is
                        // not a listener's to decide, and a Throwable is caught rather than an
                        // Exception because the outer catch would not stop an Error here either.
                        try {
                          context.getEventBus().fire(ChannelEvent.controlConnectionFailed(node));
                        } catch (Throwable t) {
                          Loggers.warnWithException(
                              LOG,
                              "[{}] Listener threw while handling controlConnectionFailed for {};"
                                  + " continuing with the next node",
                              logPrefix,
                              node,
                              t);
                        }
                        connect(nodes, newErrors, onSuccess, onFailure);
                      }
                    } else if (closeWasCalled || initFuture.isCancelled()) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but the control connection was closed, closing it",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      onSuccess.run();
                    } else if (exclusion != null) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but {}, closing and trying next node",
                          logPrefix,
                          channel,
                          exclusion);
                      channel.forceClose();
                      // Recorded for the same reason as the post-handshake exclusion below, and
                      // marked for the same reason: a plan drained entirely by exclusions has to
                      // report why rather than surface a bare NoNodeAvailableException, and the
                      // reconnection's failure callback has to be able to tell "everything was
                      // refused" from "nothing could be reached". No controlConnectionFailed event
                      // though -- nothing failed to connect.
                      List<Entry<Node, Throwable>> exclusionErrors =
                          (errors == null) ? new ArrayList<>() : errors;
                      exclusionErrors.add(
                          new SimpleEntry<>(node, new ExcludedNodeException(exclusion)));
                      connect(nodes, exclusionErrors, onSuccess, onFailure);
                    } else {
                      LOG.debug("[{}] New channel opened {}", logPrefix, channel);
                      DriverChannel previousChannel = ControlConnection.this.channel;
                      ControlConnection.this.channel = channel;
                      controlNodeState = new ControlNodeState(null, node);
                      if (previousChannel != null && previousChannel != channel) {
                        LOG.debug(
                            "[{}] Forcefully closing previous channel {}",
                            logPrefix,
                            previousChannel);
                        previousChannel.forceClose();
                      }
                      resolveChannelNodeIfNeeded(channel, (DefaultNode) node, capturedNodeInfo)
                          .whenCompleteAsync(
                              (resolvedNode, fetchError) -> {
                                if (fetchError != null) {
                                  controlNodeState = ControlNodeState.NONE;
                                  LOG.debug(
                                      "[{}] Failed to resolve control node endpoint from {}, "
                                          + "trying next node",
                                      logPrefix,
                                      node,
                                      fetchError);
                                  // Null out before forceClose() so that onChannelClosed() does not
                                  // start a redundant reconnection on top of the connect() retry
                                  // below.
                                  ControlConnection.this.channel = null;
                                  channel.forceClose();
                                  List<Entry<Node, Throwable>> newErrors =
                                      (errors == null) ? new ArrayList<>() : errors;
                                  newErrors.add(new SimpleEntry<>(node, fetchError));
                                  connect(nodes, newErrors, onSuccess, onFailure);
                                } else if (channel.closeFuture().isDone()) {
                                  controlNodeState = ControlNodeState.NONE;
                                  ControlConnection.this.channel = null;
                                  List<Entry<Node, Throwable>> newErrors =
                                      (errors == null) ? new ArrayList<>() : errors;
                                  newErrors.add(
                                      new SimpleEntry<>(
                                          node,
                                          new Exception("Channel closed during endpoint resolve")));
                                  connect(nodes, newErrors, onSuccess, onFailure);
                                } else {
                                  // The guards above ran against the node the query plan offered.
                                  // For a contact point appended by the reconnection fallback that
                                  // is an ephemeral instance which is never the subject of a
                                  // distance or state event, so it is never a key in either map and
                                  // those guards cannot have seen anything. Only now, once the
                                  // handshake has said which node actually answered, is there
                                  // something to ask about -- and asking matters, because nothing
                                  // downstream will: an unchanged distance fires no event, so a
                                  // control connection parked on an excluded node stays there.
                                  String resolvedExclusion = exclusionReason(resolvedNode);
                                  if (resolvedExclusion != null) {
                                    LOG.debug(
                                        "[{}] Channel {} turned out to be {}, which {}; "
                                            + "closing and trying next node",
                                        logPrefix,
                                        channel,
                                        resolvedNode,
                                        resolvedExclusion);
                                    controlNodeState = ControlNodeState.NONE;
                                    // Null out before forceClose(), as above, so that
                                    // onChannelClosed() does not start a redundant reconnection on
                                    // top of the connect() retry below.
                                    ControlConnection.this.channel = null;
                                    channel.forceClose();
                                    // Recorded, so that exhausting the plan this way reports why
                                    // rather than a bare NoNodeAvailableException. No
                                    // controlConnectionFailed event though: nothing failed to
                                    // connect, the node is simply not one we may use -- same as the
                                    // pre-handshake exclusion branch above.
                                    List<Entry<Node, Throwable>> newErrors =
                                        (errors == null) ? new ArrayList<>() : errors;
                                    newErrors.add(
                                        new SimpleEntry<>(
                                            resolvedNode,
                                            new ExcludedNodeException(resolvedExclusion)));
                                    connect(nodes, newErrors, onSuccess, onFailure);
                                  } else {
                                    controlNodeState = new ControlNodeState(resolvedNode, null);
                                    // Contained, because this callback is the only thing that
                                    // completes the round and it is not wrapped by the outer
                                    // catch (Exception) above -- that one guards the *outer*
                                    // whenCompleteAsync, a different stack. EventBus.fire() has no
                                    // try/catch of its own and RunOrSchedule.on(adminExecutor, ..)
                                    // runs listeners inline when already on the admin loop, so a
                                    // user NodeStateListener.onUp that throws would escape here,
                                    // skip onSuccess.run(), and leave the Reconnection parked in
                                    // ATTEMPT_IN_PROGRESS for good. The channel is open either
                                    // way; a listener's failure is not the connection's.
                                    try {
                                      context
                                          .getEventBus()
                                          .fire(ChannelEvent.channelOpened(resolvedNode));
                                    } catch (Throwable t) {
                                      Loggers.warnWithException(
                                          LOG,
                                          "[{}] Listener threw while handling channelOpened for {};"
                                              + " the control connection is up regardless",
                                          logPrefix,
                                          resolvedNode,
                                          t);
                                    }
                                    channel
                                        .closeFuture()
                                        .addListener(
                                            f ->
                                                adminExecutor
                                                    .submit(
                                                        () ->
                                                            onChannelClosed(channel, resolvedNode))
                                                    .addListener(UncaughtExceptions::log));
                                    onSuccess.run();
                                  }
                                }
                              },
                              adminExecutor);
                    }
                  } catch (Exception e) {
                    Loggers.warnWithException(
                        LOG,
                        "[{}] Unexpected exception while processing channel init result",
                        logPrefix,
                        e);
                  }
                },
                adminExecutor);
      }
    }

    /**
     * Why {@code candidate} must not be used for the control connection -- the load balancing
     * policy has excluded it, or a topology event has -- or {@code null} if nothing rules it out.
     *
     * <p>Both maps are keyed on the {@link Node} instance and filled only from events, so a node
     * that has never been the subject of one is simply absent, and absent means "nothing known
     * against it" rather than "fine". That distinction is why this is asked twice per connect: once
     * about the node the query plan offered, and again about the node the handshake proved is at
     * the other end, which for a contact point is a different instance.
     *
     * <p>Keying on the instance means this can only answer for a node the driver still holds. For a
     * node it does not -- one the monitor removed, which a contact point can lead back to -- see
     * {@link #exclusionReasonForHostId}.
     */
    private String exclusionReason(Node candidate) {
      if (lastNodeDistance.get(candidate) == NodeDistance.IGNORED) {
        return "node became ignored";
      }
      if (lastNodeState.containsKey(candidate)) {
        NodeState state = lastNodeState.get(candidate);
        if (state == null /*(removed)*/ || state == NodeState.FORCED_DOWN) {
          return "node was removed or forced down";
        }
      }
      return null;
    }

    /**
     * Why the node answering under {@code hostId} must not be used for the control connection, or
     * {@code null} if nothing rules it out.
     *
     * <p>The host-id-keyed counterpart of {@link #exclusionReason}, for the one moment that has to
     * be settled <b>before</b> {@code MetadataManager#registerNode}: a contact point whose DNS
     * record still lists a node the topology monitor has removed. Registering first would publish
     * that node back into {@code Metadata#getNodes()} and fire {@code NodeStateListener#onAdd} for
     * it, and the instance-keyed check could not then undo it -- registerNode returns a brand-new
     * {@link DefaultNode}, which is in neither event map. That is exactly the resurrection {@code
     * TopologyMonitor#reresolvesNodeAddresses()} describes, reachable through the contact-point
     * reconnection fallback whatever that flag says.
     *
     * <p>A host id the driver still has in metadata is deferred to {@link #exclusionReason}, so
     * IGNORED and FORCED_DOWN keep being reported with their own wording. A host id that is in
     * neither metadata nor {@link #removedHostIds} is simply new -- a node that joined while the
     * driver was disconnected, which the fallback exists to reach -- and is allowed through.
     */
    @Nullable
    private String exclusionReasonForHostId(@Nullable UUID hostId) {
      if (hostId == null) {
        // registerNode's own precondition reports this better than a generic exclusion would.
        return null;
      }
      Node known = context.getMetadataManager().getMetadata().getNodes().get(hostId);
      if (known != null) {
        return exclusionReason(known);
      }
      return removedHostIds.contains(hostId) ? "node was removed" : null;
    }

    /**
     * {@link #exclusionReasonForHostId} as a plain map, so that a connect hook can ask it from a
     * channel's event loop.
     *
     * <p>The hook is where a contact point's identity is settled while its remaining addresses are
     * still on hand, and refusing there costs one address instead of the whole plan entry -- but
     * the state the answer comes from cannot be read there. {@code lastNodeDistance} and {@code
     * lastNodeState} are {@link WeakHashMap}s, whose {@code get} expunges cleared entries and so
     * writes; {@code removedHostIds} is a plain {@link LinkedHashMap}-backed set the admin thread
     * mutates. All three are confined to {@code adminExecutor}, which is where this runs.
     *
     * <p>Enumerated by calling {@link #exclusionReasonForHostId} rather than by restating what it
     * does, so the two cannot drift: every host id it can answer non-null for is a key of metadata
     * or of {@code removedHostIds}, and both are asked. It also keeps no {@link Node} out of the
     * weak maps -- the values are strings -- so a snapshot outliving a connect cannot pin a node
     * that metadata has dropped.
     *
     * <p>Not usually empty, and worth knowing how big it can get: {@link #exclusionReason} answers
     * for an {@code IGNORED} node as well as a removed or forced-down one, so with a local
     * datacenter configured this is every node of every remote datacenter. That is a steady state
     * rather than a transient, and it is what the hook then refuses one address at a time -- the
     * cost of settling identity where the remaining addresses are still on hand, paid on the
     * contact points whose records point at an excluded node.
     */
    private Map<UUID, String> excludedHostIds() {
      assert adminExecutor.inEventLoop();
      Set<UUID> candidates =
          new HashSet<>(context.getMetadataManager().getMetadata().getNodes().keySet());
      candidates.addAll(removedHostIds);
      Map<UUID, String> excluded = new HashMap<>();
      for (UUID hostId : candidates) {
        String reason = exclusionReasonForHostId(hostId);
        if (reason != null) {
          excluded.put(hostId, reason);
        }
      }
      return excluded.isEmpty() ? Collections.emptyMap() : Collections.unmodifiableMap(excluded);
    }

    /**
     * Options for one connect attempt against one query-plan node. Built fresh per attempt rather
     * than cached: an attempt against an unidentified node carries a stateful holder that the
     * connect hook fills, and overlapping connect chains are reachable -- the initial {@code
     * connect()} runs outside {@code Reconnection} (which serializes only its own attempts), and
     * {@code reconnectNow()} checks only {@code initWasCalled} -- so nothing stateful may be shared
     * between attempts.
     */
    private DriverChannelOptions buildChannelOptions(Node node, NodeInfoHolder capturedNodeInfo) {
      DriverChannelOptions.Builder builder =
          DriverChannelOptions.builder()
              .withEvents(eventTypes, ControlConnection.this)
              .withOwnerLogPrefix(logPrefix + "|control")
              .reportConfig(true);
      if (node.getHostId() == null) {
        // A contact point: the driver does not yet know which node answers at each of its
        // addresses, so the identity read happens through the connect hook, inside the factory's
        // candidate loop, where the hostname's other addresses are still on hand and a rejection
        // costs one of them. Read after the connect instead, the candidates are already gone and a
        // failure writes off the whole plan entry for that round.
        //
        // Both criteria are applied there: that the node identifies itself at all, and that the
        // host id it gives is one this connection may use. The second needs state the hook's thread
        // cannot read, so it is snapshotted here, on the admin loop -- see #excludedHostIds. Taken
        // per attempt, which is also when the options are built, so it is as current as the attempt
        // is; an event landing mid-connect is caught by the second, live check in
        // #resolveChannelNodeIfNeeded.
        // Doubled, so that the hook's bound is strictly looser than the deadline of the query it
        // wraps. The stage this bounds is a system.local read, which AdminRequestHandler already
        // times out on CONTROL_CONNECTION_TIMEOUT -- and DefaultTopologyMonitor snapshots that
        // option in its constructor while this reads it live, so handing over the same value gives
        // the two deadlines no defined order at all. Whichever wins decides what the operator is
        // told: "Connect hook timed out" names the wrapper, the inner DriverTimeoutException names
        // the query and the node. A margin makes the inner one win, and makes this what it is meant
        // to be -- a backstop against a hook that never completes, which a custom TopologyMonitor
        // can produce and the built-in one cannot. It also absorbs a runtime reload that lowers the
        // option (reference.conf documents it as not runtime-modifiable, but nothing enforces that
        // and getDuration genuinely re-reads); without one, a lowered value abandons every
        // candidate of every contact point before its read can finish, and the control connection
        // can no longer come up through the contact-point fallback at all.
        //
        // At zero the margin has nothing to scale: doubling gives zero, and ChannelFactory reads a
        // non-positive hook timeout as "no timeout", the way every other consumer of a driver
        // timeout option does. So an operator who disables this option disables the backstop and
        // the deadline of the query it wraps in one stroke -- and unlike the init-query timeout,
        // where ProtocolInitHandler reads the same option and so STARTUP fails first, nothing else
        // in the connect path reads this one. What is left bounding a candidate that connects and
        // then never answers system.local is the heartbeat, and nothing at all if
        // advanced.heartbeat.interval is zero too. That is the exposure the read already had before
        // it moved behind a hook -- DefaultTopologyMonitor has always taken its query timeout from
        // this option -- so it is left alone rather than given a floor that would contradict the
        // option's own convention.
        Duration hookTimeout =
            config
                .getDefaultProfile()
                .getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT)
                .multipliedBy(2);
        Map<UUID, String> excludedHostIds = excludedHostIds();
        builder.withConnectHook(
            channel -> readChannelNodeInfo(channel, capturedNodeInfo, excludedHostIds),
            hookTimeout);
      }
      return builder.build();
    }

    /**
     * The connect hook of a contact-point attempt: reads which node answered and channels it
     * straight into the attempt's holder, rejecting the candidate when the node cannot identify
     * itself, or identifies itself as one this connection may not use.
     *
     * <p>Runs on the channel's event loop and touches no {@code SingleThreaded} state: the holder
     * is the only thing written, and it is read back on the admin thread only after the connect
     * completes. {@code excludedHostIds} was snapshotted on the admin thread when this attempt's
     * options were built, precisely so that nothing here has to read the collections it came from.
     *
     * <p>Rejecting here rather than after the connect is what confines the cost of an exclusion to
     * the one address that hit it. {@code ChannelFactory} does not treat an {@code
     * ExcludedNodeException} as node-wide, so the candidate loop moves on to the hostname's next
     * address -- which, for a contact point whose DNS still lists a node the monitor removed, is
     * quite likely a node it may use.
     */
    private CompletionStage<Void> readChannelNodeInfo(
        DriverChannel channel, NodeInfoHolder capturedNodeInfo, Map<UUID, String> excludedHostIds) {
      TopologyMonitor topologyMonitor = context.getTopologyMonitor();
      // Before the read, not after a rejection. DefaultTopologyMonitor#getChannelNodeInfo warms its
      // system.local column projection from the response, and the projection is an *intersection*:
      // it can only shrink. Until this hook existed the read only ever ran against the channel the
      // control connection kept, so what it learned was by construction the accepted node's. It now
      // runs once per candidate address, and the candidate is not known to be kept when the read
      // returns -- ChannelFactory can still abandon it on a REGISTER rejection, or on the hook
      // timeout, and #resolveChannelNodeIfNeeded re-asks about the node once the channel is open.
      //
      // Undoing it on each of those paths instead cannot work, and it is worth saying why, because
      // it is the obvious shape: none of them can see the projection a *previous* candidate left
      // behind, so an address refused after its read would still be narrowing what the next one --
      // accepted -- goes on to report. Two of them are ChannelFactory's and invisible here anyway.
      // Clearing first needs none of that: every read becomes a SELECT * that re-learns from
      // whoever answered it. It also drops a projection learned from the *previous* control node on
      // a reconnect, which would otherwise be applied to a node that need not carry those columns
      // at all. What survives is the last candidate to *answer*, which is not automatically the one
      // the loop keeps -- an abandoned candidate is not cancelled, so its response can land on
      // either side of the accepted one's, or after this attempt has finished with it. Two of those
      // three orders are closed, at the two ends: the reset here, and the reset in front of
      // #resolveChannelNodeIfNeeded's fallback read. DefaultTopologyMonitor#toLocalNodeInfo spells
      // out which one is left and why closing it needs the projection keyed to its channel.
      //
      // Narrow deliberately: the hook reads system.local and nothing else, so the peer projections
      // cannot be what it narrowed, and re-learning them would cost a SELECT * over every peer row.
      // The wide #resetColumnCaches stays what a reconnect calls, where the cluster itself may have
      // changed.
      topologyMonitor.resetLocalColumnCache();
      return topologyMonitor
          .getChannelNodeInfo(channel)
          .thenAccept(
              nodeInfo -> {
                // Mirrors DefaultTopologyMonitor's own precondition, so that a custom monitor
                // cannot smuggle a null past registerNode: rejecting here costs one address, while
                // failing in registerNode later would cost the whole plan entry.
                Objects.requireNonNull(
                    nodeInfo.getHostId(),
                    "Node info is missing its host id; the node may still be bootstrapping");
                String exclusion = excludedHostIds.get(nodeInfo.getHostId());
                if (exclusion != null) {
                  throw new ExcludedNodeException(exclusion);
                }
                capturedNodeInfo.set(channel, nodeInfo);
              });
    }

    /**
     * Resolves the identity of the node at the other end of the channel. For nodes that already
     * have a hostId, returns the node as-is. For a contact point, the connect hook has already read
     * {@code system.local} and captured the result (see {@link #readChannelNodeInfo}); this
     * registers a new metadata node from it.
     */
    private CompletionStage<Node> resolveChannelNodeIfNeeded(
        DriverChannel channel, DefaultNode node, NodeInfoHolder capturedNodeInfo) {
      if (node.getHostId() != null) {
        return CompletableFuture.completedFuture(node);
      }
      NodeInfo captured = capturedNodeInfo.getFor(channel);
      // The pairing with the channel is asserted rather than assumed, and a miss falls back to a
      // direct read: a ChannelFactory subclass that does not run the connect hook still gets a
      // functioning control connection (and the mocked factories in the unit tests exercise this
      // same path). The fallback costs one extra round trip, on that path only.
      //
      // Cleared before that read as well, for the same reason #readChannelNodeInfo clears before
      // its own -- and this is the path that most needs it. A miss means the last capture came
      // from some other channel, which is precisely what a stranded candidate's late write does,
      // so the projection in the cache is the one *its* read warmed. Identifying the node the
      // driver is keeping through a projection intersected against one it refused is the whole
      // failure this reset exists to prevent. It is also what makes
      // TopologyMonitor#resetLocalColumnCache's "before every one of these reads" true rather than
      // true of the hook only.
      CompletionStage<NodeInfo> nodeInfoFuture;
      if (captured != null) {
        nodeInfoFuture = CompletableFuture.completedFuture(captured);
      } else {
        TopologyMonitor topologyMonitor = context.getTopologyMonitor();
        topologyMonitor.resetLocalColumnCache();
        nodeInfoFuture = topologyMonitor.getChannelNodeInfo(channel);
      }
      return nodeInfoFuture.thenComposeAsync(
          nodeInfo -> {
            // Asked before registerNode, not after: registration is what publishes a node into
            // Metadata#getNodes() and fires NodeStateListener#onAdd, and for a host id the driver
            // does not know it *creates* the node. Deciding afterwards would mean resurrecting a
            // node the topology monitor has removed and only then refusing it -- and refusing it
            // would not even work, since the instance registerNode just minted is not a key in
            // either event map (see #exclusionReason).
            //
            // Asked again, rather than only in the connect hook: the hook goes on a snapshot taken
            // before the connect, so a removal or a distance change that landed while it was in
            // flight is not in it. This read is live. The hook is what keeps an exclusion from
            // costing the whole plan entry; this is what keeps the answer current.
            String exclusion = exclusionReasonForHostId(nodeInfo.getHostId());
            if (exclusion != null) {
              return CompletableFutures.<Node>failedFuture(new ExcludedNodeException(exclusion));
            }
            EndPoint resolvedEp = nodeInfo.getEndPoint();
            EndPoint channelEp = channel.getEndPoint();
            // The channel adopts the node's endpoint so that everything reading it afterwards --
            // DefaultTopologyMonitor's localEndPoint on the next refresh, refreshNode's
            // control-node
            // check, OptionalLocalDcHelper's endpoint fallback -- sees the same instance the node
            // holds, rather than the contact point this connection happened to come up through.
            //
            // Pinned to the address this channel actually reached, though, because the node's own
            // endpoint need not name one: SniEndPoint and ClientRoutesEndPoint hand out a *name* by
            // design and re-expand it per connect. Adopting such an endpoint unpinned would make
            // channel.getEndPoint().resolve() the shared proxy name, which every SniEndPoint in the
            // cluster equals -- so #isControlNode's resolve() comparison would answer true for any
            // node, and JAVA-2303's self-peer guard (broadcastRpcAddress.equals(localEndPoint
            // .resolve())) would stop matching, an unresolved address never equalling a resolved
            // one.
            //
            // pinTo() is a no-op when there is nothing to pin to, and that case is worth naming
            // rather than glossing: both implementations decline an unresolved address, as does
            // ChannelFactory#pin, whose javadoc explains why -- pinning to a name freezes an
            // endpoint on something that must re-expand. The channel can carry such an address
            // when the resolver passed the name through, which resolveCandidates deliberately
            // allows for a NoopAddressResolverGroup behind a ProxyHandler, or for a custom resolver
            // that reports the name already resolved. The adoption then stores the unpinned
            // endpoint -- but both failures above are already live in that configuration whatever
            // this line does, because they follow from the channel's address being unresolved and
            // the channel's own endpoint is equally unpinned. Skipping the adoption would buy none
            // of it back and would lose the node identity this exists to carry, so the fix belongs
            // where the unresolved address is accepted; deferred there.
            //
            // The same test as DefaultNode#setEndPoint, and deliberately not equals(): this is
            // exactly the mixed unresolved-vs-resolved case (see PinnableEndPoint#sameIdentity).
            if (resolvedEp != null
                && resolvedEp != channelEp
                && !PinnableEndPoint.sameIdentity(resolvedEp, channelEp)) {
              EndPoint adopted =
                  (resolvedEp instanceof PinnableEndPoint)
                      ? ((PinnableEndPoint) resolvedEp).pinTo(channelEp.resolve())
                      : resolvedEp;
              channel.setEndPoint(adopted);
              LOG.debug("[{}] Control channel endpoint upgraded to {}", logPrefix, adopted);
            }
            return context.getMetadataManager().registerNode(nodeInfo);
          },
          adminExecutor);
    }

    private void onSuccessfulReconnect() {
      assert adminExecutor.inEventLoop();
      // A round got through, so the count of rounds that did not is no longer consecutive. Reset it
      // here rather than only on the failure path, so a session that alternates between a refused
      // round and a good one never accumulates its way to a spurious clear.
      consecutiveAllExcludedRounds = 0;
      // If reconnectOnFailure was true and we've never connected before, complete the future now to
      // signal that the initialization is complete. Schema refresh and LBP initialization for the
      // first connection are handled by the session initialization path (DefaultSession.init), not
      // here, so we skip the full refresh below.
      boolean isFirstConnection = initFuture.complete(null);
      if (isFirstConnection) {
        return;
      }

      // Otherwise, perform a full refresh (we don't know how long we were disconnected)
      // Reset any cached column projections so the next topology refresh re-learns what
      // columns are available via SELECT * (the cluster may have changed after reconnect).
      context.getTopologyMonitor().resetColumnCaches();

      // If client routes are active, wait for the routes refresh to complete before refreshing
      // nodes, so that buildNodeEndPoint sees up-to-date route data.
      CompletionStage<Void> routesReady;
      if (context.getTopologyMonitor() instanceof ClientRoutesTopologyMonitor) {
        routesReady = ((ClientRoutesTopologyMonitor) context.getTopologyMonitor()).refresh();
      } else {
        routesReady = CompletableFuture.completedFuture(null);
      }

      routesReady.whenComplete(
          (routesResult, routesError) -> {
            if (routesError != null) {
              LOG.debug(
                  "[{}] Error while refreshing client routes on reconnect", logPrefix, routesError);
            }
            context
                .getMetadataManager()
                .refreshNodes()
                .whenCompleteAsync(
                    (result, error) -> {
                      assert adminExecutor.inEventLoop();
                      if (error != null) {
                        LOG.debug("[{}] Error while refreshing node list", logPrefix, error);
                      } else {
                        try {
                          // A failed node list refresh at startup is not fatal, so this might
                          // be the first successful refresh; make sure the LBP gets initialized
                          // (this is a no-op if it was initialized already).
                          context.getLoadBalancingPolicyWrapper().init();
                          Node controlNode = controlNodeState.current;
                          if (controlNode != null && controlNode.getHostId() != null) {
                            if (!context
                                .getMetadataManager()
                                .getMetadata()
                                .getNodes()
                                .containsKey(controlNode.getHostId())) {
                              LOG.debug(
                                  "[{}] Control node {} is no longer in metadata after "
                                      + "reconnect refresh, triggering reconnection",
                                  logPrefix,
                                  controlNode);
                              controlNodeState = ControlNodeState.NONE;
                              DriverChannel ch = ControlConnection.this.channel;
                              ControlConnection.this.channel = null;
                              if (ch != null) {
                                ch.forceClose();
                              }
                              reconnection.start();
                              return;
                            }
                          }
                          context
                              .getMetadataManager()
                              .refreshSchema(null, false, true)
                              .whenComplete(
                                  (metadata, schemaError) -> {
                                    if (schemaError != null) {
                                      Loggers.warnWithException(
                                          LOG,
                                          "[{}] Unexpected error while refreshing schema after"
                                              + " a successful reconnection, keeping previous"
                                              + " version",
                                          logPrefix,
                                          schemaError);
                                    }
                                  });
                        } catch (Throwable t) {
                          Loggers.warnWithException(
                              LOG,
                              "[{}] Unexpected error on control connection reconnect",
                              logPrefix,
                              t);
                        }
                      }
                    },
                    adminExecutor);
          });
    }

    private void onChannelClosed(DriverChannel channel, Node node) {
      assert adminExecutor.inEventLoop();
      if (!closeWasCalled) {
        if (channel == ControlConnection.this.channel) {
          controlNodeState = ControlNodeState.NONE;
        }
        context.getEventBus().fire(ChannelEvent.channelClosed(node));
        // If this channel is the current control channel, we must start a
        // reconnection attempt to get a new control channel.
        if (channel == ControlConnection.this.channel) {
          LOG.debug(
              "[{}] The current control channel {} was closed, scheduling reconnection",
              logPrefix,
              channel);
          reconnection.start();
        } else {
          LOG.trace(
              "[{}] A previous control channel {} was closed, reconnection not required",
              logPrefix,
              channel);
        }
      }
    }

    private void reconnectNow() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled && !closeWasCalled) {
        reconnection.reconnectNow(true);
      }
    }

    private boolean isControlNode(Node eventNode) {
      ControlNodeState state = controlNodeState;
      if (state.current != null
          && eventNode.getHostId() != null
          && eventNode.getHostId().equals(state.current.getHostId())) {
        return true;
      }
      if (state.current == null && state.pending != null) {
        // Resolution is still in flight, so there is no host id to compare yet and the endpoint is
        // all there is to go on. The channel's own endpoint is what to compare against: unlike the
        // pending node's, ChannelFactory has bound it to the one address the connection actually
        // went to.
        DriverChannel pendingChannel = ControlConnection.this.channel;
        if (pendingChannel == null) {
          return false;
        }
        EndPoint eventEndPoint = eventNode.getEndPoint();
        EndPoint channelEndPoint = pendingChannel.getEndPoint();
        // Two lookup-free comparisons, because neither shape is covered by the other.
        //
        // resolve() settles it when both sides hold a concrete address: the event carries a
        // metadata
        // node whose endpoint is a resolved IP, and the channel's is pinned to the IP it reached.
        // This is the case the plain hostname contact point hits, and comparing resolve() results
        // rather than the endpoints keeps DefaultEndPoint#equals -- which resolves the unresolved
        // side of a mixed comparison, i.e. a blocking DNS lookup on the admin thread, on an
        // arbitrary single address (issue #1006) -- off a path that runs for every distance or
        // state
        // event arriving during a control connect.
        if (Objects.equals(eventEndPoint.resolve(), channelEndPoint.resolve())) {
          return true;
        }
        // But resolve() cannot settle it when the endpoint's current address is a *name*, which is
        // the permanent state of an SNI proxy address, of a client route, of anything a custom
        // AddressTranslator hands over unresolved -- and, now that contact points are kept
        // unresolved, of a plain hostname contact point until its node adopts the endpoint built
        // from system.local. The event node resolves to that unresolved name while the channel
        // resolves to the IP it was pinned to, and an InetSocketAddress carrying an InetAddress
        // never equals one that does not.
        //
        // asMetricPrefix(), not equals(). Both answer this without resolving for the endpoints that
        // key their identity on something other than the current address -- SniEndPoint on proxy +
        // serverName, ClientRoutesEndPoint on the host id, both of which a Cloud contact point and
        // its metadata node share -- but "does not resolve" is a property of equals() that only
        // *this driver's* implementations have, and the one that does not (DefaultEndPoint#equals,
        // issue #1006) cannot be told apart from a third-party one written the same way. Naming it
        // by class, as this did, denylists the single instance of the hazard we happen to ship and
        // walks a custom endpoint straight into it -- a blocking lookup on the admin thread, which
        // is the one thing this branch exists to prevent.
        //
        // The prefix has no such caveat: it is contractually a short path-like string, and the
        // driver already calls it for every node on every topology refresh through
        // PinnableEndPoint#sameIdentity, so a resolving implementation of it is already broken
        // elsewhere and more loudly. It also settles the case the class check had to give up on:
        // a pinned copy carries the original's prefix by PinnableEndPoint's contract, so a
        // hostname contact point's node and the channel pinned to one of its addresses match here
        // -- where the old test answered false and left an IGNORED or forced-down control node
        // reached through a hostname without the reconnect its callers below exist to trigger.
        //
        // What the prefix does not settle, and neither did the equals() this replaced, is a
        // deployment where unrelated nodes share one: an AddressTranslator that hands back a name
        // gives every node it covers the same DefaultEndPoint, and if a contact point is that same
        // name then any node's event matches the pending channel here. Both tests collide on
        // exactly the same input -- host string plus port -- so this is inherited rather than
        // introduced, and the cost is a reconnectNow() that restarts an in-flight control connect
        // rather than a wrong answer about identity. Settling it needs the host id, which is what
        // the branch above this one uses and what a pending channel does not have yet.
        //
        // Not the same predicate as sameIdentity, deliberately: that one *also* compares resolve(),
        // because a node must not stay on a stale pin. Here the two sides are a node and a channel,
        // and their addresses differing by exactly that pin is the normal case.
        return eventEndPoint.getClass() == channelEndPoint.getClass()
            && eventEndPoint.asMetricPrefix().equals(channelEndPoint.asMetricPrefix());
      }
      return false;
    }

    private void onDistanceEvent(DistanceEvent event) {
      assert adminExecutor.inEventLoop();
      this.lastNodeDistance.put(event.node, event.distance);
      if (event.distance == NodeDistance.IGNORED
          && channel != null
          && !channel.closeFuture().isDone()
          && isControlNode(event.node)) {
        LOG.debug(
            "[{}] Control node {} became IGNORED, reconnecting to a different node",
            logPrefix,
            event.node);
        reconnectNow();
      }
    }

    private void onStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      this.lastNodeState.put(event.node, event.newState);
      UUID hostId = event.node.getHostId();
      if (hostId != null) {
        if (event.newState == null /*(removed)*/) {
          removedHostIds.add(hostId);
        } else {
          removedHostIds.remove(hostId);
        }
      }
      if ((event.newState == null /*(removed)*/ || event.newState == NodeState.FORCED_DOWN)
          && channel != null
          && !channel.closeFuture().isDone()
          && isControlNode(event.node)) {
        LOG.debug(
            "[{}] Control node {} was removed or forced down, reconnecting to a different node",
            logPrefix,
            event.node);
        reconnectNow();
      }
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Starting shutdown", logPrefix);
      reconnection.stop();
      if (channel == null) {
        LOG.debug("[{}] Shutdown complete", logPrefix);
        closeFuture.complete(null);
      } else {
        channel
            .forceClose()
            .addListener(
                f -> {
                  if (f.isSuccess()) {
                    LOG.debug("[{}] Shutdown complete", logPrefix);
                    closeFuture.complete(null);
                  } else {
                    closeFuture.completeExceptionally(f.cause());
                  }
                });
      }
    }
  }

  /**
   * Whether every contact point failed for the one reason worth telling the operator to go and fix
   * their configuration over: bad credentials, everywhere.
   *
   * <p>Each entry is tested with {@link ChannelFactory#isAuthOnly} rather than a bare {@code
   * instanceof}, because one entry no longer means one address. {@code ChannelFactory} expands a
   * contact-point hostname to every address it resolves to and reports a single failure for the
   * name, with the other addresses' failures attached as suppressed exceptions. Looking only at the
   * top-level throwable would call a name whose records failed {@code [refused, refused, auth]} an
   * authentication failure, and claim in the log that authentication is what is wrong with the
   * deployment when two thirds of it is unreachable.
   */
  @VisibleForTesting
  static boolean isAuthFailure(Throwable error) {
    if (!(error instanceof AllNodesFailedException)) {
      // Anything else carries no per-node breakdown to inspect, so there is nothing here that says
      // every contact point rejected the credentials.
      return false;
    }
    Collection<List<Throwable>> errors = ((AllNodesFailedException) error).getAllErrors().values();
    if (errors.isEmpty()) {
      return false;
    }
    // An excluded node is skipped rather than allowed to veto. It was never asked for credentials,
    // so it is no evidence either way -- and letting it answer would hide a genuine credential
    // problem behind one node that happened to be IGNORED or forced down. If skipping leaves
    // nothing, the round tried nobody and there is no verdict to report.
    //
    // Only when the exclusion is the whole story for that node, though: a contact point whose
    // addresses went [excluded, auth] was asked for credentials, on the address that reached a
    // server, and skipping it would drop the only evidence there is.
    //
    // Which is also why that node is then tested with the two-argument isAuthOnly, passing the
    // same exclusion test. The plain one walks the suppressed failures and rejects anything that
    // is not an AuthenticationException -- so it would find the excluded sibling, answer false,
    // and return false for the whole round. Setting exclusions aside on both sides is what makes
    // the skip above mean anything.
    boolean anyTried = false;
    for (List<Throwable> nodeErrors : errors) {
      for (Throwable nodeError : nodeErrors) {
        if (isExclusionOnly(nodeError)) {
          continue;
        }
        if (!ChannelFactory.isAuthOnly(nodeError, ControlConnection::isExcluded)) {
          return false;
        }
        anyTried = true;
      }
    }
    return anyTried;
  }

  /**
   * What one contact-point connect attempt learned about the node that answered: filled by the
   * connect hook on the channel's event loop, read back on the admin thread once the connect
   * completes. One instance per attempt -- overlapping attempts must not share it, which is why
   * {@code DriverChannelOptions} are built per attempt.
   */
  @VisibleForTesting
  static final class NodeInfoHolder {

    /**
     * The two values are published as one reference so that a reader can never see one candidate's
     * node info paired with another's channel. Two separate volatile fields would not do: the
     * factory's candidate loop can leave a stranded hook behind -- an attempt abandoned on the hook
     * timeout, whose {@code system.local} response then arrives anyway -- and its late write would
     * land in between the accepted candidate's two writes. The admin thread reading in that window
     * would take the rejected candidate's node info as the accepted channel's, and the control
     * connection would then register the wrong host id and endpoint for the node it is talking to.
     *
     * <p>With the pair atomic, that late write merely makes {@link #getFor} miss, which falls back
     * to reading {@code system.local} again on the channel that is actually open.
     */
    private volatile Capture capture;

    void set(DriverChannel channel, NodeInfo nodeInfo) {
      this.capture = new Capture(channel, nodeInfo);
    }

    /** The captured info if it came from {@code channel}: the pairing is asserted, not assumed. */
    NodeInfo getFor(DriverChannel channel) {
      Capture current = this.capture;
      return (current != null && current.channel == channel) ? current.nodeInfo : null;
    }

    private static final class Capture {
      final DriverChannel channel;
      final NodeInfo nodeInfo;

      Capture(DriverChannel channel, NodeInfo nodeInfo) {
        this.channel = channel;
        this.nodeInfo = nodeInfo;
      }
    }
  }

  /**
   * Immutable snapshot of the control node state. Reads from any thread see a consistent pair of
   * (current, pending) via a single volatile read of the enclosing reference.
   */
  static final class ControlNodeState {
    static final ControlNodeState NONE = new ControlNodeState(null, null);

    /**
     * The resolved control node, or {@code null} if resolution is pending or no channel is open.
     */
    final Node current;

    /** The node whose channel is open but not yet resolved, or {@code null} otherwise. */
    final Node pending;

    ControlNodeState(Node current, Node pending) {
      this.current = current;
      this.pending = pending;
    }
  }

  private static ImmutableList<String> buildEventTypes(
      boolean listenClusterEvents, boolean listenClientRoutesEvents) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    builder.add(ProtocolConstants.EventType.SCHEMA_CHANGE);
    if (listenClusterEvents) {
      builder
          .add(ProtocolConstants.EventType.STATUS_CHANGE)
          .add(ProtocolConstants.EventType.TOPOLOGY_CHANGE);
    }
    if (listenClientRoutesEvents) {
      builder.add(ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE);
    }
    return builder.build();
  }
}
