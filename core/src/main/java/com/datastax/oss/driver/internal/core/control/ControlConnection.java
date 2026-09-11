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
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.EventCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.ClientRoutesUpdateEvent;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.collection.CompositeQueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.event.ClientRoutesChangeEvent;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.event.TopologyChangeEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
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

  private final InternalDriverContext context;
  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final Random random;
  private final SingleThreaded singleThreaded;

  // The single channel used by this connection. This field is accessed concurrently, but only
  // mutated on adminExecutor (by SingleThreaded methods)
  private volatile DriverChannel channel;

  public ControlConnection(InternalDriverContext context) {
    this(context, new Random());
  }

  /** {@code random} decides the order in which a contact point's resolved addresses are tried. */
  @VisibleForTesting
  ControlConnection(InternalDriverContext context, Random random) {
    this.context = context;
    this.logPrefix = context.getSessionName();
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.random = random;
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

  private class SingleThreaded {
    private final InternalDriverContext context;
    private final DriverConfig config;
    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final ReconnectionPolicy reconnectionPolicy;
    private final Reconnection reconnection;
    private DriverChannelOptions channelOptions;
    private volatile ControlNodeState controlNodeState = ControlNodeState.NONE;
    // The last events received for each node
    private final Map<Node, NodeDistance> lastNodeDistance = new WeakHashMap<>();
    private final Map<Node, NodeState> lastNodeState = new WeakHashMap<>();

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
        ImmutableList<String> eventTypes =
            buildEventTypes(listenToClusterEvents, listenClientRoutesEvents);
        LOG.debug("[{}] Initializing with event types {}", logPrefix, eventTypes);
        channelOptions =
            DriverChannelOptions.builder()
                .withEvents(eventTypes, ControlConnection.this)
                .withOwnerLogPrefix(logPrefix + "|control")
                .reportConfig(true)
                .build();

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
            result.complete(false);
          });
      return result;
    }

    private void connect(
        Queue<Node> nodes,
        List<Entry<Node, Throwable>> errors,
        Runnable onSuccess,
        Consumer<Throwable> onFailure) {
      connect(nodes, errors, onSuccess, onFailure, true);
    }

    /**
     * @param expandContactPoints whether the next node, if it is a contact point given as a
     *     hostname, is first expanded to every address the name resolves to (see {@link
     *     #expandContactPoint}). {@code false} only when that expansion was just attempted for it
     *     and yielded nothing usable: the node is then tried as it is, the way every connect did
     *     before expansion existed.
     */
    private void connect(
        Queue<Node> nodes,
        List<Entry<Node, Throwable>> errors,
        Runnable onSuccess,
        Consumer<Throwable> onFailure,
        boolean expandContactPoints) {
      assert adminExecutor.inEventLoop();
      Node node = nodes.poll();
      if (node == null) {
        onFailure.accept(AllNodesFailedException.fromErrors(errors));
      } else if (expandContactPoints && isExpandableContactPoint(node)) {
        expandContactPoint(node, nodes, errors, onSuccess, onFailure);
      } else {
        LOG.debug("[{}] Trying to establish a connection to {}", logPrefix, node);
        context
            .getChannelFactory()
            .connect(node, channelOptions)
            .whenCompleteAsync(
                (channel, error) -> {
                  try {
                    NodeDistance lastDistance = lastNodeDistance.get(node);
                    NodeState lastState = lastNodeState.get(node);
                    if (error != null) {
                      if (closeWasCalled || initFuture.isCancelled()) {
                        onSuccess.run(); // abort, we don't really care about the result
                      } else {
                        if (error instanceof AuthenticationException) {
                          Loggers.warnWithException(
                              LOG, "[{}] Authentication error", logPrefix, error);
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
                        context.getEventBus().fire(ChannelEvent.controlConnectionFailed(node));
                        connect(nodes, newErrors, onSuccess, onFailure);
                      }
                    } else if (closeWasCalled || initFuture.isCancelled()) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but the control connection was closed, closing it",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      onSuccess.run();
                    } else if (lastDistance == NodeDistance.IGNORED) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but node became ignored, "
                              + "closing and trying next node",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      connect(nodes, errors, onSuccess, onFailure);
                    } else if (lastNodeState.containsKey(node)
                        && (lastState == null /*(removed)*/
                            || lastState == NodeState.FORCED_DOWN)) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but node was removed or forced down, "
                              + "closing and trying next node",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      connect(nodes, errors, onSuccess, onFailure);
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
                      resolveChannelNodeIfNeeded(channel, (DefaultNode) node)
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
                                } else if (isUnusableForControl(resolvedNode)) {
                                  // Events name the identified node, not the placeholder we
                                  // dialled, so isControlNode() cannot match them without a
                                  // blocking endpoint lookup. Re-checked here instead.
                                  controlNodeState = ControlNodeState.NONE;
                                  LOG.debug(
                                      "[{}] New channel opened ({}) but {} is ignored, removed "
                                          + "or forced down, closing and trying next node",
                                      logPrefix,
                                      channel,
                                      resolvedNode);
                                  // Null out before forceClose() so that onChannelClosed() does not
                                  // start a redundant reconnection on top of the connect() retry
                                  // below.
                                  ControlConnection.this.channel = null;
                                  channel.forceClose();
                                  // Recorded like every other drop; on init this list is what the
                                  // user sees. A reconnection discards it, leaving the debug log.
                                  List<Entry<Node, Throwable>> newErrors =
                                      (errors == null) ? new ArrayList<>() : errors;
                                  newErrors.add(
                                      new SimpleEntry<>(
                                          node,
                                          new Exception(
                                              "Control node "
                                                  + resolvedNode
                                                  + " is ignored, removed or forced down")));
                                  connect(nodes, newErrors, onSuccess, onFailure);
                                } else {
                                  controlNodeState = new ControlNodeState(resolvedNode, null);
                                  context
                                      .getEventBus()
                                      .fire(ChannelEvent.channelOpened(resolvedNode));
                                  channel
                                      .closeFuture()
                                      .addListener(
                                          f ->
                                              adminExecutor
                                                  .submit(
                                                      () -> onChannelClosed(channel, resolvedNode))
                                                  .addListener(UncaughtExceptions::log));
                                  onSuccess.run();
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
     * Whether {@code node} is a contact point given as a hostname, which {@link
     * #expandContactPoint} expands: not identified yet (no host id: an identified node is one
     * server at one address, and a translator's hostname on one is re-resolved by Netty on every
     * connect on purpose), an ordinary {@link DefaultEndPoint} (a custom or SNI endpoint keeps its
     * own semantics), still unresolved, and not an IP literal (under {@code resolve-contact-points
     * = false} even those are kept unresolved, and there is nothing to expand in one).
     */
    private boolean isExpandableContactPoint(Node node) {
      if (node.getHostId() != null || !(node.getEndPoint() instanceof DefaultEndPoint)) {
        return false;
      }
      SocketAddress address = node.getEndPoint().resolve();
      if (!(address instanceof InetSocketAddress)) {
        return false;
      }
      InetSocketAddress inetAddress = (InetSocketAddress) address;
      return inetAddress.isUnresolved()
          && !InetAddresses.isInetAddress(inetAddress.getHostString());
    }

    /**
     * Resolves a contact point given as a hostname to every address the name currently maps to, and
     * tries those addresses, each as its own temporary node, before the rest of the plan. So a dead
     * record no longer costs the whole contact point, and every attempt is reported under the
     * address it was made at. The resolution goes through the resolver Netty would use for the
     * connect (see {@link
     * com.datastax.oss.driver.internal.core.channel.ChannelFactory#resolveAll}), which runs it on
     * an I/O event loop: this executor stays free while it does, and picks the answer back up here.
     *
     * <p>When there is nothing to expand into (the resolver failed, or answered with anything other
     * than resolved IP addresses), the contact point is tried as it is, exactly as before: Netty
     * resolves it once more inside the connect, and a failure there is recorded against the contact
     * point as it always was.
     */
    private void expandContactPoint(
        Node contactPoint,
        Queue<Node> nodes,
        List<Entry<Node, Throwable>> errors,
        Runnable onSuccess,
        Consumer<Throwable> onFailure) {
      InetSocketAddress name = (InetSocketAddress) contactPoint.getEndPoint().resolve();
      LOG.debug("[{}] Resolving contact point {}", logPrefix, contactPoint);
      context
          .getChannelFactory()
          .resolveAll(name)
          .whenCompleteAsync(
              (addresses, error) -> {
                try {
                  if (closeWasCalled || initFuture.isCancelled()) {
                    // Abort the way the connect callback does: the round has to complete, or a
                    // Reconnection would wait on it forever.
                    onSuccess.run();
                    return;
                  }
                  List<Node> candidates =
                      (error == null)
                          ? candidatesFor(contactPoint, name, addresses)
                          : ImmutableList.<Node>of();
                  if (candidates.isEmpty()) {
                    LOG.debug(
                        "[{}] Could not expand {}, trying it as is",
                        logPrefix,
                        contactPoint,
                        error);
                    connect(
                        new CompositeQueryPlan(new SimpleQueryPlan(contactPoint), nodes),
                        errors,
                        onSuccess,
                        onFailure,
                        false);
                  } else {
                    LOG.debug(
                        "[{}] {} resolves to {} address(es), trying {}",
                        logPrefix,
                        contactPoint,
                        candidates.size(),
                        candidates);
                    connect(
                        new CompositeQueryPlan(new SimpleQueryPlan(candidates.toArray()), nodes),
                        errors,
                        onSuccess,
                        onFailure);
                  }
                } catch (Throwable t) {
                  Loggers.warnWithException(
                      LOG, "[{}] Unexpected error while expanding {}", logPrefix, contactPoint, t);
                  connect(
                      new CompositeQueryPlan(new SimpleQueryPlan(contactPoint), nodes),
                      errors,
                      onSuccess,
                      onFailure,
                      false);
                }
              },
              adminExecutor);
    }

    /**
     * One temporary node per distinct address {@code name} resolved to, each labelled with the name
     * ({@code cluster.example.com/10.0.0.1:9042}): TLS and authentication keep seeing the name the
     * user configured, and a failure names the address it happened at. Shuffled, so that a dead
     * first record is not dead for every session, then capped to {@code
     * advanced.connection.max-candidate-addresses}. Empty when any answer is not a resolved IP
     * address, in which case the caller tries the contact point as it is.
     */
    private List<Node> candidatesFor(
        Node contactPoint, InetSocketAddress name, List<SocketAddress> addresses) {
      // Resolved InetSocketAddresses compare by address bytes and port, and every candidate here
      // carries the same host string: exact duplicates collapse, nothing else does.
      Set<InetSocketAddress> distinct = new LinkedHashSet<>();
      for (SocketAddress address : addresses) {
        if (!(address instanceof InetSocketAddress)
            || ((InetSocketAddress) address).isUnresolved()) {
          return ImmutableList.of();
        }
        InetSocketAddress resolved = (InetSocketAddress) address;
        try {
          distinct.add(
              new InetSocketAddress(
                  labelled(name.getHostString(), resolved.getAddress()), resolved.getPort()));
        } catch (UnknownHostException e) {
          // Only for an address of illegal length, which no resolver produces.
          return ImmutableList.of();
        }
      }
      if (distinct.isEmpty()) {
        return ImmutableList.of();
      }
      List<InetSocketAddress> shuffled = new ArrayList<>(distinct);
      Collections.shuffle(shuffled, random);
      int cap =
          Math.max(
              1,
              config
                  .getDefaultProfile()
                  .getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES));
      if (shuffled.size() > cap) {
        LOG.debug(
            "[{}] {} resolves to {} addresses, trying at most {} "
                + "(advanced.connection.max-candidate-addresses)",
            logPrefix,
            contactPoint,
            shuffled.size(),
            cap);
        shuffled = shuffled.subList(0, cap);
      }
      List<Node> candidates = new ArrayList<>(shuffled.size());
      for (InetSocketAddress address : shuffled) {
        candidates.add(DefaultNode.newContactPoint(new DefaultEndPoint(address), context));
      }
      return ImmutableList.copyOf(candidates);
    }

    /** {@code address} relabelled with {@code hostName}, keeping an IPv6 scope id if it has one. */
    private InetAddress labelled(String hostName, InetAddress address) throws UnknownHostException {
      if (address instanceof Inet6Address && ((Inet6Address) address).getScopeId() != 0) {
        return Inet6Address.getByAddress(
            hostName, address.getAddress(), ((Inet6Address) address).getScopeId());
      }
      return InetAddress.getByAddress(hostName, address.getAddress());
    }

    /**
     * Resolves the identity of the node at the other end of the channel. For contact point nodes
     * (no hostId), queries system.local and registers a new metadata node. For nodes that already
     * have a hostId, returns the node as-is.
     */
    private CompletionStage<Node> resolveChannelNodeIfNeeded(
        DriverChannel channel, DefaultNode node) {
      if (node.getHostId() != null) {
        return CompletableFuture.completedFuture(node);
      }
      return context
          .getTopologyMonitor()
          .getChannelNodeInfo(channel)
          .thenComposeAsync(
              nodeInfo -> {
                EndPoint resolvedEp = nodeInfo.getEndPoint();
                if (resolvedEp != null && !resolvedEp.equals(channel.getEndPoint())) {
                  channel.setEndPoint(resolvedEp);
                  LOG.debug("[{}] Control channel endpoint upgraded to {}", logPrefix, resolvedEp);
                }
                return context.getMetadataManager().registerNode(nodeInfo);
              },
              adminExecutor);
    }

    private void onSuccessfulReconnect() {
      assert adminExecutor.inEventLoop();
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
      // Reference identity, not endpoint equality: with unresolved contact points in the plan,
      // DefaultEndPoint.equals resolves the unresolved side of a mixed pair -- a blocking lookup on
      // this admin executor, during the DNS outage the fallback exists for. An event naming the
      // metadata node for a host still being identified cannot match here at all; connect()
      // re-checks that node with isUnusableForControl() once the resolve completes.
      return state.current == null && state.pending != null && eventNode == state.pending;
    }

    /**
     * Whether an event has already marked this node unusable for the control connection: ignored by
     * the load balancing policy, or removed or forced down.
     *
     * <p>{@link #connect} runs the same checks on the node it dialled, before adopting the channel;
     * this one runs a turn later, on the node that channel was identified as.
     *
     * <p>Keyed by node instance, so it cannot see a node {@code MetadataManager#registerNode}
     * minted fresh for an unknown host id: that host is resurrected, not rejected. Detecting it
     * would need a removal record keyed by host id -- rejecting host ids absent from the metadata
     * would also reject the new nodes of a cluster that moved, which is what the fallback exists to
     * recover.
     */
    private boolean isUnusableForControl(Node node) {
      NodeState state = lastNodeState.get(node);
      return lastNodeDistance.get(node) == NodeDistance.IGNORED
          || (lastNodeState.containsKey(node)
              && (state == null /*(removed)*/ || state == NodeState.FORCED_DOWN));
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

  private boolean isAuthFailure(Throwable error) {
    if (error instanceof AllNodesFailedException) {
      Collection<List<Throwable>> errors =
          ((AllNodesFailedException) error).getAllErrors().values();
      if (errors.isEmpty()) {
        return false;
      }
      for (List<Throwable> nodeErrors : errors) {
        for (Throwable nodeError : nodeErrors) {
          if (!(nodeError instanceof AuthenticationException)) {
            return false;
          }
        }
      }
    }
    return true;
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
