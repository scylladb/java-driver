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

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.KeyspaceFilter;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaRows;
import com.datastax.oss.driver.internal.core.metadata.schema.refresh.SchemaRefresh;
import com.datastax.oss.driver.internal.core.protocol.TabletInfo;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Debouncer;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the immutable instance of the {@link Metadata}, and handles requests to update it. */
@ThreadSafe
public class MetadataManager implements AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

  static final EndPoint DEFAULT_CONTACT_POINT =
      new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));

  // Bounds how long a single contact-point hostname resolution may block the caller (typically the
  // admin event loop, see getResolvedContactPoints()). Not configurable on purpose: this is an
  // interim mitigation, see the note on getResolvedContactPoints() below.
  private static final Duration CONTACT_POINT_RESOLUTION_TIMEOUT = Duration.ofSeconds(3);

  private final InternalDriverContext context;
  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final DriverExecutionProfile config;
  private final SingleThreaded singleThreaded;
  private final ControlConnection controlConnection;

  // Contact-point hostname resolution (getResolvedContactPoints()) runs on the admin event loop,
  // where nothing should block. InetAddress.getAllByName() blocks, so each resolution is offloaded
  // here and bounded by CONTACT_POINT_RESOLUTION_TIMEOUT. A cached pool (rather than a single
  // shared thread) resolves each hostname on its own thread: a slow or blackholed lookup that
  // ignores its interrupt only ties up its own daemon thread until the OS resolver finally
  // returns, instead of starving the sibling contact points -- or the next reconnect that would
  // otherwise queue behind it. This is an interim mitigation, superseded by #890's non-blocking
  // EndPoint.resolveAll().
  private final ExecutorService contactPointResolverExecutor;

  private volatile DefaultMetadata metadata; // only updated from adminExecutor
  private volatile boolean schemaEnabledInConfig;
  private volatile List<String> refreshedKeyspaces;
  private volatile KeyspaceFilter keyspaceFilter;
  private volatile Boolean schemaEnabledProgrammatically;
  private volatile boolean tokenMapEnabled;
  private volatile Set<DefaultNode> contactPoints;
  private volatile boolean wasImplicitContactPoint;

  private volatile TypeCodec<TupleValue> tabletPayloadCodec = null;

  public MetadataManager(InternalDriverContext context) {
    this(context, DefaultMetadata.EMPTY);
  }

  protected MetadataManager(InternalDriverContext context, DefaultMetadata initialMetadata) {
    this.context = context;
    this.metadata = initialMetadata;
    this.logPrefix = context.getSessionName();
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.config = context.getConfig().getDefaultProfile();
    this.singleThreaded = new SingleThreaded(context, config);
    this.controlConnection = context.getControlConnection();
    this.schemaEnabledInConfig = config.getBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED);
    this.refreshedKeyspaces =
        config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList());
    this.keyspaceFilter = KeyspaceFilter.newInstance(logPrefix, refreshedKeyspaces);
    this.tokenMapEnabled = config.getBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED);
    AtomicInteger resolverThreadCount = new AtomicInteger();
    this.contactPointResolverExecutor =
        Executors.newCachedThreadPool(
            runnable -> {
              Thread thread =
                  new Thread(
                      runnable,
                      logPrefix
                          + "-contact-point-resolver-"
                          + resolverThreadCount.incrementAndGet());
              thread.setDaemon(true);
              return thread;
            });

    context.getEventBus().register(ConfigChangeEvent.class, this::onConfigChanged);
  }

  private void onConfigChanged(@SuppressWarnings("unused") ConfigChangeEvent event) {
    boolean schemaEnabledBefore = isSchemaEnabled();
    boolean tokenMapEnabledBefore = tokenMapEnabled;
    List<String> keyspacesBefore = this.refreshedKeyspaces;

    this.schemaEnabledInConfig = config.getBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED);
    this.refreshedKeyspaces =
        config.getStringList(
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES, Collections.emptyList());
    this.keyspaceFilter = KeyspaceFilter.newInstance(logPrefix, refreshedKeyspaces);
    this.tokenMapEnabled = config.getBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED);

    if ((!schemaEnabledBefore
            || !keyspacesBefore.equals(refreshedKeyspaces)
            || (!tokenMapEnabledBefore && tokenMapEnabled))
        && isSchemaEnabled()) {
      refreshSchema(null, false, true)
          .whenComplete(
              (metadata, error) -> {
                if (error != null) {
                  Loggers.warnWithException(
                      LOG,
                      "[{}] Unexpected error while refreshing schema after it was re-enabled "
                          + "in the configuration, keeping previous version",
                      logPrefix,
                      error);
                }
              });
    }
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public void addContactPoints(Set<EndPoint> providedContactPoints) {
    // Convert the EndPoints to Nodes, but we can't put them into the Metadata yet, because we
    // don't know their host_id. So store them in a volatile field instead, they will get copied
    // during the first node refresh.
    ImmutableSet.Builder<DefaultNode> contactPointsBuilder = ImmutableSet.builder();
    if (providedContactPoints == null || providedContactPoints.isEmpty()) {
      LOG.info(
          "[{}] No contact points provided, defaulting to {}", logPrefix, DEFAULT_CONTACT_POINT);
      this.wasImplicitContactPoint = true;
      contactPointsBuilder.add(DefaultNode.newContactPoint(DEFAULT_CONTACT_POINT, context));
    } else {
      for (EndPoint endPoint : providedContactPoints) {
        contactPointsBuilder.add(DefaultNode.newContactPoint(endPoint, context));
      }
    }
    this.contactPoints = contactPointsBuilder.build();
    LOG.debug("[{}] Adding initial contact points {}", logPrefix, contactPoints);
  }

  /**
   * The contact points that were used by the driver to initialize. If none were provided
   * explicitly, this will be the default (127.0.0.1:9042).
   */
  public Set<DefaultNode> getContactPoints() {
    return contactPoints;
  }

  /**
   * Returns the contact points expanded to all their DNS-resolved IPs.
   *
   * <p>Contact points are stored with unresolved hostnames (the driver always uses deferred DNS
   * resolution). For each contact point whose underlying address is an unresolved {@link
   * InetSocketAddress}, this method calls {@link InetAddress#getAllByName(String)} to obtain every
   * IP the hostname maps to and creates a synthetic contact-point {@link DefaultNode} for each IP.
   * This lets the load balancing policy iterate over all candidate IPs rather than only the first
   * one, so that a non-responsive IP does not block initial connection or control-connection
   * reconnection.
   *
   * <p>The returned synthetic nodes are IP-backed <em>connection candidates</em>, not mere hostname
   * wrappers: their endpoint resolves to a concrete IP. If such a node becomes the control node,
   * its resolved endpoint is stored as-is in metadata (see {@link #registerNode(NodeInfo)}), so
   * later reconnects keep using that IP unless they fall back to the original (unresolved) contact
   * points, which re-enters this method and re-resolves DNS.
   *
   * <p><b>TLS note:</b> each synthetic endpoint is built from the {@link InetAddress} returned by
   * resolving the original hostname, which retains that hostname. This means the TCP connection
   * uses the selected IP while the SSL engine still receives the original contact-point hostname
   * for peer host / SNI / hostname verification. This must be preserved: constructing the endpoint
   * from a raw IP string instead would break hostname verification and implicit SNI.
   *
   * <p>Already-resolved addresses, and endpoints that are not a {@link DefaultEndPoint} (e.g. an
   * SNI or client-routes endpoint), are returned as-is: those specialized endpoint types carry
   * identity beyond a plain socket address (SNI server name, host_id) and must not be naively
   * reconstructed from a resolved IP.
   *
   * <p>Resolution is best-effort: if a hostname cannot be resolved, or resolution exceeds {@link
   * #getContactPointResolutionTimeout()}, the original <em>unresolved</em> contact-point node is
   * kept as-is instead of being dropped. The query plan is therefore never emptier than the
   * configured contact points, and the hostname can still be resolved later at connection time (as
   * it was before DNS expansion existed).
   */
  public List<Node> getResolvedContactPoints() {
    Set<DefaultNode> nodes = contactPoints;
    if (nodes == null) {
      return new ArrayList<>();
    }
    List<Node> result = new ArrayList<>();
    // NOTE (interim mitigation, superseded in #890): this method is called from the
    // control-connection query-plan path, i.e. the admin event loop (ControlConnection asserts
    // inEventLoop()), where nothing should ever block. InetAddress.getAllByName() is a blocking
    // call, so each unresolved hostname is submitted to contactPointResolverExecutor (pass 1) and
    // then collected against a single shared deadline (pass 2). Resolving concurrently rather than
    // one-at-a-time keeps the total wait bounded by roughly one CONTACT_POINT_RESOLUTION_TIMEOUT
    // regardless of the number of contact points, and keeps one slow/blackholed hostname from
    // starving the others. The calling thread still waits for the result, so this is a bound, not a
    // true fix -- #890 moves multi-address resolution into the EndPoint API / ChannelFactory
    // (EndPoint.resolveAll()) for proper non-blocking resolution; revisit when that lands.
    List<PendingResolution> pending = new ArrayList<>();
    for (DefaultNode node : nodes) {
      EndPoint endPoint = node.getEndPoint();
      if (endPoint instanceof DefaultEndPoint) {
        InetSocketAddress address = ((DefaultEndPoint) endPoint).resolve();
        if (address.isUnresolved()) {
          // Expand hostname to all IPs so callers can try each one in turn.
          try {
            Future<InetAddress[]> future =
                contactPointResolverExecutor.submit(
                    () -> resolveContactPointHostname(address.getHostString()));
            pending.add(new PendingResolution(node, address, future));
          } catch (RejectedExecutionException e) {
            // Executor already shut down (session closing): keep the unresolved node as a
            // best-effort fallback rather than dropping it.
            result.add(node);
          }
          continue;
        }
      }
      // Already resolved or non-DefaultEndPoint endpoint — use as-is.
      result.add(node);
    }

    // Collect the concurrent resolutions against one shared deadline so the total wait is bounded
    // by roughly a single timeout, no matter how many hostnames are pending.
    long deadlineNanos = System.nanoTime() + getContactPointResolutionTimeout().toNanos();
    for (int i = 0; i < pending.size(); i++) {
      PendingResolution p = pending.get(i);
      InetAddress[] all;
      try {
        long remainingNanos = deadlineNanos - System.nanoTime();
        all = p.future.get(Math.max(0, remainingNanos), TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        p.future.cancel(true);
        LOG.warn(
            "[{}] Timed out resolving contact point hostname {} after {}, keeping it unresolved",
            logPrefix,
            p.address.getHostString(),
            getContactPointResolutionTimeout());
        result.add(p.node);
        continue;
      } catch (ExecutionException e) {
        LOG.warn(
            "[{}] Could not resolve contact point hostname {}, keeping it unresolved",
            logPrefix,
            p.address.getHostString(),
            e.getCause());
        result.add(p.node);
        continue;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        // Keep this and every remaining contact point unresolved (best-effort) and stop waiting.
        for (int j = i; j < pending.size(); j++) {
          pending.get(j).future.cancel(true);
          result.add(pending.get(j).node);
        }
        break;
      }
      if (all.length > 1) {
        LOG.debug(
            "[{}] Contact point {} expands to {} addresses",
            logPrefix,
            p.address.getHostString(),
            all.length);
      }
      for (InetAddress ip : all) {
        // Build the endpoint from the resolved InetAddress (not a raw IP string) on
        // purpose: the InetAddress keeps the original hostname, so the TCP connection
        // uses this IP while the SSL engine still receives the hostname for peer host,
        // SNI and hostname verification. Do not simplify this to a numeric IP address.
        InetSocketAddress resolved = new InetSocketAddress(ip, p.address.getPort());
        result.add(DefaultNode.newContactPoint(new DefaultEndPoint(resolved), context));
      }
    }
    return result;
  }

  /** A contact-point hostname whose concurrent DNS resolution is in flight. */
  private static final class PendingResolution {
    final DefaultNode node;
    final InetSocketAddress address;
    final Future<InetAddress[]> future;

    PendingResolution(DefaultNode node, InetSocketAddress address, Future<InetAddress[]> future) {
      this.node = node;
      this.address = address;
      this.future = future;
    }
  }

  /**
   * The timeout applied to each contact-point hostname resolution triggered by {@link
   * #getResolvedContactPoints()}. Extracted as a method (rather than referencing the constant
   * directly) so tests can substitute a short timeout instead of waiting out the real one.
   */
  @VisibleForTesting
  Duration getContactPointResolutionTimeout() {
    return CONTACT_POINT_RESOLUTION_TIMEOUT;
  }

  /**
   * Resolves a contact-point hostname to all its IPs. Extracted as a method (rather than calling
   * {@link InetAddress#getAllByName(String)} directly) so tests can substitute a slow or failing
   * resolution to exercise the timeout path in {@link #getResolvedContactPoints()}.
   */
  @VisibleForTesting
  InetAddress[] resolveContactPointHostname(String host) throws UnknownHostException {
    return InetAddress.getAllByName(host);
  }

  /** Whether the default contact point was used (because none were provided explicitly). */
  public boolean wasImplicitContactPoint() {
    return wasImplicitContactPoint;
  }

  /**
   * Creates a new metadata node from the given {@link NodeInfo} and registers it into metadata so
   * that subsequent refreshes can find and reuse it by hostId. If a node with the same hostId
   * already exists, returns the existing node.
   *
   * <p>Note: this always creates a new {@link DefaultNode} rather than reusing the caller's contact
   * point node. Contact point nodes are ephemeral objects used only for the connection query plan;
   * they are never added to metadata and never exposed to user-facing APIs (events, {@link
   * com.datastax.oss.driver.api.core.metadata.Metadata#getNodes()}, or {@link
   * com.datastax.oss.driver.api.core.metadata.NodeStateListener} callbacks).
   *
   * <p>The metadata node stores {@code nodeInfo.getEndPoint()} as-is. When the control node was
   * reached through a DNS-expanded synthetic contact point (see {@link
   * #getResolvedContactPoints()}), that resolved IP endpoint is therefore persisted in metadata and
   * is never re-resolved afterwards. Re-resolving the original hostname only happens through the
   * original-contact-point reconnection fallback (see {@code
   * advanced.control-connection.reconnection.fallback-to-original-contact-points}).
   */
  public CompletionStage<Node> registerNode(NodeInfo nodeInfo) {
    Preconditions.checkNotNull(nodeInfo.getHostId(), "Cannot register node without hostId");
    CompletableFuture<Node> result = new CompletableFuture<>();
    RunOrSchedule.on(
        adminExecutor,
        () -> {
          try {
            assert adminExecutor.inEventLoop();
            Node existing = metadata.getNodes().get(nodeInfo.getHostId());
            if (existing != null) {
              LOG.debug(
                  "[{}] Node with hostId {} already in metadata, returning existing node",
                  logPrefix,
                  nodeInfo.getHostId());
              result.complete(existing);
              return;
            }
            DefaultNode newNode = new DefaultNode(nodeInfo.getEndPoint(), context);
            NodesRefresh.copyInfos(nodeInfo, newNode, context);
            Map<UUID, Node> newNodes = new HashMap<>(metadata.getNodes());
            newNodes.put(newNode.getHostId(), newNode);
            this.metadata =
                new DefaultMetadata(
                    ImmutableMap.copyOf(newNodes),
                    metadata.getKeyspaces(),
                    metadata.getTokenMap().orElse(null),
                    metadata.getClusterName().orElse(null),
                    metadata.getTabletMap().orElse(DefaultTabletMap.emptyMap()));
            if (singleThreaded.didFirstNodeListRefresh) {
              LOG.debug(
                  "[{}] registerNode inserting new node {} after initial refresh, "
                      + "firing added event",
                  logPrefix,
                  nodeInfo.getHostId());
              context.getEventBus().fire(NodeStateEvent.added(newNode));
            }
            result.complete(newNode);
          } catch (Exception e) {
            result.completeExceptionally(e);
          }
        });
    return result;
  }

  public CompletionStage<Void> refreshNodes() {
    return context
        .getTopologyMonitor()
        .refreshNodeList()
        .thenApplyAsync(singleThreaded::refreshNodes, adminExecutor);
  }

  public CompletionStage<Void> refreshNode(Node node) {
    return context
        .getTopologyMonitor()
        .refreshNode(node)
        .thenApplyAsync(
            maybeInfo -> {
              if (maybeInfo.isPresent()) {
                boolean tokensChanged =
                    NodesRefresh.copyInfos(maybeInfo.get(), (DefaultNode) node, context);
                if (tokensChanged) {
                  apply(new TokensChangedRefresh());
                }
              } else {
                LOG.debug(
                    "[{}] Topology monitor did not return any info for the refresh of {}, skipping",
                    logPrefix,
                    node);
              }
              return null;
            },
            adminExecutor);
  }

  public void addNode(InetSocketAddress broadcastRpcAddress) {
    context
        .getTopologyMonitor()
        .getNewNodeInfo(broadcastRpcAddress)
        .whenCompleteAsync(
            (info, error) -> {
              if (error != null) {
                LOG.debug(
                    "[{}] Error refreshing node info for {}, "
                        + "this will be retried on the next full refresh",
                    logPrefix,
                    broadcastRpcAddress,
                    error);
              } else {
                singleThreaded.addNode(broadcastRpcAddress, info.orElse(null));
              }
            },
            adminExecutor);
  }

  public void removeNode(InetSocketAddress broadcastRpcAddress) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.removeNode(broadcastRpcAddress));
  }

  /**
   * @param keyspace if this refresh was triggered by an event, that event's keyspace, otherwise
   *     null (this is only used to discard the event if it targets a keyspace that we're ignoring)
   * @param evenIfDisabled force the refresh even if schema is currently disabled (used for user
   *     request)
   * @param flushNow bypass the debouncer and force an immediate refresh (used to avoid a delay at
   *     startup)
   */
  public CompletionStage<RefreshSchemaResult> refreshSchema(
      String keyspace, boolean evenIfDisabled, boolean flushNow) {
    CompletableFuture<RefreshSchemaResult> future = new CompletableFuture<>();
    RunOrSchedule.on(
        adminExecutor,
        () -> singleThreaded.refreshSchema(keyspace, evenIfDisabled, flushNow, future));
    return future;
  }

  public static class RefreshSchemaResult {
    private final Metadata metadata;
    private final boolean isSchemaInAgreement;

    public RefreshSchemaResult(Metadata metadata, boolean isSchemaInAgreement) {
      this.metadata = metadata;
      this.isSchemaInAgreement = isSchemaInAgreement;
    }

    public RefreshSchemaResult(Metadata metadata) {
      this(
          metadata,
          // This constructor is used in corner cases where agreement doesn't matter
          true);
    }

    public Metadata getMetadata() {
      return metadata;
    }

    public boolean isSchemaInAgreement() {
      return isSchemaInAgreement;
    }
  }

  public boolean isSchemaEnabled() {
    return (schemaEnabledProgrammatically != null)
        ? schemaEnabledProgrammatically
        : schemaEnabledInConfig;
  }

  public CompletionStage<Metadata> setSchemaEnabled(Boolean newValue) {
    boolean wasEnabledBefore = isSchemaEnabled();
    schemaEnabledProgrammatically = newValue;
    if (!wasEnabledBefore && isSchemaEnabled()) {
      return refreshSchema(null, false, true).thenApply(RefreshSchemaResult::getMetadata);
    } else {
      return CompletableFuture.completedFuture(metadata);
    }
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return this.closeAsync();
  }

  private class SingleThreaded {
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final CompletableFuture<Void> firstSchemaRefreshFuture = new CompletableFuture<>();
    private final Debouncer<
            CompletableFuture<RefreshSchemaResult>, CompletableFuture<RefreshSchemaResult>>
        schemaRefreshDebouncer;
    private final SchemaQueriesFactory schemaQueriesFactory;
    private final SchemaParserFactory schemaParserFactory;

    // We don't allow concurrent schema refreshes. If one is already running, the next one is queued
    // (and the ones after that are merged with the queued one).
    private CompletableFuture<RefreshSchemaResult> currentSchemaRefresh;
    private CompletableFuture<RefreshSchemaResult> queuedSchemaRefresh;

    private boolean didFirstNodeListRefresh;

    private SingleThreaded(InternalDriverContext context, DriverExecutionProfile config) {
      this.schemaRefreshDebouncer =
          new Debouncer<>(
              logPrefix + "|metadata debouncer",
              adminExecutor,
              this::coalesceSchemaRequests,
              this::startSchemaRequest,
              config.getDuration(DefaultDriverOption.METADATA_SCHEMA_WINDOW),
              config.getInt(DefaultDriverOption.METADATA_SCHEMA_MAX_EVENTS));
      this.schemaQueriesFactory = context.getSchemaQueriesFactory();
      this.schemaParserFactory = context.getSchemaParserFactory();
    }

    private Void refreshNodes(Iterable<NodeInfo> nodeInfos) {
      MetadataRefresh refresh =
          didFirstNodeListRefresh
              ? new FullNodeListRefresh(nodeInfos)
              : new InitialNodeListRefresh(nodeInfos);
      didFirstNodeListRefresh = true;
      return apply(refresh);
    }

    private void addNode(InetSocketAddress address, NodeInfo info) {
      try {
        if (info != null) {
          if (!address.equals(info.getBroadcastRpcAddress().orElse(null))) {
            // This would be a bug in the TopologyMonitor, protect against it
            LOG.warn(
                "[{}] Received a request to add a node for broadcast RPC address {}, "
                    + "but the provided info reports {}, ignoring it",
                logPrefix,
                address,
                info.getBroadcastAddress());
          } else {
            apply(new AddNodeRefresh(info));
          }
        } else {
          LOG.debug(
              "[{}] Ignoring node addition for {} because the "
                  + "topology monitor didn't return any information",
              logPrefix,
              address);
        }
      } catch (Throwable t) {
        LOG.warn("[" + logPrefix + "] Unexpected exception while handling added node", logPrefix);
      }
    }

    private void removeNode(InetSocketAddress broadcastRpcAddress) {
      apply(new RemoveNodeRefresh(broadcastRpcAddress));
    }

    private void refreshSchema(
        String keyspace,
        boolean evenIfDisabled,
        boolean flushNow,
        CompletableFuture<RefreshSchemaResult> future) {

      if (!didFirstNodeListRefresh) {
        // This happen if the control connection receives a schema event during init. We can't
        // refresh yet because we don't know the nodes' versions, simply ignore.
        future.complete(new RefreshSchemaResult(metadata));
        return;
      }

      // If this is an event, make sure it's not targeting a keyspace that we're ignoring.
      boolean isRefreshedKeyspace = keyspace == null || keyspaceFilter.includes(keyspace);

      if (isRefreshedKeyspace && (evenIfDisabled || isSchemaEnabled())) {
        acceptSchemaRequest(future, flushNow);
      } else {
        future.complete(new RefreshSchemaResult(metadata));
        singleThreaded.firstSchemaRefreshFuture.complete(null);
      }
    }

    // An external component has requested a schema refresh, feed it to the debouncer.
    private void acceptSchemaRequest(
        CompletableFuture<RefreshSchemaResult> future, boolean flushNow) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        future.complete(new RefreshSchemaResult(metadata));
      } else {
        schemaRefreshDebouncer.receive(future);
        if (flushNow) {
          schemaRefreshDebouncer.flushNow();
        }
      }
    }

    // Multiple requests have arrived within the debouncer window, coalesce them.
    private CompletableFuture<RefreshSchemaResult> coalesceSchemaRequests(
        List<CompletableFuture<RefreshSchemaResult>> futures) {
      assert adminExecutor.inEventLoop();
      assert !futures.isEmpty();
      // Keep only one, but ensure that the discarded ones will still be completed when we're done
      CompletableFuture<RefreshSchemaResult> result = null;
      for (CompletableFuture<RefreshSchemaResult> future : futures) {
        if (result == null) {
          result = future;
        } else {
          CompletableFutures.completeFrom(result, future);
        }
      }
      return result;
    }

    // The debouncer has flushed, start the actual work.
    private void startSchemaRequest(CompletableFuture<RefreshSchemaResult> refreshFuture) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        refreshFuture.complete(new RefreshSchemaResult(metadata));
        return;
      }
      if (currentSchemaRefresh == null) {
        currentSchemaRefresh = refreshFuture;
        LOG.debug("[{}] Starting schema refresh", logPrefix);
        initControlConnectionForSchema()
            .thenCompose(v -> context.getTopologyMonitor().checkSchemaAgreement())
            .whenComplete(
                (schemaInAgreement, agreementError) -> {
                  if (agreementError != null) {
                    refreshFuture.completeExceptionally(agreementError);
                    onSchemaRefreshComplete();
                  } else {
                    try {
                      schemaQueriesFactory
                          .newInstance()
                          .execute()
                          .thenApplyAsync(this::parseAndApplySchemaRows, adminExecutor)
                          .whenComplete(
                              (newMetadata, metadataError) -> {
                                if (metadataError != null) {
                                  refreshFuture.completeExceptionally(metadataError);
                                } else {
                                  refreshFuture.complete(
                                      new RefreshSchemaResult(newMetadata, schemaInAgreement));
                                }
                                onSchemaRefreshComplete();
                              });
                    } catch (Throwable t) {
                      LOG.debug("[{}] Exception getting new metadata", logPrefix, t);
                      refreshFuture.completeExceptionally(t);
                      onSchemaRefreshComplete();
                    }
                  }
                });
      } else if (queuedSchemaRefresh == null) {
        queuedSchemaRefresh = refreshFuture; // wait for our turn
      } else {
        CompletableFutures.completeFrom(
            queuedSchemaRefresh, refreshFuture); // join the queued request
      }
    }

    private void onSchemaRefreshComplete() {
      assert adminExecutor.inEventLoop();
      firstSchemaRefreshFuture.complete(null);
      currentSchemaRefresh = null;
      // If another refresh was enqueued during this one, run it now
      if (queuedSchemaRefresh != null) {
        CompletableFuture<RefreshSchemaResult> tmp = this.queuedSchemaRefresh;
        this.queuedSchemaRefresh = null;
        startSchemaRequest(tmp);
      }
    }

    // To query schema tables, we need the control connection.
    // Normally that the topology monitor has already initialized it to query node tables. But if a
    // custom topology monitor is in place, it might not use the control connection at all.
    private CompletionStage<Void> initControlConnectionForSchema() {
      if (firstSchemaRefreshFuture.isDone()) {
        // We tried to refresh the schema before, so we know we called init already. Don't call it
        // again since that is cheaper.
        return firstSchemaRefreshFuture;
      } else {
        // Trigger init (a no-op if the topology monitor already done so)
        return controlConnection.init(false, true, false);
      }
    }

    private Metadata parseAndApplySchemaRows(SchemaRows schemaRows) {
      assert adminExecutor.inEventLoop();
      SchemaRefresh schemaRefresh = schemaParserFactory.newInstance(schemaRows).parse();
      long start = System.nanoTime();
      apply(schemaRefresh);
      LOG.debug("[{}] Applying schema refresh took {}", logPrefix, NanoTime.formatTimeSince(start));
      return metadata;
    }

    private void addTablet(CqlIdentifier keyspace, CqlIdentifier table, Tablet tablet) {
      apply(new AddTabletRefresh(keyspace, table, tablet));
    }

    private void close() {
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Closing", logPrefix);
      // The current schema refresh should fail when its channel gets closed.
      if (queuedSchemaRefresh != null) {
        queuedSchemaRefresh.completeExceptionally(new IllegalStateException("Cluster is closed"));
      }
      contactPointResolverExecutor.shutdownNow();
      closeFuture.complete(null);
    }
  }

  @VisibleForTesting
  Void apply(MetadataRefresh refresh) {
    assert adminExecutor.inEventLoop();
    MetadataRefresh.Result result = refresh.compute(metadata, tokenMapEnabled, context);
    metadata = result.newMetadata;
    boolean isFirstSchemaRefresh =
        refresh instanceof SchemaRefresh && !singleThreaded.firstSchemaRefreshFuture.isDone();
    if (!singleThreaded.closeWasCalled && !isFirstSchemaRefresh) {
      for (Object event : result.events) {
        context.getEventBus().fire(event);
      }
    }
    return null;
  }

  private TypeCodec<TupleValue> getTabletPayloadCodec() {
    if (tabletPayloadCodec == null) {
      TupleType payloadOuterTuple =
          DataTypes.tupleOf(
              DataTypes.BIGINT,
              DataTypes.BIGINT,
              DataTypes.listOf(DataTypes.tupleOf(DataTypes.UUID, DataTypes.INT)));
      tabletPayloadCodec = context.getCodecRegistry().codecFor(payloadOuterTuple);
    }
    return tabletPayloadCodec;
  }

  public void addTabletFromPayload(
      CqlIdentifier keyspace,
      CqlIdentifier table,
      @NonNull Map<String, ByteBuffer> incomingPayload) {
    // Assumes payload is present
    TupleValue tabletTuple =
        getTabletPayloadCodec()
            .decode(
                incomingPayload.get(TabletInfo.TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY),
                context.getProtocolVersion());
    if (tabletTuple == null) {
      LOG.warn(
          "Custom payload containing tablet information for table {}.{} decoded to null. This should not ever "
              + "happen.",
          keyspace,
          table);
      return;
    }
    DefaultTabletMap.DefaultTablet tabletToAdd =
        DefaultTabletMap.DefaultTablet.parseTabletPayloadV1(tabletTuple, getMetadata().getNodes());
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.addTablet(keyspace, table, tabletToAdd));
  }
}
