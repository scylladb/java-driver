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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeShardingInfo;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.PinnableEndPoint;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import com.datastax.oss.driver.internal.core.util.ProtocolUtils;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolFeatures;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.Timeout;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds {@link DriverChannel} objects for an instance of the driver. */
@ThreadSafe
public class ChannelFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelFactory.class);

  /**
   * A value for {@link #productType} that indicates that we are connected to DataStax Cloud. This
   * value matches the one defined at DSE DB server side at {@code ProductType.java}.
   */
  private static final String DATASTAX_CLOUD_PRODUCT_TYPE = "DATASTAX_APOLLO";

  private static final AtomicBoolean LOGGED_ORPHAN_WARNING = new AtomicBoolean();

  /**
   * A value for {@link #productType} that indicates that the server does not report any product
   * type.
   */
  private static final String UNKNOWN_PRODUCT_TYPE = "UNKNOWN";

  // The names of the handlers on the pipeline:
  public static final String SSL_HANDLER_NAME = "ssl";
  public static final String INBOUND_TRAFFIC_METER_NAME = "inboundTrafficMeter";
  public static final String OUTBOUND_TRAFFIC_METER_NAME = "outboundTrafficMeter";
  public static final String FRAME_TO_BYTES_ENCODER_NAME = "frameToBytesEncoder";
  public static final String FRAME_TO_SEGMENT_ENCODER_NAME = "frameToSegmentEncoder";
  public static final String SEGMENT_TO_BYTES_ENCODER_NAME = "segmentToBytesEncoder";
  public static final String BYTES_TO_FRAME_DECODER_NAME = "bytesToFrameDecoder";
  public static final String BYTES_TO_SEGMENT_DECODER_NAME = "bytesToSegmentDecoder";
  public static final String SEGMENT_TO_FRAME_DECODER_NAME = "segmentToFrameDecoder";
  public static final String HEARTBEAT_HANDLER_NAME = "heartbeat";
  public static final String INFLIGHT_HANDLER_NAME = "inflight";
  public static final String INIT_HANDLER_NAME = "init";

  /**
   * The number of orphaned requests a connection is actually built with, which is not always the
   * configured {@code advanced.connection.max-orphan-requests}: that option has to stay below
   * {@code advanced.connection.max-requests-per-connection}, and a value that does not is silently
   * corrected to a quarter of it (the caller logs a warning when that happens).
   *
   * <p>Shared with {@code DefaultDriverConfigReporter}, which reports this number as {@code
   * connection.requests.orphaned.max}: one implementation means the report cannot claim a limit the
   * connection was not built with.
   *
   * @param maxRequestsPerConnection the configured {@code max-requests-per-connection}.
   * @param maxOrphanRequests the configured {@code max-orphan-requests}.
   */
  public static int effectiveMaxOrphanRequests(
      int maxRequestsPerConnection, int maxOrphanRequests) {
    return (maxOrphanRequests >= maxRequestsPerConnection)
        ? maxRequestsPerConnection / 4
        : maxOrphanRequests;
  }

  private final String logPrefix;
  protected final InternalDriverContext context;

  /**
   * Guards the one-time warning in {@link #newBootstrap()}. Per factory rather than per JVM: what
   * it reports is a property of this session's {@link NettyOptions}, and the message names the
   * session, so a JVM-wide latch would report the first offender and silence every one after it.
   */
  private final AtomicBoolean loggedHandlerWarning = new AtomicBoolean();

  private final AtomicBoolean loggedGroupWarning = new AtomicBoolean();

  /** Guards the one-time warning in {@link #warnAboutPassThrough}, on the same terms. */
  private final AtomicBoolean loggedPassThroughWarning = new AtomicBoolean();

  /**
   * Randomizes the order in which a name's expanded addresses are tried (see {@link
   * #shuffleAndLimit}). Injectable so tests can seed it and observe a deterministic order.
   */
  @VisibleForTesting Random random = new Random();

  /** either set from the configuration, or null and will be negotiated */
  @VisibleForTesting volatile ProtocolVersion protocolVersion;

  private volatile String clusterName;

  /**
   * The value of the {@code PRODUCT_TYPE} option reported by the first channel we opened, in
   * response to a {@code SUPPORTED} request.
   *
   * <p>If the server does not return that option, the value will be {@link #UNKNOWN_PRODUCT_TYPE}.
   */
  @VisibleForTesting volatile String productType;

  public ChannelFactory(InternalDriverContext context) {
    this.logPrefix = context.getSessionName();
    this.context = context;

    DriverExecutionProfile defaultConfig = context.getConfig().getDefaultProfile();

    if (defaultConfig.isDefined(DefaultDriverOption.PROTOCOL_VERSION)) {
      String versionName = defaultConfig.getString(DefaultDriverOption.PROTOCOL_VERSION);
      this.protocolVersion = context.getProtocolVersionRegistry().fromName(versionName);
    } // else it will be negotiated with the first opened connection
  }

  public ProtocolVersion getProtocolVersion() {
    ProtocolVersion result = this.protocolVersion;
    Preconditions.checkState(
        result != null, "Protocol version not known yet, this should only be called after init");
    return result;
  }

  /**
   * WARNING: this is only used at the very beginning of the init process (when we just refreshed
   * the list of nodes for the first time, and found out that one of them requires a lower version
   * than was negotiated with the first contact point); it's safe at this time because we are in a
   * controlled state (only the control connection is open, it's not executing queries and we're
   * going to reconnect immediately after). Calling this method at any other time will likely wreak
   * havoc.
   */
  public void setProtocolVersion(ProtocolVersion newVersion) {
    this.protocolVersion = newVersion;
  }

  public String getClusterName() {
    return clusterName;
  }

  public CompletionStage<DriverChannel> connect(Node node, DriverChannelOptions options) {
    NodeMetricUpdater nodeMetricUpdater;
    if (node instanceof DefaultNode) {
      nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
    } else {
      nodeMetricUpdater = NoopNodeMetricUpdater.INSTANCE;
    }
    return connect(node.getEndPoint(), null, null, options, nodeMetricUpdater, isIdentified(node));
  }

  public CompletionStage<DriverChannel> connect(
      Node node, Integer shardId, DriverChannelOptions options) {
    NodeMetricUpdater nodeMetricUpdater;
    if (node instanceof DefaultNode) {
      nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
    } else {
      nodeMetricUpdater = NoopNodeMetricUpdater.INSTANCE;
    }
    return connect(
        node.getEndPoint(),
        node.getShardingInfo(),
        shardId,
        options,
        nodeMetricUpdater,
        isIdentified(node));
  }

  /**
   * Whether we know <b>which</b> node we are connecting to, as opposed to merely which address to
   * try. Both {@link #spreadAcrossAddresses} and {@link #sameServerAtEveryAddress} turn on it,
   * because an unidentified contact-point name may expand to addresses of <b>different</b> nodes,
   * while every address of an identified node is that same node.
   *
   * <p>{@link Node#getHostId()} is null exactly for a contact point, and stays null for the life of
   * that instance: {@code MetadataManager.registerNode} mints a fresh {@code DefaultNode} from each
   * {@code system.local}/{@code system.peers} row rather than back-filling the contact point it was
   * reached through, and those ephemeral contact-point nodes are never added to metadata. So this
   * is not a state a contact point grows out of once the driver has read host ids -- the driver
   * simply stops using that instance for anything except the reconnection fallback, which keeps
   * handing it back.
   */
  private static boolean isIdentified(Node node) {
    return node.getHostId() != null;
  }

  @VisibleForTesting
  CompletionStage<DriverChannel> connect(
      EndPoint endPoint,
      NodeShardingInfo shardingInfo,
      Integer shardId,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater) {
    // A bare endpoint carries no host id, so this matches the contact-point case (see
    // isIdentified()).
    return connect(endPoint, shardingInfo, shardId, options, nodeMetricUpdater, false);
  }

  @VisibleForTesting
  CompletionStage<DriverChannel> connect(
      EndPoint endPoint,
      NodeShardingInfo shardingInfo,
      Integer shardId,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      boolean nodeIsIdentified) {
    CompletableFuture<DriverChannel> resultFuture = new CompletableFuture<>();

    ProtocolVersion currentVersion;
    boolean isNegotiating;
    if (this.protocolVersion != null) {
      currentVersion = protocolVersion;
      isNegotiating = false;
    } else {
      currentVersion = context.getProtocolVersionRegistry().highestNonBeta();
      isNegotiating = true;
    }

    connect(
        endPoint,
        shardingInfo,
        shardId,
        options,
        nodeMetricUpdater,
        currentVersion,
        isNegotiating,
        nodeIsIdentified,
        resultFuture);
    return resultFuture;
  }

  private void connect(
      EndPoint endPoint,
      NodeShardingInfo shardingInfo,
      Integer shardId,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      ProtocolVersion currentVersion,
      boolean isNegotiating,
      boolean nodeIsIdentified,
      CompletableFuture<DriverChannel> resultFuture) {

    // Built once per connect() rather than once per candidate: it is the only handle on the Netty
    // AddressResolverGroup (see resolveCandidates()), and it means the user's
    // afterBootstrapInitialized() hook runs once per logical connection instead of once per address
    // attempt. Each attempt gets its own clone() with its own handler.
    //
    // The event loop is likewise picked once per connect() and shared by name resolution and the
    // channel itself (the per-attempt clones are bound to it, see connectToAddress()). Advancing
    // the group's round-robin chooser exactly once per connect keeps channels evenly distributed:
    // taking one loop for resolution and letting Bootstrap.connect() take another would advance
    // the chooser twice per connect, parking all channels on half the loops with the default
    // power-of-two chooser. It also mirrors what Netty itself does with an unresolved address:
    // Bootstrap resolves on the connecting channel's own event loop.
    Bootstrap baseBootstrap;
    EventLoop eventLoop;
    try {
      baseBootstrap = newBootstrap();
      eventLoop = context.getNettyOptions().ioEventLoopGroup().next();
    } catch (Throwable e) {
      resultFuture.completeExceptionally(e);
      return;
    }

    // EndPoint.resolve() is contractually non-blocking and performs no name resolution, so it is
    // safe to call here even though connect() runs on the admin event loop for control-connection
    // reconnects. Everything a name needs to become connectable happens in resolveCandidates().
    SocketAddress address;
    try {
      address = endPoint.resolve();
    } catch (Throwable e) {
      resultFuture.completeExceptionally(e);
      return;
    }
    if (address == null) {
      // EndPoint.resolve() is contractually non-null; fail fast instead of NPE-ing inside an
      // event-loop task later, which would leave resultFuture hanging (see resolveCandidates()).
      resultFuture.completeExceptionally(
          new IllegalArgumentException("EndPoint.resolve() returned null: " + endPoint));
      return;
    }

    // Guarded for the same reason resolve() is: addressesAreInterchangeable() calls through to
    // PinnableEndPoint.addressesAreInterchangeable(), another method the endpoint implementation
    // supplies and can therefore throw from. As a bare argument to resolveCandidates() it would
    // escape connect() synchronously, and nothing upstream would complete resultFuture:
    // ControlConnection.reconnect() does not wrap its connect() call, and the recursive ones run
    // inside a whenCompleteAsync callback with no catch, so the throwable would be swallowed and
    // the attempt left hanging with Reconnection stuck in ATTEMPT_IN_PROGRESS.
    //
    // Throwable, not Exception, and likewise for the two guards above: an endpoint supplied by
    // someone else can fail with an Error just as easily as with an exception -- a
    // NoClassDefFoundError or ExceptionInInitializerError out of lazy class initialization in a
    // shaded or OSGi deployment, an AssertionError under -ea -- and a hang is the outcome either
    // way. Every other guard in this class that owns a future's completion already catches
    // Throwable.
    boolean interchangeable;
    try {
      interchangeable = addressesAreInterchangeable(endPoint, address);
    } catch (Throwable e) {
      resultFuture.completeExceptionally(e);
      return;
    }

    // Two questions over the same two facts. Not each other's negation: a connect can be both
    // (an identified node behind an SNI proxy) or neither (an identified node on a plain name).
    boolean spreadAcrossAddresses = spreadAcrossAddresses(nodeIsIdentified, interchangeable);
    boolean sameServerAtEveryAddress = sameServerAtEveryAddress(nodeIsIdentified, interchangeable);

    resolveCandidates(baseBootstrap, address, eventLoop, spreadAcrossAddresses)
        .whenComplete(
            (candidates, error) -> {
              if (error != null) {
                Throwable cause =
                    (error instanceof CompletionException && error.getCause() != null)
                        ? error.getCause()
                        : error;
                resultFuture.completeExceptionally(cause);
                return;
              }
              tryNextCandidate(
                  baseBootstrap,
                  eventLoop,
                  endPoint,
                  shardingInfo,
                  shardId,
                  options,
                  nodeMetricUpdater,
                  currentVersion,
                  isNegotiating,
                  sameServerAtEveryAddress,
                  resultFuture,
                  candidates,
                  0,
                  new ArrayList<>());
            });
  }

  /**
   * Builds the {@link Bootstrap} shared by every connection attempt of a single {@code connect()}
   * call, including the user's {@link NettyOptions#afterBootstrapInitialized(Bootstrap)} hook. Per
   * attempt, {@link #connectToAddress} takes a {@link
   * Bootstrap#clone(io.netty.channel.EventLoopGroup)} of it bound to the event loop the connect
   * picked, and installs its own handler; the copy carries the resolver configuration over. The
   * base bootstrap itself keeps the full I/O group, so the hook observes the same group as always.
   */
  private Bootstrap newBootstrap() {
    NettyOptions nettyOptions = context.getNettyOptions();
    Bootstrap bootstrap =
        new Bootstrap()
            .group(nettyOptions.ioEventLoopGroup())
            .channel(nettyOptions.channelClass())
            .option(ChannelOption.ALLOCATOR, nettyOptions.allocator());
    nettyOptions.afterBootstrapInitialized(bootstrap);
    if (bootstrap.config().handler() != null && loggedHandlerWarning.compareAndSet(false, true)) {
      LOG.warn(
          "[{}] NettyOptions.afterBootstrapInitialized() installed a channel handler on the"
              + " bootstrap; it will be replaced by the driver's own handler. Use"
              + " NettyOptions.afterChannelInitialized() to customize the pipeline instead.",
          logPrefix);
    }
    // Same shape as the handler above, and for the same reason: connectToAddress() clones this
    // bootstrap with clone(eventLoop), which assigns the group unconditionally, so a group the
    // hook set here is dropped and the driver's own ioEventLoopGroup is used instead. Silently
    // moving a deployment's I/O back onto the driver's threads is exactly the kind of thing that
    // is noticed months later, so say it once.
    if (bootstrap.config().group() != nettyOptions.ioEventLoopGroup()
        && loggedGroupWarning.compareAndSet(false, true)) {
      LOG.warn(
          "[{}] NettyOptions.afterBootstrapInitialized() replaced the bootstrap's event loop"
              + " group; it will be ignored, because each connection attempt is bound to an event"
              + " loop picked from NettyOptions.ioEventLoopGroup(). Override ioEventLoopGroup() to"
              + " run driver I/O on your own threads.",
          logPrefix);
    }
    return bootstrap;
  }

  /**
   * Turns the address an {@link EndPoint} denotes into the concrete, connectable addresses to try,
   * expanding it to <b>all</b> the addresses it maps to when it is a name.
   *
   * <p>Expansion goes through the bootstrap's Netty {@link AddressResolverGroup} rather than a
   * direct {@code InetAddress.getAllByName()} call, so a custom resolver installed via {@link
   * NettyOptions#afterBootstrapInitialized(Bootstrap)} is honoured — that is the resolver an
   * unresolved address would have reached had it been handed straight to {@code
   * Bootstrap.connect()}, as it was before multi-address support. This is also why endpoints are
   * forbidden from resolving names themselves (see {@link EndPoint#resolve()}): doing it here is
   * the only way to keep that configuration point working, and the only way to keep {@code
   * resolve()} non-blocking.
   *
   * <p>Whether an address needs resolving at all is the resolver's decision, not ours: exactly as
   * in {@code Bootstrap#doResolveAndConnect0}, the address is passed through untouched only when
   * the resolver says it does not {@linkplain AddressResolver#isSupported support} it (e.g. {@link
   * io.netty.channel.local.LocalAddress}) or that it {@linkplain AddressResolver#isResolved is
   * already resolved}. Both are overridable, and a custom resolver may well report an
   * already-resolved address as unresolved in order to redirect it — Netty consulted it either way,
   * so a pre-check here on {@code InetSocketAddress#isUnresolved()} would silently take that
   * configuration point away for every connect to an already-resolved node, which is to say for
   * almost every connect. A null group means the user called {@link Bootstrap#disableResolver()},
   * which is likewise respected.
   *
   * <p>On the two branches where <b>nothing</b> is going to resolve the address -- no resolver at
   * all, or one that declines it -- an unresolved IP literal is materialized locally instead of
   * being failed; see {@link #materializeLiteral}. A host name still fails there, with a message
   * naming which of the two put it in that position. The third pass-through branch is different in
   * kind: a resolver that reports the address already resolved has claimed it, so the address goes
   * out untouched and only a warning is logged (see {@link #warnAboutPassThrough}).
   *
   * <p>Note that with Netty's <i>default</i> resolver the lookup blocks the event loop it runs on,
   * because {@code DefaultNameResolver} performs {@code InetAddress.getAllByName()} inline. That is
   * the pre-existing behaviour of handing an unresolved address to {@code Bootstrap.connect()}, and
   * it is an I/O loop, never the admin loop that {@code connect()} is called from. Deployments that
   * need non-blocking resolution can now install {@code DnsAddressResolverGroup} and have it take
   * effect.
   */
  private CompletionStage<List<SocketAddress>> resolveCandidates(
      Bootstrap bootstrap,
      SocketAddress address,
      EventLoop eventLoop,
      boolean spreadAcrossAddresses) {

    AddressResolverGroup<?> resolverGroup = bootstrap.config().resolver();
    if (resolverGroup == null) {
      // Bootstrap.disableResolver(): the user wants the address passed through as-is, which only
      // works if it is usable as-is -- or can be made so without resolving anything.
      SocketAddress literal = materializeLiteral(address);
      if (literal != null) {
        return CompletableFuture.completedFuture(Collections.singletonList(literal));
      }
      IllegalStateException unusable =
          unusableWithoutResolution(
              address,
              "the bootstrap has name resolution disabled",
              "Either remove Bootstrap.disableResolver() from"
                  + " NettyOptions.afterBootstrapInitialized(), or supply an already-resolved"
                  + " address.");
      return (unusable != null)
          ? CompletableFutures.failedFuture(unusable)
          : CompletableFuture.completedFuture(Collections.singletonList(address));
    }

    // The supplied event loop is the same one the channel will be registered on (see connect()),
    // which is what Netty itself does with an unresolved address: Bootstrap resolves on the
    // connecting channel's own event loop. Its transport also matches the channel class, which
    // matters because DnsAddressResolverGroup registers a datagram channel on the executor it
    // resolves for.
    CompletableFuture<List<SocketAddress>> result = new CompletableFuture<>();
    // Every path below must complete `result`: nothing at this stage has a timeout, so a task or
    // listener that dies with the future still pending (Netty swallows their throwables, it only
    // logs them) would hang the connect attempt -- and with it control-connection init or a pool
    // reconnect -- forever. Hence the blanket catches around the task body, the listener body, and
    // the execute() call itself (which throws RejectedExecutionException while shutting down).
    try {
      eventLoop.execute(
          () -> {
            try {
              AddressResolver<? extends SocketAddress> resolver =
                  resolverGroup.getResolver(eventLoop);
              boolean unsupported = !resolver.isSupported(address);
              if (unsupported || resolver.isResolved(address)) {
                // Nothing for the resolver to do; same short-circuit as
                // Bootstrap#doResolveAndConnect0. The two halves are not the same situation,
                // though, and are not treated the same.
                //
                // An address the resolver *declines* is in the same position as one with no
                // resolver at all: nothing has taken responsibility for it and nothing downstream
                // will resolve it. So it gets the same check, and the same rescue for an IP
                // literal, which needs no name service to begin with.
                //
                // An address the resolver reports as *already resolved* is a claim, and the claim
                // is honoured even when the address plainly is not resolved. That combination is
                // not a broken resolver, it is NoopAddressResolverGroup: Netty's documented way of
                // saying "leave the name alone, something in the pipeline will deal with it",
                // which is exactly what a ProxyHandler installed through
                // NettyOptions#afterChannelInitialized(Channel) does -- it intercepts the connect
                // and sends the name on to the proxy instead of to a socket. Netty itself hands
                // such an address straight to doConnect(), so refusing it here would turn a
                // supported configuration into a hard failure of every connect for the whole
                // session. It passes through, with one warning for the deployment that arrived
                // here by accident rather than on purpose.
                if (!unsupported) {
                  warnAboutPassThrough(address, resolver);
                  result.complete(Collections.singletonList(address));
                  return;
                }
                SocketAddress literal = materializeLiteral(address);
                if (literal != null) {
                  result.complete(Collections.singletonList(literal));
                  return;
                }
                IllegalStateException unusable =
                    unusableWithoutResolution(
                        address,
                        "the configured resolver does not support this address",
                        "Either install a resolver that supports it in"
                            + " NettyOptions.afterBootstrapInitialized(), or supply an"
                            + " already-resolved address.");
                if (unusable != null) {
                  result.completeExceptionally(unusable);
                } else {
                  result.complete(Collections.singletonList(address));
                }
                return;
              }
              resolver
                  .resolveAll(address)
                  .addListener(
                      (Future<? super List<? extends SocketAddress>> future) -> {
                        try {
                          if (!future.isSuccess()) {
                            result.completeExceptionally(future.cause());
                            return;
                          }
                          @SuppressWarnings("unchecked")
                          List<? extends SocketAddress> addresses =
                              (List<? extends SocketAddress>) future.getNow();
                          if (addresses == null || addresses.isEmpty()) {
                            result.completeExceptionally(
                                new IllegalStateException(
                                    "Resolver returned no address for " + address));
                            return;
                          }
                          List<SocketAddress> connectable =
                              dropUnresolved(address, reattachHostnames(address, addresses));
                          if (connectable.isEmpty()) {
                            result.completeExceptionally(
                                new IllegalStateException(
                                    String.format(
                                        "Cannot connect to %s: the configured resolver (%s) "
                                            + "expanded it to %d address(es) and every one of them "
                                            + "is still unresolved, so nothing will resolve them.",
                                        address, resolver.getClass().getName(), addresses.size())));
                            return;
                          }
                          result.complete(shuffleAndLimit(connectable, spreadAcrossAddresses));
                        } catch (Throwable t) {
                          result.completeExceptionally(t);
                        }
                      });
            } catch (Throwable t) {
              result.completeExceptionally(t);
            }
          });
    } catch (Throwable t) {
      result.completeExceptionally(t);
    }
    return result;
  }

  /**
   * Says once that a resolver reported an address it plainly has not resolved, and that {@link
   * #resolveCandidates} took it at its word.
   *
   * <p>The claim is honoured because it is a supported thing to say. {@code
   * NoopAddressResolverGroup} reports every address resolved, and it is Netty's own way to hand
   * name resolution to something in the pipeline -- a {@code ProxyHandler} added through {@link
   * NettyOptions#afterChannelInitialized(io.netty.channel.Channel)}, which intercepts the connect
   * and sends the unresolved name to the proxy. Netty's {@code Bootstrap#doResolveAndConnect0}
   * short-circuits on exactly this and calls {@code doConnect()} with the address untouched, so
   * that deployment worked before multi-address support and has to keep working. It is also the one
   * path that leaves {@link PinnableEndPoint#pinTo} an unresolved address, which is why that method
   * documents refusing one.
   *
   * <p>But the same claim is what a resolver whose {@code isResolved()} is simply wrong makes, and
   * that deployment gets no resolution and no proxy either -- just {@code
   * UnresolvedAddressException} out of {@code doConnect}, naming neither the address nor the
   * reason. Hence the warning: it costs the intentional case one log line and gives the accidental
   * one the only diagnosis it will get. Only for an address that really is unresolved, since a
   * resolved one passing through is unremarkable.
   */
  private void warnAboutPassThrough(SocketAddress address, AddressResolver<?> resolver) {
    if (address instanceof InetSocketAddress
        && ((InetSocketAddress) address).isUnresolved()
        && loggedPassThroughWarning.compareAndSet(false, true)) {
      LOG.warn(
          "[{}] {} reports {} as already resolved, so it is being connected to unresolved. That is"
              + " what NoopAddressResolverGroup does when something in the pipeline resolves the"
              + " name instead (a ProxyHandler added in NettyOptions.afterChannelInitialized()); if"
              + " nothing does, the connect will fail with UnresolvedAddressException. This message"
              + " is logged once.",
          logPrefix,
          resolver.getClass().getName(),
          address);
    }
  }

  /**
   * The failure to report when {@link #resolveCandidates} is about to pass an address through
   * without resolving it, or {@code null} if passing it through is fine.
   *
   * <p>{@link #connectToAddress} hands the candidate to a bootstrap clone with {@link
   * Bootstrap#disableResolver()}, so an address that is still unresolved by the time it gets there
   * cannot connect: Netty raises {@code UnresolvedAddressException} from inside {@code doConnect},
   * naming neither the address nor the reason nothing resolved it. That is a hard failure of every
   * connection attempt for the whole session, and it is worth a message that says which endpoint
   * and which configuration produced it -- the endpoints most likely to hit it (SNI, client routes)
   * hand out unresolved addresses by design, and contact-point hostnames are now always kept
   * unresolved.
   *
   * <p>Deliberately not a general {@code isUnresolved()} pre-check on every path: see {@link
   * #resolveCandidates}'s javadoc for why an address the resolver merely declines to touch must
   * still go through. This fires only where nothing downstream will resolve it either -- as does
   * {@link #dropUnresolved}, which applies the same reasoning to what {@code resolveAll} returns.
   *
   * <p>Its callers try {@link #materializeLiteral} first, so this is reached only by an address
   * that genuinely needs a name service. "Supply an already-resolved address" is therefore always
   * advice about a host name.
   *
   * @param why what put the address in this position, in a clause that reads after "it is an
   *     unresolved address and".
   * @param fix what the operator should do about it. Each caller supplies its own: the two
   *     situations that reach here are diagnosed differently, and naming the wrong one sends the
   *     operator looking for configuration nobody wrote.
   */
  private static IllegalStateException unusableWithoutResolution(
      SocketAddress address, String why, String fix) {
    if (!(address instanceof InetSocketAddress) || !((InetSocketAddress) address).isUnresolved()) {
      return null;
    }
    return new IllegalStateException(
        String.format(
            "Cannot connect to %s: it is an unresolved address and %s, so nothing will resolve it. %s",
            address, why, fix));
  }

  /**
   * The address as something connectable when nothing is going to resolve it, or {@code null} if it
   * is not an unresolved IP literal.
   *
   * <p>A literal needs no name service, so an endpoint that holds one has no business failing on a
   * path where resolution is unavailable -- and endpoints now hold one routinely, contact points
   * being kept unresolved whatever they were written as (see {@code
   * SessionBuilder#addContactPoint}). Before that, {@code 127.0.0.1:9042} arrived here already
   * resolved and {@link Bootstrap#disableResolver()} worked with it; this keeps that true.
   *
   * <p>Deliberately not a general pre-check. It is applied only where {@link #resolveCandidates} is
   * already committed to passing the address through unresolved, never before an enabled resolver
   * has been consulted: a custom resolver is entitled to redirect a literal, exactly as it is
   * entitled to redirect a name, and testing for one earlier would take that away.
   *
   * <p>Consults no name service, which is what makes it safe on both call sites -- one runs on a
   * Netty I/O loop, the other on whatever thread called {@code connect()}, the admin loop for the
   * control connection. {@link InetAddress#getByName} goes to DNS only for a name, and {@link
   * AddressUtils#carriesName} has just established there is none. The one exception is a literal
   * carrying a <b>named</b> IPv6 zone ({@code fe80::1%eth0}): the JDK turns the name into a scope
   * id through {@code NetworkInterface}, which is a syscall rather than a lookup. Bounded and
   * local, so it is accepted rather than special-cased.
   *
   * <p>Two spellings {@link AddressUtils#carriesName} calls literals do not survive {@code
   * getByName}, and both fall through to the caller's diagnostic -- which then gives advice about
   * host names, the one thing {@link #unusableWithoutResolution} promises it is always about:
   *
   * <ul>
   *   <li>A zone naming an interface this host does not have, or one that carries no address in
   *       that scope -- {@code fe80::1%eth0} where there is no {@code eth0}, {@code fe80::1%lo}
   *       where {@code lo} has no link-local address. Measured on JDK 11.0.30. Such an address
   *       could not have been connected to anyway, so the outcome is right and only the wording is
   *       wrong.
   *   <li>The <b>shorthand</b> IPv4 forms {@code getByName} accepts and Guava's parser does not:
   *       {@code 127.1} becomes {@code /127.0.0.1} for the JDK and for Netty's default resolver,
   *       but {@code InetAddresses#isInetAddress} requires four dotted parts, so {@code
   *       carriesName} calls it a host name and this returns {@code null} at the first gate. Such a
   *       contact point works normally and fails only where no resolver runs. Deliberately not
   *       fixed by loosening the gate: {@code getByName("1234")} returns {@code /0.0.4.210}, so a
   *       test as lenient as the JDK's would silently turn an all-digit host name into a packed
   *       IPv4 address. Guava's strictness is the guard, and being strict costs an unusual spelling
   *       an unusual configuration.
   * </ul>
   *
   * <p>The literal is re-attached as the address's host-name label rather than left off, so that
   * {@code getHostName()} stays a field read answering what the operator configured. A nameless
   * address sends {@code DefaultSslEngineFactory} to a reverse lookup on an event loop and has it
   * validate the certificate against a PTR record -- the same hazard {@link #reattachHostname}'s
   * literal branch exists to prevent, and the reason the label goes on here rather than being left
   * to that method, whose byte-matching re-derives through {@link AddressUtils#parseLiteral} what
   * is known here by construction (and which drops a zone rather than carrying it). The label is
   * the spelling that was configured, less the brackets of the URI form -- {@link
   * InetAddress#getByAddress(String, byte[])} strips those from any host name it is handed. A
   * non-canonically written literal then makes {@link AddressUtils#carriesName} report {@code true}
   * for the result: the same imprecision that method already documents, and nothing re-labels a
   * candidate twice.
   */
  @VisibleForTesting
  static SocketAddress materializeLiteral(SocketAddress address) {
    if (!(address instanceof InetSocketAddress)) {
      return null;
    }
    InetSocketAddress inet = (InetSocketAddress) address;
    if (!inet.isUnresolved() || AddressUtils.carriesName(inet)) {
      return null;
    }
    String literal = inet.getHostString();
    try {
      return new InetSocketAddress(
          AddressUtils.withHostName(literal, InetAddress.getByName(literal)), inet.getPort());
    } catch (UnknownHostException notAfterAll) {
      // carriesName() and getByName() disagreeing about what a literal is: fall through to the
      // diagnostic the caller was about to raise, which says more than a bare parse failure.
      return null;
    }
  }

  /** Applies {@link #reattachHostname} to every expanded candidate. */
  private static List<SocketAddress> reattachHostnames(
      SocketAddress original, List<? extends SocketAddress> candidates) {
    List<SocketAddress> result = new ArrayList<>(candidates.size());
    for (SocketAddress candidate : candidates) {
      result.add(reattachHostname(original, candidate));
    }
    return result;
  }

  /**
   * Drops the candidates a resolver returned still unresolved, keeping the order of the rest.
   *
   * <p>{@code resolveAll} is contracted to return resolved addresses, but a custom resolver that
   * rewrites what it is given -- which {@link #resolveCandidates} deliberately supports -- may hand
   * back one that is not. Such a candidate cannot connect: {@link #connectToAddress} uses a
   * bootstrap clone with {@link Bootstrap#disableResolver()}, so nothing downstream will resolve it
   * either, and Netty raises {@code UnresolvedAddressException} from inside {@code doConnect}. This
   * is the same reasoning as {@link #unusableWithoutResolution}, applied where the addresses come
   * from the resolver itself; dropping them here rather than after the cap means the cap counts
   * only addresses that can actually be tried. The caller reports the case where nothing is left,
   * which is the one that fails every connection attempt for the whole session.
   */
  private List<SocketAddress> dropUnresolved(
      SocketAddress original, List<SocketAddress> candidates) {
    List<SocketAddress> result = new ArrayList<>(candidates.size());
    for (SocketAddress candidate : candidates) {
      if (candidate instanceof InetSocketAddress
          && ((InetSocketAddress) candidate).isUnresolved()) {
        LOG.debug(
            "[{}] Resolver returned {} for {} but it is still unresolved, skipping it",
            logPrefix,
            candidate,
            original);
      } else {
        result.add(candidate);
      }
    }
    return result;
  }

  /**
   * Re-attaches the {@code original} address's host name to one of the resolved candidates it
   * expanded to, whatever name that candidate carries.
   *
   * <p>The JDK and Netty-DNS resolvers already attach the queried name to the {@link InetAddress}es
   * they return, so this is a no-op for them. A custom resolver, however, may build its results
   * from raw address bytes, or label them with a canonical/CNAME name of its own. The channel's
   * pinned endpoint is built from the candidate (see {@link PinnableEndPoint}), and it is what
   * {@code DefaultSslEngineFactory} and {@code SniSslEngineFactory} derive the SSL peer host from,
   * inside the channel initializer. So whatever name the candidate carries is the name TLS hostname
   * verification checks the server certificate against, and the only name that may be is the one
   * the user configured: with a nameless address, {@code InetSocketAddress#getHostName()}
   * additionally triggers a blocking reverse-DNS lookup on the event loop and validation falls back
   * to the IP or the PTR record, and with a resolver-supplied label it validates a name the
   * operator never chose. Hence the queried name always wins here; before multi-address support the
   * initializer kept the original endpoint and Netty resolved only the TCP destination, which had
   * the same effect.
   *
   * <p>Re-attaching changes nothing else: {@link AddressUtils#withHostName} performs no lookup, the
   * TCP connect target is the same IP, and a resolved {@link InetSocketAddress}'s equality ignores
   * host names, so pinning and the pin-equality shortcuts are unaffected. A scoped IPv6 candidate
   * keeps its zone.
   *
   * <p>An <b>IP literal</b> gets its own literal re-attached, and only when the resolver handed
   * back that very address. Leaving the candidate nameless there would not be neutral: a nameless
   * address is exactly what {@code InetSocketAddress#getHostName()} answers with a blocking reverse
   * lookup, so {@code DefaultSslEngineFactory} would validate the certificate against a PTR record
   * instead of the literal the operator configured, on a Netty I/O loop — where before, contact
   * points were kept unresolved and the literal came back with no lookup at all. Labelling with the
   * literal keeps {@code getHostName()} a field read that answers the literal, which is what it
   * answered before. (A non-canonically written IPv6 literal then makes {@link
   * AddressUtils#carriesName} report {@code true} for the labelled candidate: the same imprecision
   * that method already documents, and nothing re-labels a candidate twice.)
   *
   * <p>A candidate the resolver <b>redirected</b> to a different IP is left alone: labelling it
   * with the literal form of the one we asked for would invent a name that resolves to something
   * else.
   *
   * <p>A <b>resolved</b> original is treated exactly like an unresolved one, and reaches this at
   * all only because a resolver may report an already-resolved address as unresolved in order to
   * redirect it (see {@link #resolveCandidates}). Its host string is not as trustworthy: it renders
   * a mutable field on the shared {@link InetAddress} (see {@link AddressUtils#carriesName}), which
   * holds the name the operator configured when the address was built from one -- {@code new
   * InetSocketAddress("db.example.com", 9042)} resolves eagerly and keeps the name -- but holds
   * whatever reverse-DNS name an earlier TLS handshake cached when it was not. The two are
   * indistinguishable from the object.
   *
   * <p>Re-attaching regardless is still the better of the two, because of what the alternative
   * costs. Leaving a redirected candidate unlabelled does not leave it neutral: {@code
   * DefaultSslEngineFactory} derives the TLS peer host from {@code resolve()}, which for the pinned
   * copy is this candidate, and for a nameless address that is a blocking reverse lookup on an
   * event loop. So in the configured-name case the choice is between validating the certificate
   * against the configured DNS SAN and validating it against a PTR record -- which is what the
   * pre-multi-address path did, when {@code resolve()} still handed back the endpoint's own
   * address. And in the cached-PTR case it is between one address's PTR name and another's, neither
   * of which the operator ever wrote. One case is fixed and the other is a wash.
   */
  @VisibleForTesting
  static SocketAddress reattachHostname(SocketAddress original, SocketAddress candidate) {
    if (!(original instanceof InetSocketAddress) || !(candidate instanceof InetSocketAddress)) {
      return candidate;
    }
    InetSocketAddress originalInet = (InetSocketAddress) original;
    InetSocketAddress candidateInet = (InetSocketAddress) candidate;
    InetAddress candidateIp = candidateInet.getAddress();
    if (candidateIp == null) {
      return candidate;
    }
    String hostString = originalInet.getHostString();
    if (AddressUtils.carriesName(originalInet)) {
      // The queried name always wins -- unless the candidate already carries it, which is the
      // common case (the JDK and Netty-DNS resolvers attach it themselves). getHostString() never
      // looks anything up, and for a nameless candidate it falls back to the IP literal, which
      // cannot equal a name -- so this test does not mistake one for the other.
      return hostString.equals(candidateInet.getHostString())
          ? candidate
          : relabel(candidateInet, candidateIp, hostString);
    }
    // An IP literal: re-attach it only to the address it denotes, so a redirect stays unlabelled.
    // The whole original string, zone and brackets included, becomes the label; only the address
    // part is matched on, which is what AddressUtils#parseLiteral hands back.
    //
    // Parsed there rather than here, next to the AddressUtils#isLiteral that put us on this branch
    // in the first place. The two have to accept the same strings -- a literal recognised here and
    // rejected by the parse returns the candidate unlabelled, which is the one outcome this branch
    // exists to prevent: getHostName() would then answer with a reverse lookup and the SSL engine
    // would validate against a PTR record -- and keeping the parse on this side made that agreement
    // a matter of comment rather than of code.
    InetAddress literal = AddressUtils.parseLiteral(hostString);
    if (literal == null) {
      return candidate;
    }
    // A byte-exact comparison, with IPv4 and IPv6 told apart by array length. This is equivalent
    // to InetAddress.equals(), which ignores the scope id as well -- verified on JDK 11.0.30, where
    // two Inet6Addresses built from the same bytes with scope ids 3 and 5 compare equal -- and is
    // written out so that the scope-blindness is visible rather than inherited: the zone was split
    // off just above and goes into the label, never into this test.
    //
    // The consequence is accepted, not overlooked. A candidate carrying a different scope than the
    // configured zone still matches here and is relabelled with that zone, so getHostString() names
    // one interface while the connect goes out on the candidate's own. Reaching that needs a
    // resolver that answers a zoned literal with a different scope than it was asked about; the
    // alternative -- resolving the zone name through NetworkInterface to compare it -- buys a
    // NetworkInterface lookup on the connect path for that one case, so it is deferred rather than
    // taken here.
    if (!Arrays.equals(literal.getAddress(), candidateIp.getAddress())) {
      return candidate;
    }
    // Deliberately no getHostString() short-circuit on this branch: a *nameless* candidate's host
    // string is its own IP literal, so it compares equal to the label about to be attached and the
    // relabel would be skipped -- leaving getHostName() to answer with a reverse lookup, which is
    // the one thing this branch exists to prevent. Relabelling is idempotent, so paying for it
    // unconditionally is cheaper than telling the two apart.
    return relabel(candidateInet, candidateIp, hostString);
  }

  /**
   * {@code candidate} rebuilt with {@code hostName} as its label, or itself if that is not
   * possible.
   */
  private static SocketAddress relabel(
      InetSocketAddress candidate, InetAddress candidateIp, String hostName) {
    try {
      return new InetSocketAddress(
          AddressUtils.withHostName(hostName, candidateIp), candidate.getPort());
    } catch (UnknownHostException impossible) {
      // getByAddress only rejects illegal byte lengths, and these bytes come from a real
      // InetAddress; keep the raw candidate rather than failing the connect over a cosmetic step.
      return candidate;
    }
  }

  /**
   * Whether every address this endpoint expands to is another way in to the same server, which only
   * the endpoint knows — see {@link PinnableEndPoint#addressesAreInterchangeable(SocketAddress)}
   * for the two cases and why they differ. An endpoint that does not implement {@link
   * PinnableEndPoint} is treated as not interchangeable, which is also the conservative reading for
   * a third-party implementation.
   *
   * <p>Asked <b>once</b> per connect, in {@link #connect}, with both of the booleans that depend on
   * it derived from the one answer: an endpoint backed by mutable state asked twice could answer
   * about a different address than the one being dialled, and the two would then disagree. {@code
   * resolvedAddress} is passed in rather than re-derived for the same reason — it is what {@link
   * EndPoint#resolve()} already returned for this connect.
   */
  @VisibleForTesting
  static boolean addressesAreInterchangeable(EndPoint endPoint, SocketAddress resolvedAddress) {
    return endPoint instanceof PinnableEndPoint
        && ((PinnableEndPoint) endPoint).addressesAreInterchangeable(resolvedAddress);
  }

  /**
   * Whether {@link #shuffleAndLimit} may spread this connect across the addresses the endpoint
   * expands to.
   *
   * <p>A <b>contact point</b> always may: its addresses may well be different nodes, so there is no
   * node identity to preserve, and spreading both balances load and varies which address an attempt
   * starts from. An {@linkplain #isIdentified(Node) identified} node may only when its addresses
   * are interchangeable -- which is what {@code SniEndPoint#resolve()} used to do for itself,
   * rotating through the sorted records, before resolution moved to this layer.
   */
  @VisibleForTesting
  static boolean spreadAcrossAddresses(boolean nodeIsIdentified, boolean interchangeable) {
    return !nodeIsIdentified || interchangeable;
  }

  /**
   * Whether a rejection observed at one address is a verdict on the <b>server</b>, and so on every
   * remaining address, rather than on the record that reached it. See {@link #isNodeWideFailure},
   * the only thing that asks.
   *
   * <p>An identified node always qualifies: every address of it is that same node. An unidentified
   * contact point qualifies only when its addresses are interchangeable, i.e. when the endpoint
   * says they all lead to one server. Otherwise they may be distinct servers running distinct
   * software, which is not an edge case but what a rolling upgrade looks like from the client.
   *
   * <p>Which reads as the opposite of what {@link #shuffleAndLimit} says about the same input, and
   * the difference is deliberate rather than an oversight. One {@code Node} denoting one server is
   * the driver's model, and a configured {@code AddressTranslator} that hands back a name can
   * violate it. The two questions answer that differently because being wrong costs differently:
   * withholding the shuffle is free, so spreading hedges and keeps the resolver's order, while a
   * failure has to be attributed to something and every address of an identified node is all this
   * layer has. So this trusts the model, and the mirror case it leaves unrescued -- a heterogeneous
   * identified node -- is owned in {@link #isNodeWideFailure}, where the cost of it is spelt out.
   */
  @VisibleForTesting
  static boolean sameServerAtEveryAddress(boolean nodeIsIdentified, boolean interchangeable) {
    return nodeIsIdentified || interchangeable;
  }

  /**
   * Truncates the expanded address list to {@code advanced.connection.max-candidate-addresses},
   * shuffling it first when the addresses may be spread across (see {@link
   * #spreadAcrossAddresses}).
   *
   * <p>The shuffle spreads load: without it, every connection would try the resolver's first
   * address first and healthy connections would pile onto one IP, while the whole point of a
   * multi-record name is usually to spread them. A fresh random order per connect also means
   * successive attempts start at different addresses, with no per-name counter state to maintain
   * and nothing depending on the order the resolver, or a sort, happened to choose.
   *
   * <p>Where the order is kept instead, it is because the addresses are <b>not</b> known to be
   * interchangeable: an identified node whose endpoint is an unresolved <i>name</i> that may map to
   * several hosts, which is what a configured {@code AddressTranslator} returns by default ({@code
   * SubnetAddressTranslator} under {@code resolve-addresses = false}). Each pool connection is its
   * own {@code connect()}, so shuffling there would land one {@code Node}'s channels on different
   * hosts while routing, shard awareness and per-node metrics attribute them all to that node.
   * Keeping the resolver's order means such a pool converges on one address, as it did before
   * multi-address support -- {@code Bootstrap.connect()} resolved through {@code resolve()},
   * singular, i.e. the first record -- while the remaining addresses still serve as fallback.
   *
   * <p>The cap bounds what a single connect attempt can cost: every address tried is a full TCP
   * connect plus init handshake -- and, with wrong credentials, a rejected login (see {@link
   * #tryNextCandidate} on why an authentication failure does not stop the loop). For a shuffled
   * list, a capped attempt tries a different sample of the addresses each time, so a name with more
   * records than the cap still reaches all of them across successive attempts; one attempt just no
   * longer walks them all. Where the order is kept, the cap is a hard limit -- a capped attempt
   * keeps dialing the same prefix of the list, so records beyond it are never reached. That is
   * accepted rather than worked around: it is still strictly more than the single address such an
   * endpoint got before multi-address support, and rotating the window instead would give up the
   * convergence the stable order exists for.
   */
  @VisibleForTesting
  List<SocketAddress> shuffleAndLimit(
      List<? extends SocketAddress> addresses, boolean spreadAcrossAddresses) {
    List<SocketAddress> shuffled = new ArrayList<>(addresses);
    if (shuffled.size() > 1 && spreadAcrossAddresses) {
      Collections.shuffle(shuffled, random);
    }
    int cap =
        Math.max(
            1,
            context
                .getConfig()
                .getDefaultProfile()
                .getInt(DefaultDriverOption.CONNECTION_MAX_CANDIDATE_ADDRESSES));
    if (shuffled.size() > cap) {
      LOG.debug(
          "[{}] Resolved {} addresses, will try at most {}"
              + " (advanced.connection.max-candidate-addresses)",
          logPrefix,
          shuffled.size(),
          cap);
      return shuffled.subList(0, cap);
    }
    return shuffled;
  }

  /**
   * Iterates through the candidate addresses produced by {@link #resolveCandidates}. Tries each one
   * in sequence; when an address fails, the next candidate is tried, and only when all candidates
   * are exhausted is the overall {@code resultFuture} failed.
   *
   * <p>Two failures are <b>node-wide</b> -- they doom every remaining address rather than only the
   * one that was tried: an {@link UnsupportedProtocolVersionException} and an {@link
   * UnsupportedEventTypeException}. Both are properties of the server rather than of the record
   * that reached it, so both are gated on the same thing, {@code sameServerAtEveryAddress} (see
   * {@link #sameServerAtEveryAddress}, and {@link #isNodeWideFailure} for why that is the right
   * question for each).
   *
   * <p>What that covers is narrower than it looks, because {@code isNegotiating} is true only while
   * {@link #protocolVersion} is still unset -- i.e. on the session's first connection, which is
   * always to a contact point, and a contact point is never {@linkplain #isIdentified(Node)
   * identified}. So a version rejection reached by <i>negotiation</i> stops the loop only when the
   * contact point's endpoint reports its addresses interchangeable, as an SNI or client-routes
   * proxy does. A plain multi-record name still walks the downgrade ladder from the top on every
   * candidate -- each gets a fresh {@code attemptedVersions} list, so N records cost N ladders,
   * with N bounded by {@link #shuffleAndLimit} -- and that is the intent, because those records may
   * be different servers.
   *
   * <p>Every other failure -- authentication included -- advances to the next candidate: the
   * addresses a name expands to may well belong to different nodes, so a rejection by the first of
   * them says nothing about the rest. That also preserves the behaviour this PR would otherwise
   * have removed: with {@code advanced.resolve-contact-points = true} each resolved address used to
   * be a separate {@code Node}, and {@code ControlConnection} advances to the next node in its
   * query plan on any error, including these.
   *
   * <p>Authentication in particular has to advance, for a reason only visible in the order of the
   * handshake: {@link ProtocolInitHandler} runs {@code STARTUP -> AUTH_RESPONSE ->
   * GET_CLUSTER_NAME}, so authentication completes <i>before</i> the cluster-name check. A stale
   * DNS record pointing at a foreign cluster that wants different credentials therefore fails at
   * AUTH, and treating that as terminal would write off the whole hostname -- making the
   * cluster-name mismatch that would have advanced to the next address unreachable, in exactly the
   * multi-record case this loop exists for. What bounds the cost of genuinely wrong credentials is
   * the candidate cap ({@link #shuffleAndLimit}): one attempt pays at most {@code
   * advanced.connection.max-candidate-addresses} rejected logins, with the earlier failures
   * attached as suppressed exceptions.
   *
   * <p><b>Timeout note:</b> addresses are tried serially, so the worst-case time before failure is
   * N times a full attempt, and an attempt is a connect <i>plus</i> the init handshake. Each of the
   * handshake's steps arms its own {@code advanced.connection.init-query-timeout} when it is sent,
   * so they accumulate instead of sharing one deadline: a single address that accepts the
   * connection and then stalls costs {@code connect-timeout} plus several times {@code
   * init-query-timeout} before the loop moves on. This is an intentional tradeoff: failing
   * immediately on the first unreachable IP would prevent fallback to healthy ones. The candidate
   * cap ({@link #shuffleAndLimit}) is what bounds N.
   *
   * <p>When every candidate fails, one of their errors is propagated -- see {@link
   * #surfacedFailure} for which, and why it is not simply the last -- with every other candidate's
   * failure attached to it as a {@linkplain Throwable#addSuppressed(Throwable) suppressed}
   * exception, so no cause is lost.
   *
   * <p>What that does <b>not</b> give is which address produced which failure. Every one of these
   * exceptions is built from the endpoint, and a pinned copy is required to render identically to
   * the unpinned original ({@link PinnableEndPoint}), so a three-record name yields three messages
   * that all name the same hostname. The DEBUG line above is where the pairing lives; making the
   * exceptions themselves carry it would mean either relaxing that contract or wrapping causes in a
   * driver-owned type, which would in turn break the {@code instanceof} tests the callers do on
   * them (see {@link #surfacedFailure}).
   */
  private void tryNextCandidate(
      Bootstrap baseBootstrap,
      EventLoop eventLoop,
      EndPoint endPoint,
      NodeShardingInfo shardingInfo,
      Integer shardId,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      ProtocolVersion currentVersion,
      boolean isNegotiating,
      boolean sameServerAtEveryAddress,
      CompletableFuture<DriverChannel> resultFuture,
      List<SocketAddress> candidates,
      int index,
      List<Throwable> priorErrors) {

    // Invariant: this method always (eventually) completes resultFuture. It is invoked from
    // CompletionStage and Netty callbacks that swallow throwables, so a synchronous throw -- a
    // custom PinnableEndPoint.pinTo() for instance -- would otherwise leave the connect attempt
    // hanging forever. Double completion is harmless: completeExceptionally() on an already
    // completed future is a no-op.
    try {
      SocketAddress candidate = candidates.get(index);
      // Everything downstream of here -- the channel, its pipeline (SSL engine, authenticator) and
      // the DriverChannel handed to the caller -- sees an endpoint bound to this one address
      // instead of the multi-address original. See PinnableEndPoint for why that matters.
      EndPoint pinnedEndPoint = pin(endPoint, candidate);
      CandidateFuture perAddressFuture = new CandidateFuture();
      // Fresh per candidate address: connectToAddress()'s downgrade retries stay on this one
      // address, so the final UnsupportedProtocolVersionException (if negotiation is what dooms
      // this candidate) only reports versions actually tried against it, not earlier candidates'.
      List<ProtocolVersion> attemptedVersions = new CopyOnWriteArrayList<>();
      connectToAddress(
          baseBootstrap,
          eventLoop,
          pinnedEndPoint,
          shardingInfo,
          shardId,
          options,
          nodeMetricUpdater,
          currentVersion,
          isNegotiating,
          attemptedVersions,
          perAddressFuture,
          candidate);

      perAddressFuture.whenComplete(
          (channel, error) -> {
            try {
              boolean nodeWide =
                  error != null && isNodeWideFailure(error, sameServerAtEveryAddress);
              if (error == null) {
                if (!resultFuture.complete(channel)) {
                  // Same guard as completeCandidate and abandonCandidate: resultFuture is handed to
                  // callers as a CompletionStage and every path that can complete it early does so
                  // exceptionally (the blanket catches in resolveCandidates and below), so losing
                  // this race is possible -- and would otherwise leak a live socket and its
                  // pipeline for the life of the JVM, since nobody else holds this channel.
                  channel.forceClose();
                }
              } else if (!nodeWide && index + 1 < candidates.size()) {
                LOG.debug(
                    "[{}] Failed to connect to {} ({}), trying next address",
                    logPrefix,
                    candidate,
                    error.getMessage());
                priorErrors.add(error);
                tryNextCandidate(
                    baseBootstrap,
                    eventLoop,
                    // Deliberately the original, not the pinned copy: the next candidate must be
                    // pinned from the unpinned endpoint.
                    endPoint,
                    shardingInfo,
                    shardId,
                    options,
                    nodeMetricUpdater,
                    currentVersion,
                    isNegotiating,
                    sameServerAtEveryAddress,
                    resultFuture,
                    candidates,
                    index + 1,
                    priorErrors);
              } else {
                if (index + 1 < candidates.size()) {
                  // Only reachable for a node-wide failure (see the javadoc).
                  LOG.debug(
                      "[{}] Not trying the remaining addresses of {}: this failure is a property of"
                          + " the node, not of the address ({})",
                      logPrefix,
                      endPoint,
                      error.getMessage());
                }
                // Surface one failure, carrying the others as suppressed exceptions so they are
                // not lost (they were only logged at DEBUG above). Deduplicated by identity:
                // nothing stops two candidates from failing with the same Throwable instance, and
                // this mutates an object we do not own -- attaching it twice would show the same
                // cause twice, and would keep growing a shared instance's suppressed list on every
                // connect.
                List<Throwable> allErrors = new ArrayList<>(priorErrors);
                allErrors.add(error);
                Throwable surfaced = surfacedFailure(allErrors, nodeWide);
                Set<Throwable> attached = Collections.newSetFromMap(new IdentityHashMap<>());
                attached.add(surfaced);
                for (Throwable candidateError : allErrors) {
                  if (attached.add(candidateError)) {
                    surfaced.addSuppressed(candidateError);
                  }
                }
                // Note: might be completed already if the failure happened in initializer()
                resultFuture.completeExceptionally(surfaced);
              }
            } catch (Throwable t) {
              resultFuture.completeExceptionally(t);
            }
          });
    } catch (Throwable t) {
      resultFuture.completeExceptionally(t);
    }
  }

  /**
   * Whether {@code error} dooms every remaining address of the endpoint, making it pointless for
   * {@link #tryNextCandidate} to try them. See its javadoc for the reasoning behind each case.
   *
   * <p>A {@link ClusterNameMismatchException} is deliberately absent, for an identified node as
   * much as for a contact point. It says that the address just tried fronts a different cluster,
   * which is a property of that record rather than of the node -- a stale DNS entry is exactly what
   * it looks like -- so advancing to the next address is the whole point. {@link #surfacedFailure}
   * treats it with the same caution on the way out.
   *
   * <p>An {@link AuthenticationException} is deliberately absent too, even for an identified node:
   * see {@link #tryNextCandidate} on why authentication must advance, and {@link #shuffleAndLimit}
   * for the cap that bounds what wrong credentials can cost.
   *
   * <p>Both cases here are properties of the <b>server</b> rather than of the record that reached
   * it: which protocol versions it speaks, and which event types it knows. That is why they can be
   * node-wide at all -- where the same server answers everywhere, replaying either against the
   * remaining addresses can only fail the same way, and each replay costs a full TCP connect plus
   * the STARTUP/AUTH/cluster-name handshake and the connect hook's round trip. Stopping at the
   * first restores what a rejection cost before an endpoint expanded to several addresses, which
   * was one failed connect per contact point.
   *
   * <p>Where it is <b>not</b> the same server, that saving is given up on purpose, and the case it
   * is given up for is the common one: {@link UnsupportedEventTypeException} is raised only for
   * {@code CLIENT_ROUTES_CHANGE} (see {@link #translateRegisterFailure}), which only the control
   * connection registers -- so on the initial connect the endpoint is always an unidentified
   * contact point and this always advances. A multi-record contact point mid-rolling-upgrade is
   * exactly the deployment where advancing finds the address that works, so paying one connect per
   * record to find it is the trade. What must not be given up with it is the diagnosis, which is
   * why {@link #surfacedFailure} gives this type a rung of its own rather than letting whichever
   * address failed last speak for the endpoint.
   *
   * <p>And it is why both are gated on the same question -- whether the same server really does
   * answer at every address (see {@link #connect}, which derives it). Asking only whether the
   * <i>node</i> is identified would be wrong in both directions. Too narrow: an unidentified
   * contact point behind an SNI or client-routes proxy has every address routed to one node, so a
   * rejection settles all of them and replaying the downgrade ladder against each proxy IP is
   * waste. Too wide: an unidentified contact point that is a plain multi-record name may front
   * distinct servers, and during a rolling upgrade they genuinely differ -- writing the name off
   * because the first address answered was the one not yet upgraded would skip the addresses that
   * would have worked.
   *
   * <p>What that leaves unrescued is the mirror of the second case, and is accepted: a
   * heterogeneous <i>identified</i> node, whose own addresses disagree about protocol versions or
   * event types. Every address of an identified node is that node, so the driver has nowhere better
   * to look. Reachable only by pointing a node at a name that covers several hosts -- see {@link
   * #sameServerAtEveryAddress} -- and unchanged by this loop either way: before it, {@code
   * resolve()} handed over the resolver's first record alone, so the same bad record produced the
   * same verdict from a single-address connect. What the loop does not do is rescue it, and {@link
   * #surfacedFailure} keeps it that way on purpose: a node-wide failure outranks the unanimity rule
   * there, so one such record still forces the node down rather than being demoted to whatever
   * transport error a sibling address produced.
   */
  private static boolean isNodeWideFailure(Throwable error, boolean sameServerAtEveryAddress) {
    return sameServerAtEveryAddress
        && (error instanceof UnsupportedProtocolVersionException
            || error instanceof UnsupportedEventTypeException);
  }

  /**
   * Which of the candidates' failures to propagate once they are all exhausted, the rest being
   * attached to it as suppressed exceptions.
   *
   * <p>Not simply the last one. Callers branch on the <b>type</b> of what they receive -- {@link
   * com.datastax.oss.driver.internal.core.pool.ChannelPool#handleError} treats a cluster-name
   * mismatch and a protocol-version rejection as fatal, an invalid keyspace as a keyspace error and
   * an authentication failure as warn-and-retry; {@code ControlConnection} logs authentication
   * failures differently from transport ones -- and with a multi-record name the address that
   * happens to be tried last is arbitrary. Letting it win would report a firewalled IP's connect
   * timeout for what is really a rejected password, and take the reconnect path where the caller
   * asked for the fatal one.
   *
   * <p>So a failure the callers classify is preferred over one they do not, in the order they test
   * for it, and the last <i>non-fatal</i> failure is only used when no candidate produced a
   * classified one. Every failure is still attached, so nothing is lost either way. One rung is
   * there for a reader rather than a caller: an unsupported event type is nothing any caller
   * branches on, but it is the only failure here that tells an operator what to change.
   *
   * <p>The two <b>fatal</b> types are the exception to that: they are only preferred when every
   * candidate failed that way, or when the last one is the node-wide failure that stopped the loop.
   * See the comments in the body.
   */
  private static Throwable surfacedFailure(List<Throwable> errors, boolean lastIsNodeWide) {
    Throwable lastError = errors.get(errors.size() - 1);
    // A node-wide failure is what ended the loop, and it is a verdict about the node rather than
    // about the one address it was observed on (see tryNextCandidate). It therefore outranks
    // everything below, including the unanimity rule -- which would otherwise demote it to whatever
    // transport failure an earlier address happened to produce, turning the forced-down node that a
    // single-address connect has always produced into a reconnect.
    if (lastIsNodeWide) {
      return lastError;
    }
    // An irreversible verdict needs evidence from every address. handleError turns these two into
    // TopologyEvent.forceDown, and nothing in the driver ever reverses one -- no component fires
    // FORCE_UP, and a SUGGEST_UP is explicitly refused for a FORCED_DOWN node -- so the node is out
    // for the rest of the session. Meanwhile tryNextCandidate() classifies a cluster-name mismatch
    // as a property of the address and advances past it, which is the point: it means this record
    // is stale, not that this node belongs to another cluster. Promoting one such record over the
    // other candidates' transport failures would write a healthy node off on the strength of the
    // one address that was never going to work. Requiring unanimity leaves a single-address
    // endpoint exactly as it was before this loop existed -- one candidate is unanimous by
    // definition -- and a mixed pass simply reconnects, forcing down on the first pass that is.
    boolean everyCandidateFatal = true;
    for (Throwable error : errors) {
      if (!isFatalToCallers(error)) {
        everyCandidateFatal = false;
        break;
      }
    }
    if (everyCandidateFatal) {
      return errors.get(0);
    }
    // An invalid keyspace, unlike those two, is a property of the cluster's schema rather than of
    // the address, so one address answering settles it and no unanimity is required. It outranks an
    // authentication failure because it is the rung a caller acts on -- handleError routes it to
    // onKeyspaceError, which is how PoolManager fails session init fast instead of reconnecting for
    // a keyspace that will never appear -- and because reaching the keyspace step at all proves the
    // credentials were accepted on that address.
    for (Throwable error : errors) {
      if (error instanceof InvalidKeyspaceException) {
        return error;
      }
    }
    for (Throwable error : errors) {
      if (error instanceof AuthenticationException) {
        return error;
      }
    }
    // An event type the server does not know is a verdict about the deployment, and the message
    // says what to do about it ("requires ScyllaDB Enterprise >= ..."), so it outranks an
    // unclassified transport failure that happened to come later. Without a rung of its own it
    // would fall through to the last non-fatal failure below and ClientRoutesTopologyMonitor#init
    // would report a bare connect timeout, with the actionable message reachable only through
    // getSuppressed().
    //
    // Below the authentication rung, not above it: if the credentials were rejected, whether the
    // server also speaks CLIENT_ROUTES_CHANGE was never established. And no unanimity is required,
    // for the same reason as the invalid keyspace above -- one server answering settles what the
    // software supports. It cannot force a node down (see #isFatalToCallers), so promoting it
    // costs nothing irreversible.
    for (Throwable error : errors) {
      if (error instanceof UnsupportedEventTypeException) {
        return error;
      }
    }
    // The last failure -- but not a fatal one. The address tried last is arbitrary, so promoting a
    // fatal failure here would force the node down on the strength of the single address that
    // produced it, which is exactly what the unanimity rule above refuses to do.
    for (int i = errors.size() - 1; i >= 0; i--) {
      Throwable error = errors.get(i);
      if (!isFatalToCallers(error)) {
        return error;
      }
    }
    // Unreachable: a list of nothing but fatal failures returned at the unanimity check above.
    return lastError;
  }

  /**
   * Whether the callers treat {@code error} as fatal, i.e. as grounds to write the node off: {@code
   * ChannelPool#handleError} turns these two, and only these two, into {@code
   * TopologyEvent.forceDown}.
   */
  private static boolean isFatalToCallers(Throwable error) {
    return error instanceof ClusterNameMismatchException
        || error instanceof UnsupportedProtocolVersionException;
  }

  /**
   * Whether {@code error} and every failure attached to it are authentication failures, i.e.
   * whether "authentication" is the whole story for the endpoint that produced it.
   *
   * <p>The test callers must use in place of a bare {@code instanceof AuthenticationException}, and
   * it lives here because this class is what makes the bare test wrong: one failure no longer means
   * one address. A connect expands an endpoint to every address it resolves to and reports a single
   * failure for the endpoint, with the others attached as {@linkplain Throwable#getSuppressed()
   * suppressed} exceptions -- and {@link #surfacedFailure} deliberately promotes an authentication
   * failure over transport ones, so an endpoint whose records failed {@code [refused, refused,
   * auth]} surfaces the auth error. Counting that as {@code errors.connection.auth} alone, and
   * telling the operator their credentials are wrong, hides that two thirds of the deployment is
   * unreachable.
   *
   * @see com.datastax.oss.driver.internal.core.pool.ChannelPool
   * @see com.datastax.oss.driver.internal.core.control.ControlConnection
   */
  public static boolean isAuthOnly(Throwable error) {
    return isAuthOnly(error, ignored -> false);
  }

  /**
   * {@link #isAuthOnly(Throwable)}, with some of the attached failures set aside as no evidence
   * either way.
   *
   * <p>For the caller that has a class of failure which says nothing about credentials, and must
   * not let it decide the question. {@code ControlConnection} passes its own exclusions: a node the
   * connection was not allowed to use was never asked for a password, so a contact point whose
   * addresses went {@code [excluded, auth]} is an authentication failure and nothing else. Testing
   * it with the plain method would find the exclusion among the suppressed and answer {@code
   * false}, which is how one such contact point comes to veto the verdict for a whole round.
   *
   * <p>Setting everything aside is not an authentication failure. At least one real one has to
   * remain, or an endpoint that was only ever refused would report as an auth failure -- the
   * opposite mistake, and the one the caller's own skip is there to prevent.
   */
  public static boolean isAuthOnly(Throwable error, Predicate<Throwable> ignore) {
    boolean anyAuth = false;
    if (error instanceof AuthenticationException) {
      anyAuth = true;
    } else if (!ignore.test(error)) {
      return false;
    }
    for (Throwable suppressed : error.getSuppressed()) {
      if (suppressed instanceof AuthenticationException) {
        anyAuth = true;
      } else if (!ignore.test(suppressed)) {
        return false;
      }
    }
    return anyAuth;
  }

  /**
   * Whether an authentication failure appears anywhere in what {@code error} reports -- as the
   * failure itself or as one of the {@linkplain Throwable#getSuppressed() suppressed} ones.
   *
   * <p>The counterpart to {@link #isAuthOnly}, for the caller that needs to know a login was
   * rejected at all rather than whether that is the whole story. {@link #surfacedFailure} promotes
   * an invalid keyspace, and a node-wide failure, over an authentication failure, so for an
   * endpoint that expands to several addresses the auth error is routinely not the one that comes
   * out -- and a caller testing the type of what it received would never see it.
   */
  public static boolean mentionsAuthentication(Throwable error) {
    if (error instanceof AuthenticationException) {
      return true;
    }
    for (Throwable suppressed : error.getSuppressed()) {
      if (suppressed instanceof AuthenticationException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Performs a Netty bootstrap connect to a single, already-resolved address. Handles
   * protocol-version negotiation (downgrade retries) internally, staying on the same address. Uses
   * {@code perAddressFuture} so {@link #tryNextCandidate} can distinguish a per-address TCP failure
   * (try the next IP) from a successful protocol handshake.
   */
  private void connectToAddress(
      Bootstrap baseBootstrap,
      EventLoop eventLoop,
      EndPoint endPoint,
      NodeShardingInfo shardingInfo,
      Integer shardId,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      ProtocolVersion currentVersion,
      boolean isNegotiating,
      List<ProtocolVersion> attemptedVersions,
      CandidateFuture perAddressFuture,
      SocketAddress resolvedAddress) {

    if (shardId == null || shardingInfo == null) {
      if (shardId != null) {
        LOG.debug(
            "Requested connection to shard {} but shardingInfo is currently missing for Node at endpoint {}. Falling back to arbitrary local port.",
            shardId,
            endPoint);
      }
      bootstrapAndConnect(
          baseBootstrap,
          eventLoop,
          endPoint,
          shardingInfo,
          shardId,
          options,
          nodeMetricUpdater,
          currentVersion,
          isNegotiating,
          attemptedVersions,
          perAddressFuture,
          resolvedAddress,
          null);
      return;
    }

    // Picking a shard-aware local port means probing ports with ServerSocket.bind(): a blocking
    // syscall per probe, and when the range is contended a scan across the whole of
    // [port-low, port-high] -- twice, if the first pass wraps (see PortAllocator).
    //
    // That must not run on `eventLoop`. It is one of the I/O loops, shared with every established
    // channel registered on it, and advanced shard awareness is enabled by default, so this is the
    // ordinary path against Scylla rather than an edge case. The loop is reached by two separate
    // routes -- resolveCandidates() completes its future there, and the downgrade retry below
    // re-enters this method from inside a Netty listener -- so the guard belongs here, at the
    // blocking call, rather than at either caller.
    //
    // The admin group, because that is where this scan already ran: before resolution moved into
    // this class, connect() did it inline on its calling thread, which is the adminExecutor of the
    // ChannelPool or ControlConnection driving the connect. It carries no request traffic.
    //
    // Two things do differ from that, both accepted rather than unnoticed. next() takes a thread
    // from the group (advanced.netty.admin-group.size, 2 by default) instead of the caller's own,
    // so a pool's scan can now land on the thread the control connection runs on, where before it
    // could only stall the caller's own queue. And the candidate loop reaches this once per address
    // tried rather than once per connect(), so an endpoint whose earlier addresses fail pays it
    // more than once. Both stay bounded -- the loop is sequential within a connect, and
    // max-candidate-addresses caps the addresses at 5 -- and neither puts blocking work on this
    // group that was not already there. Moving the scan off the control plane altogether is the
    // real fix and is tracked in the deferred ledger.
    try {
      context
          .getNettyOptions()
          .adminEventExecutorGroup()
          .next()
          .execute(
              () -> {
                // The same invariant as below, restated because it is a fresh entry point: this
                // body runs as an executor task, and Netty only logs a task's throwables.
                try {
                  int localPort =
                      PortAllocator.getNextAvailablePort(
                          shardingInfo.getShardsCount(), shardId, context);
                  if (localPort == -1) {
                    LOG.warn(
                        "Could not find free port for shard {} at {}. Falling back to arbitrary local port.",
                        shardId,
                        endPoint);
                  }
                  bootstrapAndConnect(
                      baseBootstrap,
                      eventLoop,
                      endPoint,
                      shardingInfo,
                      shardId,
                      options,
                      nodeMetricUpdater,
                      currentVersion,
                      isNegotiating,
                      attemptedVersions,
                      perAddressFuture,
                      resolvedAddress,
                      localPort == -1 ? null : localPort);
                } catch (Throwable t) {
                  perAddressFuture.completeExceptionally(t);
                }
              });
    } catch (Throwable t) {
      // RejectedExecutionException, if the group is shutting down. Completing the future here is
      // what keeps a connect from hanging on it.
      perAddressFuture.completeExceptionally(t);
    }
  }

  /**
   * The rest of {@link #connectToAddress}, once the local port (if any) has been settled.
   *
   * @param localPort the local port to bind to, or {@code null} to let the OS pick one.
   */
  private void bootstrapAndConnect(
      Bootstrap baseBootstrap,
      EventLoop eventLoop,
      EndPoint endPoint,
      NodeShardingInfo shardingInfo,
      Integer shardId,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      ProtocolVersion currentVersion,
      boolean isNegotiating,
      List<ProtocolVersion> attemptedVersions,
      CandidateFuture perAddressFuture,
      SocketAddress resolvedAddress,
      Integer localPort) {

    // Invariant, as in tryNextCandidate(): every path completes perAddressFuture. The synchronous
    // section can throw from Bootstrap validation; the connect listener runs inside a Netty
    // callback that swallows throwables and contains the downgrade recursion, the version-registry
    // lookup and the config overrides, any of which throwing would otherwise hang the attempt.
    try {
      // Captured here, beside the pipeline that is about to be built, because ProtocolInitHandler
      // snapshots this same option in its constructor (its `timeoutMillis`) and every init step
      // then runs on that one value. Reading it again later -- REGISTER is sent after init now, so
      // it would be a second read -- lets a config reload landing inside this connect apply a new
      // value to a connection that already exists, which is precisely the scope reference.conf
      // rules out: "the new value will be used for connections created after the change". This is
      // not quite the same instant as ProtocolInitHandler's own read -- that one happens in
      // initChannel, once the connect below registers the channel -- so a reload landing in
      // between would still split the two. What closes is the window that matters: the one
      // spanning the whole handshake, from STARTUP to the REGISTER that now follows it.
      //
      // It also removes a way to hang the attempt. The two request classes disagree about a
      // non-positive value -- AdminRequestHandler#onWriteComplete arms no timer at all, while the
      // ChannelHandlerRequest the init steps use arms one unconditionally -- so a reload to zero
      // mid-handshake used to leave STARTUP bounded by the old value and REGISTER bounded by
      // nothing, on a connection with no hook backstop behind it.
      Duration initQueryTimeout =
          context
              .getConfig()
              .getDefaultProfile()
              .getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT);

      // clone(eventLoop) so each attempt gets its own handler while sharing the options (including
      // anything afterBootstrapInitialized() set), and is registered on the event loop the
      // connect() picked -- the same one resolution ran on, so the group's chooser advances exactly
      // once per logical connect (see connect()).
      //
      // disableResolver() because resolveCandidates() has already done the one resolution pass this
      // connect gets, and `resolvedAddress` is one of its results. Bootstrap.clone() otherwise
      // carries the resolver over and Netty resolves again -- through resolve(), *singular*. That
      // is inert for the default resolver, which short-circuits on isResolved(), but a resolver
      // that reports resolved addresses as unresolved in order to redirect them -- which
      // resolveCandidates() deliberately supports -- would remap every candidate onto its first
      // answer: the remaining candidates would never actually be tried, and the endpoint pinned
      // onto the channel would name an address the channel is not connected to (which is what the
      // SSL engine's peer host and DefaultTopologyMonitor#savePort are derived from). Every other
      // exit from resolveCandidates() yields an address Netty would itself have passed through
      // untouched -- no group, !isSupported, or isResolved -- so nothing else changes.
      Bootstrap bootstrap =
          baseBootstrap
              .clone(eventLoop)
              .disableResolver()
              .handler(
                  initializer(
                      endPoint, currentVersion, options, nodeMetricUpdater, perAddressFuture));

      ChannelFuture connectFuture =
          (localPort == null)
              ? bootstrap.connect(resolvedAddress)
              : bootstrap.connect(resolvedAddress, new InetSocketAddress(localPort));

      connectFuture.addListener(
          cf -> {
            try {
              if (connectFuture.isSuccess()) {
                Channel channel = connectFuture.channel();
                DriverChannel driverChannel =
                    new DriverChannel(
                        endPoint, channel, context.getWriteCoalescer(), currentVersion);
                finishCandidate(
                    driverChannel,
                    options,
                    initQueryTimeout,
                    perAddressFuture,
                    () -> latchNegotiatedState(driverChannel, currentVersion, isNegotiating));
              } else {
                Throwable error = connectFuture.cause();
                if (error instanceof UnsupportedProtocolVersionException && isNegotiating) {
                  attemptedVersions.add(currentVersion);
                  Optional<ProtocolVersion> downgraded =
                      context.getProtocolVersionRegistry().downgrade(currentVersion);
                  if (downgraded.isPresent()) {
                    LOG.debug(
                        "[{}] Failed to connect with protocol {}, retrying with {}",
                        logPrefix,
                        currentVersion,
                        downgraded.get());
                    // Stay on the same address for protocol-version downgrade retries.
                    connectToAddress(
                        baseBootstrap,
                        eventLoop,
                        endPoint,
                        shardingInfo,
                        shardId,
                        options,
                        nodeMetricUpdater,
                        downgraded.get(),
                        true,
                        attemptedVersions,
                        perAddressFuture,
                        resolvedAddress);
                  } else {
                    perAddressFuture.completeExceptionally(
                        UnsupportedProtocolVersionException.forNegotiation(
                            endPoint, attemptedVersions));
                  }
                } else {
                  // Note: might be completed already if the failure happened in initializer(), this
                  // is fine
                  perAddressFuture.completeExceptionally(error);
                }
              }
            } catch (Throwable t) {
              // Close the channel we opened before giving up on it. Nothing else holds it once this
              // listener returns -- the DriverChannel wrapper is out of scope, and no candidate was
              // completed with it -- so the socket and its pipeline would stay open for the life of
              // the JVM. Only when the future is ours to fail, though: a candidate that completed
              // successfully is the caller's channel, not ours to close.
              if ((perAddressFuture.completeExceptionally(t)
                      || perAddressFuture.isCompletedExceptionally())
                  && connectFuture.isSuccess()) {
                connectFuture.channel().close();
              }
            }
          });
    } catch (Throwable t) {
      perAddressFuture.completeExceptionally(t);
    }
  }

  /**
   * Remembers what the first accepted connection negotiated, so later connections skip the
   * negotiation: the protocol version, the cluster name to check others against, and the server's
   * product type (which for Cloud also lowers the default consistency level).
   *
   * <p>Run only once a candidate has been <b>accepted</b>, not as soon as its transport connect and
   * init handshake succeed. Init is no longer the last word on a candidate: the connect hook can
   * reject it (the control connection's identity read does, for a node with no {@code host_id}),
   * and REGISTER, which used to be the final init step, now runs after that hook. A candidate the
   * driver is about to throw away must not leave its cluster name latched here -- a stale DNS
   * record pointing at a foreign cluster would otherwise make every subsequent connection fail its
   * cluster-name check, which {@code ChannelPool} turns into an irreversible forced-down node.
   *
   * <p>What enforces that is {@link CandidateFuture#settle()}: only the candidate that wins it
   * reaches {@link #completeCandidate}, and only {@link #completeCandidate} runs this. Note that
   * "accepted" is per connect attempt, not per factory -- two concurrent negotiating connects each
   * accept a candidate and each latch, so {@code protocolVersion} can legitimately be written more
   * than once (it has no first-write-wins guard, unlike the two below). Both writers negotiated
   * against the same cluster, so they agree except while it is being upgraded.
   */
  private void latchNegotiatedState(
      DriverChannel driverChannel, ProtocolVersion currentVersion, boolean isNegotiating) {
    if (isNegotiating) {
      this.protocolVersion = currentVersion;
    }
    if (this.clusterName == null) {
      this.clusterName = driverChannel.getClusterName();
    }
    Map<String, List<String>> supportedOptions = driverChannel.getOptions();
    if (this.productType == null && supportedOptions != null) {
      List<String> productTypes = supportedOptions.get("PRODUCT_TYPE");
      String productType =
          productTypes != null && !productTypes.isEmpty()
              ? productTypes.get(0)
              : UNKNOWN_PRODUCT_TYPE;
      this.productType = productType;
      DriverConfig driverConfig = context.getConfig();
      if (driverConfig instanceof TypesafeDriverConfig
          && productType.equals(DATASTAX_CLOUD_PRODUCT_TYPE)) {
        ((TypesafeDriverConfig) driverConfig)
            .overrideDefaults(
                ImmutableMap.of(
                    DefaultDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.LOCAL_QUORUM.name()));
      }
    }
  }

  /**
   * Disarms the connect-hook backstop, if one was armed.
   *
   * <p>{@code null} when the timeout was disabled, which is why the null check lives here rather
   * than at each of the three call sites.
   *
   * <p>The return value is deliberately ignored. Netty's {@code Timeout#cancel()} answers false
   * both for a task already cancelled and for one currently expiring, so it distinguishes nothing a
   * caller here could act on: the timer task and this thread both funnel into {@link
   * #abandonCandidate}, whose {@code settle()} latch decides which of them owns the failure.
   * Cancelling is an optimization -- it keeps a wheel slot from holding a dead reference for the
   * rest of the timeout -- not a synchronization point.
   */
  private static void cancelQuietly(Timeout hookTimeout) {
    if (hookTimeout != null) {
      hookTimeout.cancel();
    }
  }

  /**
   * The tail of a candidate attempt, once transport connect and protocol initialization have both
   * succeeded: runs the caller's {@link ConnectHook} (if any), then registers for protocol events
   * (if requested), and only then completes the candidate's future.
   *
   * <p>Both steps happen while this attempt still holds the endpoint's remaining addresses, so a
   * failure in either is settled inside the loop: the channel is force-closed on the spot and
   * {@link #tryNextCandidate} decides what to do with the addresses that are left. Usually that
   * means advancing to the next address; the exception is a failure {@link #isNodeWideFailure}
   * classifies, which stops the loop because it describes the server rather than the address it was
   * seen on. REGISTER can produce one -- see {@link #registerForEvents}.
   *
   * <p>REGISTER used to be the last protocol-init step; it moved behind the hook so that a channel
   * the hook is about to reject never registers for events. The window in which a live channel is
   * not yet registered grows by the hook's round trip -- the same order of cost as the init step it
   * follows.
   */
  private void finishCandidate(
      DriverChannel driverChannel,
      DriverChannelOptions options,
      Duration initQueryTimeout,
      CandidateFuture perAddressFuture,
      Runnable onAccepted) {
    if (options.connectHook == null) {
      registerForEvents(driverChannel, options, initQueryTimeout, perAddressFuture, onAccepted);
      return;
    }
    // The hook's contract says its stage eventually completes, but only the driver can make that
    // true: a wedged hook (a topology monitor whose own timeout is broken, say) would otherwise
    // hang the whole connect attempt, and with it control-connection init or a reconnect.
    //
    // Armed before the hook is called, not after it returns. A hook that blocks inside onConnect
    // never returns, so arming afterwards would not bound it on any thread -- the arming statement
    // is simply never reached, which is a different failure from the one the thread choice below
    // addresses and is not fixed by it. The cost of arming first is a
    // timeout scheduled and cancelled again for every candidate whose hook answers promptly, which
    // is a wheel insertion and a flag.
    //
    // What the timer can and cannot do for a blocking hook is worth being exact about: it releases
    // the *connect*, not the loop. abandonCandidate completes perAddressFuture from the timer
    // thread, so the Reconnection stops waiting and a later attempt can be made; the hook goes on
    // holding the channel's event loop until it returns, and every other channel registered there
    // stays stalled meanwhile. Bounding the connect is what is on offer, and it is worth having.
    //
    // Unless the timeout is zero or negative, which every other consumer of a driver timeout option
    // reads as "no timeout" (see AdminRequestHandler#onWriteComplete). Scheduling it anyway would
    // fire on the next event-loop turn, before any round trip can complete, and abandon every
    // candidate of every contact point -- so an operator who disabled the control-connection
    // timeout
    // would find that the session cannot initialize at all.
    //
    // On the driver's timer, and neither of the two threads this connect is already using.
    //
    // Not this channel's event loop. The hook runs there -- this method is reached from a
    // channel-promise listener, which Netty notifies on it -- so a hook that wedges that loop would
    // keep it from ever dequeuing a task armed on it. Blocking is exactly what
    // TopologyMonitor#getChannelNodeInfo's contract has to ask implementations not to do, because
    // nothing enforces it, and it takes two shapes that need different answers: a hook that returns
    // a stage and separately stalls the loop is caught by arming off that loop, and a hook that
    // blocks inside onConnect is caught only by arming before the call. Neither is caught by the
    // hook's own machinery. A hook that simply never completes its stage is caught wherever the
    // timer lives.
    //
    // Not the admin group either, for a weaker version of the same reason: #connectToAddress
    // dispatches the shard-aware port scan there, and that scan blocks -- a bind() probe per port
    // across advanced.shard-awareness.port-{low,high} -- once per candidate address. The group is
    // two threads by default and one of them is the control connection's own executor, so a
    // backstop armed on it can be sitting behind the very kind of work it exists to bound. Delay
    // rather than deadlock, but there is no reason to accept even that.
    //
    // The timer is a thread of its own, and what it carries is only ever timeout callbacks -- it is
    // already where every request deadline in the driver lives (CqlRequestHandler,
    // CqlPrepareHandler, the graph and continuous-paging handlers, metrics expiry), so one task per
    // candidate is nothing beside its existing traffic, and none of that traffic blocks. And
    // abandonCandidate needs nothing from any particular thread: the settle() latch is an
    // AtomicBoolean and forceClose() is safe from any of them. Its granularity is
    // advanced.netty.timer.tick-duration, 100ms by default, against a timeout measured in seconds.
    Timeout hookTimeout;
    try {
      hookTimeout =
          (options.connectHookTimeout == null || options.connectHookTimeout.toNanos() <= 0)
              ? null
              : context
                  .getNettyOptions()
                  .getTimer()
                  .newTimeout(
                      timeout ->
                          abandonCandidate(
                              driverChannel,
                              perAddressFuture,
                              new ConnectionInitException(
                                  "Connect hook timed out after " + options.connectHookTimeout,
                                  null)),
                      options.connectHookTimeout.toNanos(),
                      TimeUnit.NANOSECONDS);
    } catch (Throwable t) {
      // A stopped timer rejects the task -- NettyOptions#onClose, or a custom implementation that
      // caps pending timeouts, which the driver's own does not. Fail the candidate rather than run
      // the hook with nothing bounding it: this method is called from a Netty listener that
      // swallows throwables, so the attempt would otherwise hang (see connectToAddress's
      // invariant).
      abandonCandidate(
          driverChannel,
          perAddressFuture,
          new ConnectionInitException("Could not schedule the connect hook timeout", t));
      return;
    }
    CompletionStage<Void> vetted;
    try {
      vetted = options.connectHook.onConnect(driverChannel);
    } catch (Throwable t) {
      // A synchronous throw is a rejection, like an exceptional stage. Blanket-caught: this runs
      // inside a Netty listener that swallows throwables, so a caller-supplied callback leaking
      // one would otherwise leave the attempt hanging forever.
      cancelQuietly(hookTimeout);
      abandonCandidate(
          driverChannel,
          perAddressFuture,
          new ConnectionInitException("Connect hook rejected the channel", t));
      return;
    }
    if (vetted == null) {
      cancelQuietly(hookTimeout);
      abandonCandidate(
          driverChannel,
          perAddressFuture,
          new ConnectionInitException("Connect hook returned a null stage", null));
      return;
    }
    vetted.whenComplete(
        (aVoid, error) -> {
          try {
            cancelQuietly(hookTimeout);
            if (error != null) {
              abandonCandidate(
                  driverChannel,
                  perAddressFuture,
                  new ConnectionInitException("Connect hook rejected the channel", error));
            } else {
              registerForEvents(
                  driverChannel, options, initQueryTimeout, perAddressFuture, onAccepted);
            }
          } catch (Throwable t) {
            // Blanket-caught, as everywhere else in this class: nobody consumes the stage this
            // callback returns, and the timeout that would have failed the candidate has just been
            // cancelled, so anything escaping here -- registerForEvents' config read, for instance
            // -- would leave perAddressFuture uncompleted forever.
            abandonCandidate(
                driverChannel,
                perAddressFuture,
                new ConnectionInitException(
                    "Unexpected error after the connect hook accepted the channel", t));
          }
        });
  }

  /**
   * Sends the REGISTER request when the options ask for protocol events, then completes the
   * candidate.
   *
   * <p>A registration failure is normally a per-candidate failure, and the attempt moves to the
   * next address. One is not: {@link #translateRegisterFailure} mints an {@link
   * UnsupportedEventTypeException} for a server that rejects the event type outright, and {@link
   * #isNodeWideFailure} treats that as node-wide wherever the same server answers at every address
   * -- which is every SNI or client-routes contact point, i.e. exactly the deployments that ask for
   * {@code CLIENT_ROUTES_CHANGE} in the first place. Replaying the rejection against each proxy IP
   * would only collect the same answer. That is a deliberate difference from the days when REGISTER
   * was a protocol-init step, where the failure took the whole endpoint down and there were no
   * other addresses to take with it.
   */
  private void registerForEvents(
      DriverChannel driverChannel,
      DriverChannelOptions options,
      Duration initQueryTimeout,
      CandidateFuture perAddressFuture,
      Runnable onAccepted) {
    if (options.eventTypes.isEmpty()) {
      completeCandidate(driverChannel, perAddressFuture, onAccepted);
      return;
    }
    // initQueryTimeout is the value bootstrapAndConnect captured for this attempt, not a fresh
    // read: this request runs after protocol initialization, so reading the option again here would
    // give a connection that already exists a value configured after it was created. See the
    // capture site for why that is both a documented-scope violation and, at zero, a way to leave
    // this request unbounded.
    // The owner's prefix plus the channel id, matching what ProtocolInitHandler builds for the
    // steps that used to send this request. This factory's own logPrefix is the session name,
    // which for a REGISTER timeout would name neither the connection pool nor the channel that
    // timed out -- and with an endpoint expanding to several addresses there can be more than one
    // of these in flight for the same node.
    // Same derivation as ProtocolInitHandler#channelActive: DriverChannel#toString delegates to
    // the Netty channel, whose toString is "[id: 0x..., L:... - R:...]", and the brackets come off.
    String channelId = driverChannel.toString();
    channelId = channelId.length() > 1 ? channelId.substring(1, channelId.length() - 1) : channelId;
    AdminRequestHandler.register(
            driverChannel,
            options.eventTypes,
            initQueryTimeout,
            options.ownerLogPrefix + "|" + channelId)
        .start()
        .whenComplete(
            (aVoid, error) -> {
              try {
                if (error != null) {
                  abandonCandidate(
                      driverChannel, perAddressFuture, translateRegisterFailure(error));
                } else {
                  completeCandidate(driverChannel, perAddressFuture, onAccepted);
                }
              } catch (Throwable t) {
                // Blanket-caught, as everywhere else in this class: nobody consumes the stage this
                // callback returns, and by this point no timeout is left to fail the candidate, so
                // anything escaping -- translateRegisterFailure's casts, a forceClose() on an event
                // loop that is shutting down -- would leave perAddressFuture uncompleted forever
                // and hang the connect attempt (see connectToAddress's invariant).
                abandonCandidate(
                    driverChannel,
                    perAddressFuture,
                    new ConnectionInitException("Unexpected error after REGISTER", t));
              }
            });
  }

  /**
   * Gives the one REGISTER rejection with a known cause a message that names it: the server not
   * knowing the {@code CLIENT_ROUTES_CHANGE} event type. This translation lived in the init handler
   * when REGISTER was an init step, and exists so that the caller
   * (ClientRoutesTopologyMonitor.init()) reports a clear error instead of silently degrading.
   */
  private static Throwable translateRegisterFailure(Throwable error) {
    if (error instanceof UnexpectedResponseException) {
      Message response = ((UnexpectedResponseException) error).message;
      if (response instanceof com.datastax.oss.protocol.internal.response.Error) {
        com.datastax.oss.protocol.internal.response.Error protocolError =
            (com.datastax.oss.protocol.internal.response.Error) response;
        if (protocolError.code == ProtocolConstants.ErrorCode.PROTOCOL_ERROR
            && protocolError.message.contains(ProtocolConstants.EventType.CLIENT_ROUTES_CHANGE)) {
          return new UnsupportedEventTypeException(
              "Server does not support CLIENT_ROUTES_CHANGE event "
                  + "(requires ScyllaDB Enterprise >= 2026.1). "
                  + "Either upgrade the server or remove the client routes configuration.",
              error);
        }
        // Any other server error naming REGISTER. Reported the way ProtocolInitHandler reported it
        // while REGISTER was an init step -- error code name included, which
        // UnexpectedResponseException does not carry (its message renders the Error as
        // "ERROR(<text>)", dropping the code). The type is deliberately not what the init handler
        // produced: that path ended in failOnUnexpected(), whose IllegalArgumentException is
        // neither a DriverException nor especially informative about what failed.
        return new ConnectionInitException(
            String.format(
                "REGISTER: server replied with unexpected error code [%s]: %s",
                ProtocolUtils.errorCodeString(protocolError.code), protocolError.message),
            error);
      }
    }
    return error;
  }

  /**
   * A REGISTER rejection that is a property of the <b>server</b> -- it does not know an event type
   * the driver asked for -- rather than of the address that was dialled.
   *
   * <p>A {@link ConnectionInitException}, so that callers which branch on the type (see {@link
   * #surfacedFailure}, and {@code ClientRoutesTopologyMonitor#init}, which reports the message)
   * treat it exactly as they treated the same rejection when REGISTER was an init step. The subtype
   * exists only so {@link #isNodeWideFailure} can recognise it.
   */
  @VisibleForTesting
  static class UnsupportedEventTypeException extends ConnectionInitException {
    UnsupportedEventTypeException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * One candidate address's future, together with the one-shot latch that says which of {@link
   * #completeCandidate} and {@link #abandonCandidate} owns the outcome.
   *
   * <p>A separate latch rather than the future's own completion state, because the two decisions
   * have to be made in opposite orders. A candidate must be known accepted <b>before</b> it
   * publishes, so that {@link #latchNegotiatedState} has already run by the time any caller can
   * hold the channel -- while {@code complete()} only reports whether it won <b>after</b>
   * publishing. Settling first separates the two: the winner latches and then publishes, the loser
   * touches neither.
   */
  private static class CandidateFuture extends CompletableFuture<DriverChannel> {

    // newIncompleteFuture() is deliberately not overridden: a derived stage carrying its own copy
    // of the latch would imply a guarantee it does not have, and nothing needs one here -- the only
    // stage derived from this future is the discarded return of tryNextCandidate's whenComplete.

    private final AtomicBoolean settled = new AtomicBoolean();

    /** Whether the caller is the one that gets to decide this candidate's outcome. */
    boolean settle() {
      return settled.compareAndSet(false, true);
    }
  }

  /**
   * Records what the candidate negotiated and publishes its channel, in that order.
   *
   * <p>{@code onAccepted} -- see {@link #latchNegotiatedState} -- runs <b>before</b> the channel is
   * published. {@code complete()} drives the downstream continuations synchronously, so the moment
   * it returns a caller on another thread may already hold the channel; latching afterwards leaves
   * a window in which it does while {@link #getProtocolVersion()} still sees {@code null} and
   * throws its "not known yet" precondition, and in which a concurrently-built channel reads a null
   * {@code clusterName} and skips the cluster-name check. The fields are {@code volatile}, so that
   * is ordering rather than visibility -- but the window is real, and it is what made {@code
   * ChannelFactoryProtocolNegotiationTest} await the value instead of reading it.
   *
   * <p>Latching first is only safe because {@link CandidateFuture#settle()} has already decided the
   * outcome. Latching unconditionally would not be: a candidate the hook timeout has abandoned
   * would still leave its cluster name behind, which is exactly what {@link #latchNegotiatedState}
   * must not allow.
   *
   * <p>Losing the latch means the channel is nobody's -- the winner failed the future and will not
   * be handed this channel -- so it is closed here rather than leaked.
   *
   * <p>Winning it carries the opposite duty: <b>this call must then complete the future on every
   * path, including a throwing {@code onAccepted}.</b> Every blanket catch downstream of {@link
   * #finishCandidate} discharges the "always completes {@code perAddressFuture}" invariant by
   * calling {@link #abandonCandidate}, and that is a no-op once the candidate is settled -- so a
   * throw escaping here would leave the future settled but never completed, hanging the connect
   * with {@code Reconnection} stuck in ATTEMPT_IN_PROGRESS and leaking the channel, with no timeout
   * left to rescue it (REGISTER has completed and the hook timeout is already cancelled). {@link
   * #latchNegotiatedState} is not throw-free: on the Cloud path it reaches {@code
   * TypesafeDriverConfig#overrideDefaults}, which re-parses the whole configuration.
   */
  private static void completeCandidate(
      DriverChannel driverChannel, CandidateFuture perAddressFuture, Runnable onAccepted) {
    if (!perAddressFuture.settle()) {
      driverChannel.forceClose();
      return;
    }
    try {
      onAccepted.run();
    } catch (Throwable t) {
      // Settling made this the only call that can still complete the future -- see the javadoc.
      perAddressFuture.completeExceptionally(t);
      driverChannel.forceClose();
      return;
    }
    if (!perAddressFuture.complete(driverChannel)) {
      // Defensive. Several paths complete the future without settling it: the blanket catches in
      // connectToAddress and bootstrapAndConnect, and ChannelFactoryInitializer#initChannel. One
      // of them -- bootstrapAndConnect's connect-listener catch -- wraps the whole listener body
      // and so can fire with this very channel already built. None can reach here today, all being
      // upstream of the hook and REGISTER, but an unpublished channel nobody holds is a leak.
      driverChannel.forceClose();
    }
  }

  /**
   * Closes a candidate channel that will not be used and fails its future -- unless that channel
   * has meanwhile been handed to the caller, in which case it is theirs and must be left alone.
   */
  private static void abandonCandidate(
      DriverChannel driverChannel, CandidateFuture perAddressFuture, Throwable error) {
    // The hook timeout and the hook's own completion race, and the hook's stage may complete off
    // the channel's event loop -- the contract allows it, and a custom TopologyMonitor behind the
    // control connection's hook is free to -- so cancel(false) can lose to a timeout task that has
    // already started running. Losing the settle means completeCandidate got there first, so the
    // channel is the caller's: closing it would leave them owning a dead channel with no error to
    // explain it, and this error is moot anyway.
    //
    // Winning it makes the channel ours even if the future was already failed elsewhere (a blanket
    // catch in the connect listener), in which case completeExceptionally is a no-op and the close
    // is the point. forceClose is idempotent.
    if (!perAddressFuture.settle()) {
      return;
    }
    perAddressFuture.completeExceptionally(error);
    driverChannel.forceClose();
  }

  /**
   * Binds {@code endPoint} to the address a connection is being opened to, when the implementation
   * supports it.
   *
   * <p>Third-party {@link EndPoint}s that do not implement {@link PinnableEndPoint} are returned
   * unchanged, so they keep behaving exactly as they did before multi-address support: the channel
   * carries the endpoint it was given.
   *
   * <p>So is an endpoint whose candidate came back unresolved. {@link
   * PinnableEndPoint#pinTo(SocketAddress)} is documented to take an address that is already
   * resolved, and one path through {@link #resolveCandidates} does not provide one: a resolver that
   * reports the address already resolved is taken at its word and the name goes out untouched (the
   * other two pass-throughs materialize an IP literal or fail, and {@link #dropUnresolved} removes
   * unresolved results of {@code resolveAll}). For an endpoint that hands out a hostname, pinning
   * there would freeze it on a name that still re-expands on every connect: no address stability
   * gained, and whatever the endpoint does instead of consulting its own source once pinned is
   * lost.
   */
  private static EndPoint pin(EndPoint endPoint, SocketAddress resolvedAddress) {
    if (resolvedAddress instanceof InetSocketAddress
        && ((InetSocketAddress) resolvedAddress).isUnresolved()) {
      return endPoint;
    }
    return endPoint instanceof PinnableEndPoint
        ? ((PinnableEndPoint) endPoint).pinTo(resolvedAddress)
        : endPoint;
  }

  @VisibleForTesting
  ChannelInitializer<Channel> initializer(
      EndPoint endPoint,
      ProtocolVersion protocolVersion,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      CompletableFuture<DriverChannel> resultFuture) {
    return new ChannelFactoryInitializer(
        endPoint, protocolVersion, options, nodeMetricUpdater, resultFuture);
  }

  class ChannelFactoryInitializer extends ChannelInitializer<Channel> {

    private final EndPoint endPoint;
    private final ProtocolVersion protocolVersion;
    private final DriverChannelOptions options;
    private final NodeMetricUpdater nodeMetricUpdater;
    private final CompletableFuture<DriverChannel> resultFuture;

    ChannelFactoryInitializer(
        EndPoint endPoint,
        ProtocolVersion protocolVersion,
        DriverChannelOptions options,
        NodeMetricUpdater nodeMetricUpdater,
        CompletableFuture<DriverChannel> resultFuture) {

      this.endPoint = endPoint;
      this.protocolVersion = protocolVersion;
      this.options = options;
      this.nodeMetricUpdater = nodeMetricUpdater;
      this.resultFuture = resultFuture;
    }

    @Override
    protected void initChannel(Channel channel) {
      try {
        DriverExecutionProfile defaultConfig = context.getConfig().getDefaultProfile();

        long setKeyspaceTimeoutMillis =
            defaultConfig
                .getDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT)
                .toMillis();
        int maxFrameLength =
            (int) defaultConfig.getBytes(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH);
        int maxRequestsPerConnection =
            defaultConfig.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS);
        int configuredMaxOrphanRequests =
            defaultConfig.getInt(DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS);
        int maxOrphanRequests =
            effectiveMaxOrphanRequests(maxRequestsPerConnection, configuredMaxOrphanRequests);
        if (configuredMaxOrphanRequests >= maxRequestsPerConnection) {
          if (LOGGED_ORPHAN_WARNING.compareAndSet(false, true)) {
            LOG.warn(
                "[{}] Invalid value for {}: {}. It must be lower than {}. "
                    + "Defaulting to {} (1/4 of max-requests) instead.",
                logPrefix,
                DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS.getPath(),
                configuredMaxOrphanRequests,
                DefaultDriverOption.CONNECTION_MAX_REQUESTS.getPath(),
                maxOrphanRequests);
          }
        }

        InFlightHandler inFlightHandler =
            new InFlightHandler(
                protocolVersion,
                new StreamIdGenerator(maxRequestsPerConnection),
                maxOrphanRequests,
                setKeyspaceTimeoutMillis,
                channel.newPromise(),
                options.eventCallback,
                options.ownerLogPrefix);
        HeartbeatHandler heartbeatHandler = new HeartbeatHandler(defaultConfig);
        ProtocolInitHandler initHandler =
            new ProtocolInitHandler(
                context, protocolVersion, clusterName, endPoint, options, heartbeatHandler, true);

        ChannelPipeline pipeline = channel.pipeline();
        context
            .getSslHandlerFactory()
            .ifPresent(f -> pipeline.addLast(SSL_HANDLER_NAME, f.newSslHandler(channel, endPoint)));

        // Only add meter handlers on the pipeline if metrics are enabled.
        SessionMetricUpdater sessionMetricUpdater = context.getMetricsFactory().getSessionUpdater();
        if (nodeMetricUpdater.isEnabled(DefaultNodeMetric.BYTES_RECEIVED, null)
            || sessionMetricUpdater.isEnabled(DefaultSessionMetric.BYTES_RECEIVED, null)) {
          pipeline.addLast(
              INBOUND_TRAFFIC_METER_NAME,
              new InboundTrafficMeter(nodeMetricUpdater, sessionMetricUpdater));
        }

        if (nodeMetricUpdater.isEnabled(DefaultNodeMetric.BYTES_SENT, null)
            || sessionMetricUpdater.isEnabled(DefaultSessionMetric.BYTES_SENT, null)) {
          pipeline.addLast(
              OUTBOUND_TRAFFIC_METER_NAME,
              new OutboundTrafficMeter(nodeMetricUpdater, sessionMetricUpdater));
        }

        pipeline
            .addLast(
                FRAME_TO_BYTES_ENCODER_NAME,
                new FrameEncoder(context.getFrameCodec(), ProtocolFeatures.EMPTY, maxFrameLength))
            .addLast(
                BYTES_TO_FRAME_DECODER_NAME,
                new FrameDecoder(context.getFrameCodec(), ProtocolFeatures.EMPTY, maxFrameLength))
            // Note: HeartbeatHandler is inserted here once init completes
            .addLast(INFLIGHT_HANDLER_NAME, inFlightHandler)
            .addLast(INIT_HANDLER_NAME, initHandler);

        context.getNettyOptions().afterChannelInitialized(channel);
      } catch (Throwable t) {
        // If the init handler throws an exception, Netty swallows it and closes the channel. We
        // want to propagate it instead, so fail this candidate's future. Note that is the
        // per-address one, not the result of connect(): a pipeline failure that is not specific to
        // the address (a bad truststore, say) therefore advances to the next candidate and is
        // retried against each of them, which tryNextCandidate() documents as the deliberate
        // trade-off for not being able to tell the two apart.
        resultFuture.completeExceptionally(t);
        throw t;
      }
    }
  }

  static class PortAllocator {
    private static final AtomicInteger lastPort = new AtomicInteger(-1);
    private static final Logger LOG = LoggerFactory.getLogger(PortAllocator.class);

    public static int getNextAvailablePort(int shardCount, int shardId, DriverContext context) {
      int lowPort =
          context
              .getConfig()
              .getDefaultProfile()
              .getInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW);
      int highPort =
          context
              .getConfig()
              .getDefaultProfile()
              .getInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH);
      if (highPort - lowPort < shardCount) {
        LOG.error(
            "There is not enough ports in range [{},{}] for {} shards. Update your configuration.",
            lowPort,
            highPort,
            shardCount);
      }
      int lastPortValue, foundPort = -1;
      do {
        lastPortValue = lastPort.get();

        // We will scan from lastPortValue
        // (or lowPort is there was no lastPort or lastPort is too low)
        int scanStart = lastPortValue == -1 ? lowPort : lastPortValue;
        if (scanStart < lowPort) {
          scanStart = lowPort;
        }

        // Round it up to "% shardCount == shardId"
        scanStart += (shardCount - scanStart % shardCount) + shardId;

        // Scan from scanStart upwards to highPort.
        for (int port = scanStart; port <= highPort; port += shardCount) {
          if (isTcpPortAvailable(port, context)) {
            foundPort = port;
            break;
          }
        }

        // If we started scanning from a high scanStart port
        // there might have been not enough ports left that are
        // smaller than highPort. Scan from the beginning
        // from the lowPort.
        if (foundPort == -1) {
          scanStart = lowPort + (shardCount - lowPort % shardCount) + shardId;

          for (int port = scanStart; port <= highPort; port += shardCount) {
            if (isTcpPortAvailable(port, context)) {
              foundPort = port;
              break;
            }
          }
        }

        // No luck! All ports taken!
        if (foundPort == -1) {
          return -1;
        }
      } while (!lastPort.compareAndSet(lastPortValue, foundPort));

      return foundPort;
    }

    public static boolean isTcpPortAvailable(int port, DriverContext context) {
      try {
        ServerSocket serverSocket = new ServerSocket();
        try {
          serverSocket.setReuseAddress(
              context
                  .getConfig()
                  .getDefaultProfile()
                  .getBoolean(DefaultDriverOption.SOCKET_REUSE_ADDRESS, false));
          serverSocket.bind(new InetSocketAddress(port), 1);
          return true;
        } finally {
          serverSocket.close();
        }
      } catch (IOException ex) {
        return false;
      }
    }
  }
}
