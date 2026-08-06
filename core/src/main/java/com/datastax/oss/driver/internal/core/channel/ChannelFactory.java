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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeShardingInfo;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
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
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
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
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
   * try. {@link #tryNextCandidate} needs the distinction because an unidentified contact-point name
   * may expand to addresses of <b>different</b> nodes, while every address of an identified node is
   * that same node.
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
    } catch (Exception e) {
      resultFuture.completeExceptionally(e);
      return;
    }

    // EndPoint.resolve() is contractually non-blocking and performs no name resolution, so it is
    // safe to call here even though connect() runs on the admin event loop for control-connection
    // reconnects. Everything a name needs to become connectable happens in resolveCandidates().
    SocketAddress address;
    try {
      address = endPoint.resolve();
    } catch (Exception e) {
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

    resolveCandidates(
            baseBootstrap, address, eventLoop, spreadAcrossAddresses(endPoint, nodeIsIdentified))
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
                  nodeIsIdentified,
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
      // works if it is usable as-is.
      IllegalStateException unusable =
          unusableWithoutResolution(address, "the bootstrap has name resolution disabled");
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
              if (!resolver.isSupported(address) || resolver.isResolved(address)) {
                // Nothing for the resolver to do; same short-circuit as
                // Bootstrap#doResolveAndConnect0. An address the resolver declines is in the same
                // position as one with no resolver at all, so it gets the same check; an
                // already-resolved one is usable by definition and passes straight through.
                IllegalStateException unusable =
                    unusableWithoutResolution(
                        address, "the configured resolver does not support this address");
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
   */
  private static IllegalStateException unusableWithoutResolution(
      SocketAddress address, String why) {
    if (!(address instanceof InetSocketAddress) || !((InetSocketAddress) address).isUnresolved()) {
      return null;
    }
    return new IllegalStateException(
        String.format(
            "Cannot connect to %s: it is an unresolved address and %s, so nothing will resolve it. "
                + "Either remove Bootstrap.disableResolver() from "
                + "NettyOptions.afterBootstrapInitialized(), or supply an already-resolved address.",
            address, why));
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
   * <p>Re-attaching changes nothing else: {@code InetAddress.getByAddress(host, bytes)} performs no
   * lookup, the TCP connect target is the same IP, and a resolved {@link InetSocketAddress}'s
   * equality ignores host names, so pinning and the pin-equality shortcuts are unaffected. A scoped
   * IPv6 candidate keeps its scope, since {@link Inet6Address} has {@code getByAddress} overloads
   * that carry one.
   *
   * <p>An original that carries no name of its own is left alone (see {@link
   * AddressUtils#carriesName}): a resolver is free to redirect it to a different IP, and labelling
   * that IP with the literal form of the one we asked for would invent a name that resolves to
   * something else.
   */
  @VisibleForTesting
  static SocketAddress reattachHostname(SocketAddress original, SocketAddress candidate) {
    if (!(original instanceof InetSocketAddress) || !(candidate instanceof InetSocketAddress)) {
      return candidate;
    }
    InetSocketAddress originalInet = (InetSocketAddress) original;
    InetSocketAddress candidateInet = (InetSocketAddress) candidate;
    InetAddress candidateIp = candidateInet.getAddress();
    if (!AddressUtils.carriesName(originalInet)
        || candidateIp == null
        // Nothing to change: the candidate already carries the queried name, which is the common
        // case (the JDK and Netty-DNS resolvers attach it themselves). getHostString() never looks
        // anything up -- for a nameless address it falls back to the IP literal.
        || candidateInet.getHostString().equals(originalInet.getHostString())) {
      return candidate;
    }
    try {
      return new InetSocketAddress(
          withHostName(originalInet.getHostString(), candidateIp), candidateInet.getPort());
    } catch (UnknownHostException impossible) {
      // getByAddress only rejects illegal byte lengths, and these bytes come from a real
      // InetAddress; keep the raw candidate rather than failing the connect over a cosmetic step.
      return candidate;
    }
  }

  /**
   * Returns a copy of {@code ip} labelled with {@code hostName}, preserving an IPv6 scope if there
   * is one.
   *
   * <p>{@link InetAddress#getByAddress(String, byte[])} cannot carry a scope, and dropping one
   * would change where the address actually points — a link-local address is only meaningful
   * together with its zone. {@link Inet6Address#getByAddress(String, byte[], int)} carries the zone
   * as its numeric id, which is what the connect itself goes on; a scope id of 0 means "unscoped"
   * and is accepted, so this needs no special case for a plain IPv6 address.
   *
   * <p>The sibling overload taking a {@link NetworkInterface} is deliberately not used: it
   * re-derives the numeric scope by searching that interface for an address of the same local type,
   * and throws {@code UnknownHostException("no scope_id found")} when it finds none — so it can
   * fail for an address that was legitimately built from an interface in the first place. All that
   * is lost by going numeric is the interface <i>name</i>, which surfaces in {@code toString()} and
   * nowhere else.
   */
  private static InetAddress withHostName(String hostName, InetAddress ip)
      throws UnknownHostException {
    return ip instanceof Inet6Address
        ? Inet6Address.getByAddress(hostName, ip.getAddress(), ((Inet6Address) ip).getScopeId())
        : InetAddress.getByAddress(hostName, ip.getAddress());
  }

  /**
   * Whether {@link #shuffleAndLimit} may spread this connect across the addresses the endpoint
   * expands to.
   *
   * <p>A <b>contact point</b> always may: its addresses may well be different nodes, so there is no
   * node identity to preserve, and spreading both balances load and varies which address an attempt
   * starts from. For an {@linkplain #isIdentified(Node) identified} node it depends on what the
   * name denotes, which only the endpoint knows — see {@link
   * PinnableEndPoint#addressesAreInterchangeable()} for the two cases and why they differ. An
   * endpoint that does not implement {@link PinnableEndPoint} is treated as not interchangeable,
   * which is also the conservative reading for a third-party implementation.
   */
  @VisibleForTesting
  static boolean spreadAcrossAddresses(EndPoint endPoint, boolean nodeIsIdentified) {
    return !nodeIsIdentified
        || (endPoint instanceof PinnableEndPoint
            && ((PinnableEndPoint) endPoint).addressesAreInterchangeable());
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
   * <p>One failure is <b>node-wide</b> -- it dooms every remaining address rather than only the one
   * that was tried: an {@link UnsupportedProtocolVersionException} against an <b>identified</b>
   * node ({@code nodeIsIdentified}, see {@link #isIdentified(Node)}). Every address of an
   * identified node is that same node, so a protocol-version rejection is a property of all of
   * them, and the attempt fails immediately instead of replaying the negotiation against every
   * remaining IP. This matches the pre-multi-address behaviour of a single-address connect. The
   * corner it deliberately does not rescue: a heterogeneous rolling upgrade where different IPs of
   * one identified node genuinely support different protocol versions.
   *
   * <p>Note what that leaves uncovered, because it is narrower than it looks: {@code isNegotiating}
   * is true only while {@link #protocolVersion} is still unset, i.e. on the session's first
   * connection, which is always to a contact point -- and a contact point is never identified (see
   * {@link #isIdentified}). So a rejection reached by <i>negotiation</i> never takes this branch;
   * only a <i>forced</i> version rejected by an already-identified node does, which is the
   * single-attempt case. A negotiated rejection still walks the downgrade ladder from the top on
   * every candidate, since each gets a fresh {@code attemptedVersions} list, so a name with N
   * records costs N ladders (with N bounded by {@link #shuffleAndLimit}).
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
      boolean nodeIsIdentified,
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
      CompletableFuture<DriverChannel> perAddressFuture = new CompletableFuture<>();
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
              boolean nodeWide = error != null && isNodeWideFailure(error, nodeIsIdentified);
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
                    nodeIsIdentified,
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
   */
  private static boolean isNodeWideFailure(Throwable error, boolean nodeIsIdentified) {
    return error instanceof UnsupportedProtocolVersionException && nodeIsIdentified;
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
   * classified one. Every failure is still attached, so nothing is lost either way.
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
      CompletableFuture<DriverChannel> perAddressFuture,
      SocketAddress resolvedAddress) {

    // Invariant, as in tryNextCandidate(): every path completes perAddressFuture. The synchronous
    // section can throw from Bootstrap validation; the connect listener runs inside a Netty
    // callback that swallows throwables and contains the downgrade recursion, the version-registry
    // lookup and the config overrides, any of which throwing would otherwise hang the attempt.
    try {
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

      ChannelFuture connectFuture;
      if (shardId == null || shardingInfo == null) {
        if (shardId != null) {
          LOG.debug(
              "Requested connection to shard {} but shardingInfo is currently missing for Node at endpoint {}. Falling back to arbitrary local port.",
              shardId,
              endPoint);
        }
        connectFuture = bootstrap.connect(resolvedAddress);
      } else {
        int localPort =
            PortAllocator.getNextAvailablePort(shardingInfo.getShardsCount(), shardId, context);
        if (localPort == -1) {
          LOG.warn(
              "Could not find free port for shard {} at {}. Falling back to arbitrary local port.",
              shardId,
              endPoint);
          connectFuture = bootstrap.connect(resolvedAddress);
        } else {
          connectFuture = bootstrap.connect(resolvedAddress, new InetSocketAddress(localPort));
        }
      }

      connectFuture.addListener(
          cf -> {
            try {
              if (connectFuture.isSuccess()) {
                Channel channel = connectFuture.channel();
                DriverChannel driverChannel =
                    new DriverChannel(
                        endPoint, channel, context.getWriteCoalescer(), currentVersion);
                // If this is the first successful connection, remember the protocol version and
                // cluster name for future connections.
                if (isNegotiating) {
                  ChannelFactory.this.protocolVersion = currentVersion;
                }
                if (ChannelFactory.this.clusterName == null) {
                  ChannelFactory.this.clusterName = driverChannel.getClusterName();
                }
                Map<String, List<String>> supportedOptions = driverChannel.getOptions();
                if (ChannelFactory.this.productType == null && supportedOptions != null) {
                  List<String> productTypes = supportedOptions.get("PRODUCT_TYPE");
                  String productType =
                      productTypes != null && !productTypes.isEmpty()
                          ? productTypes.get(0)
                          : UNKNOWN_PRODUCT_TYPE;
                  ChannelFactory.this.productType = productType;
                  DriverConfig driverConfig = context.getConfig();
                  if (driverConfig instanceof TypesafeDriverConfig
                      && productType.equals(DATASTAX_CLOUD_PRODUCT_TYPE)) {
                    ((TypesafeDriverConfig) driverConfig)
                        .overrideDefaults(
                            ImmutableMap.of(
                                DefaultDriverOption.REQUEST_CONSISTENCY,
                                ConsistencyLevel.LOCAL_QUORUM.name()));
                  }
                }
                perAddressFuture.complete(driverChannel);
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
   * Binds {@code endPoint} to the address a connection is being opened to, when the implementation
   * supports it.
   *
   * <p>Third-party {@link EndPoint}s that do not implement {@link PinnableEndPoint} are returned
   * unchanged, so they keep behaving exactly as they did before multi-address support: the channel
   * carries the endpoint it was given.
   *
   * <p>So is an endpoint whose candidate came back unresolved. {@link
   * PinnableEndPoint#pinTo(SocketAddress)} is documented to take an address that is already
   * resolved, and {@link #resolveCandidates} has three paths that hand the original address
   * straight through -- the user disabled the resolver, the resolver does not support the address,
   * or it reports it as already resolved. For an endpoint that hands out a hostname, pinning there
   * would freeze it on a name that still re-expands on every connect: no address stability gained,
   * and whatever the endpoint does instead of consulting its own source once pinned is lost.
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
