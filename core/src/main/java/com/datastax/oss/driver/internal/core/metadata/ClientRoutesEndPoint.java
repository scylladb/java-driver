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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientRoutesEndPoint implements PinnableEndPoint {

  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesEndPoint.class);

  private final UUID hostId;
  private final ClientRoutesTopologyMonitor topologyMonitor;
  private final String metricPrefix;
  @NonNull private final EndPoint fallbackEndPoint;
  /** Kept only so that {@link #pinTo(SocketAddress)} can rebuild an identical copy. */
  @Nullable private final InetAddress broadcastInetAddress;

  /**
   * The address this endpoint has been {@linkplain #pinTo(SocketAddress) pinned} to, or {@code
   * null} if it is not pinned. Deliberately excluded from {@link #equals} and {@link #hashCode},
   * which key off the host id alone.
   */
  @Nullable private final InetSocketAddress pinnedAddress;

  /**
   * @param topologyMonitor the topology monitor used to resolve the endpoint address on demand.
   * @param hostId the host UUID identifying this node in the cluster.
   * @param broadcastInetAddress the node's broadcast address (from system.peers or system.local),
   *     used to build a stable metric prefix. May be {@code null} if the address could not be
   *     determined, in which case the hostId is used as the metric prefix instead.
   * @param fallbackEndPoint the default endpoint to fall back to when {@code
   *     topologyMonitor.resolve()} returns {@code null}, i.e. when this node is not accessed via a
   *     cloud private endpoint. Must not be {@code null}.
   */
  public ClientRoutesEndPoint(
      @NonNull ClientRoutesTopologyMonitor topologyMonitor,
      @NonNull UUID hostId,
      @Nullable InetAddress broadcastInetAddress,
      @NonNull EndPoint fallbackEndPoint) {
    this(topologyMonitor, hostId, broadcastInetAddress, fallbackEndPoint, null);
  }

  private ClientRoutesEndPoint(
      @NonNull ClientRoutesTopologyMonitor topologyMonitor,
      @NonNull UUID hostId,
      @Nullable InetAddress broadcastInetAddress,
      @NonNull EndPoint fallbackEndPoint,
      @Nullable InetSocketAddress pinnedAddress) {
    this.topologyMonitor =
        Objects.requireNonNull(topologyMonitor, "Topology monitor cannot be null");
    this.hostId = Objects.requireNonNull(hostId, "HOST uuid cannot be null");
    this.fallbackEndPoint =
        Objects.requireNonNull(fallbackEndPoint, "Fallback endpoint cannot be null");
    this.metricPrefix = buildMetricPrefix(broadcastInetAddress, hostId);
    this.broadcastInetAddress = broadcastInetAddress;
    this.pinnedAddress = pinnedAddress;
  }

  @NonNull
  public UUID getHostId() {
    return hostId;
  }

  /**
   * The endpoint {@link #resolve()} falls back to when this node has no client route.
   *
   * <p>Exposed so that {@link ClientRoutesTopologyMonitor#buildNodeEndPoint} can avoid nesting one
   * of these inside another: for the {@code system.local} row the superclass hands back the control
   * channel's own endpoint, which in a client-routes deployment is already a {@code
   * ClientRoutesEndPoint} -- and a <b>pinned</b> one, so nesting it would freeze the fallback on
   * one proxy IP and add a level per control reconnect.
   */
  @NonNull
  EndPoint getFallbackEndPoint() {
    return fallbackEndPoint;
  }

  /**
   * Returns the address connections should be opened to.
   *
   * <p>The client route for this host id is an in-memory lookup over the cached {@code
   * system.client_routes} contents, and it yields exactly one address by design, so this neither
   * blocks nor expands to several candidates. The route's hostname is returned {@linkplain
   * InetSocketAddress#isUnresolved() unresolved}: {@link
   * com.datastax.oss.driver.internal.core.channel.ChannelFactory} resolves it through Netty's
   * configured {@code AddressResolverGroup}, so a custom resolver is honoured and no DNS lookup
   * runs on the caller (the admin event loop, for control-connection reconnects).
   *
   * <p>When the topology monitor has no route for this host id — i.e. the node is not reached
   * through a cloud private endpoint — this delegates to the fallback endpoint.
   *
   * <p>Once {@linkplain #pinTo(SocketAddress) pinned} the pinned address is returned directly.
   */
  @NonNull
  @Override
  public SocketAddress resolve() {
    if (pinnedAddress != null) {
      return pinnedAddress;
    }
    InetSocketAddress address;
    try {
      address = topologyMonitor.resolve(hostId);
    } catch (IllegalStateException e) {
      // The monitor is closed, so its route cache is gone -- but resolve() still has to answer, and
      // the honest answer is "no route available", which is what the fallback endpoint is for.
      // Throwing here is not contained anywhere useful: PinnableEndPoint#sameIdentity compares
      // resolve() results for every node of every topology refresh, and neither NodesRefresh nor
      // MetadataManager#apply catches, so a refresh that raced session shutdown would be dropped
      // whole and surface only as a DEBUG log in ControlConnection#onSuccessfulReconnect.
      //
      // Logged rather than swallowed silently: in a private-endpoint deployment the fallback is the
      // node's raw broadcast address, which is not client-routable, so the visible symptom is a
      // bare
      // connect timeout with nothing naming the cause.
      LOG.debug(
          "[{}] Client routes monitor is closed, falling back to {} for this node",
          hostId,
          fallbackEndPoint);
      address = null;
    }
    return address != null ? address : fallbackEndPoint.resolve();
  }

  @NonNull
  @Override
  public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
    Objects.requireNonNull(resolvedAddress, "resolvedAddress cannot be null");
    // Mirror DefaultEndPoint: an address we cannot hold in an InetSocketAddress field skips
    // pinning rather than failing the connection. So does an unresolved one -- resolve() hands out
    // the route's hostname unresolved, and ChannelFactory passes it straight back when the user
    // disabled the resolver or a custom one declines it. Pinning that would freeze the endpoint on
    // a name that still re-expands on every connect: no address stability gained, and the route
    // lookup silenced for good, since resolve() short-circuits once pinned.
    if (!(resolvedAddress instanceof InetSocketAddress)
        || ((InetSocketAddress) resolvedAddress).isUnresolved()
        || resolvedAddress.equals(this.pinnedAddress)) {
      return this;
    }
    return new ClientRoutesEndPoint(
        topologyMonitor,
        hostId,
        broadcastInetAddress,
        fallbackEndPoint,
        (InetSocketAddress) resolvedAddress);
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code true} <b>when the address came from a route</b>: a route's addresses are alternative
   * ways in to this one node, so connections may be spread across them.
   *
   * <p>Otherwise the address is the fallback endpoint's, and whether <i>those</i> addresses are
   * interchangeable is not this class's to claim -- for the usual fallback, a {@code
   * DefaultEndPoint} built from a translated broadcast address, it is {@code false}, and a
   * translator that hands back a name ({@code SubnetAddressTranslator} does, under {@code
   * resolve-addresses = false}) is exactly the case where spreading would land one node's channels
   * on different hosts. So the question is deferred to whoever owns the address.
   *
   * <p>Which of the two it is is read off {@code resolvedAddress} rather than by asking the route
   * cache a second time. The cache is an {@code AtomicReference} swapped from the routes-query
   * thread, not from the one {@code ChannelFactory#connect} runs on, so a {@code
   * CLIENT_ROUTES_CHANGE} landing between {@link #resolve()} and this call would otherwise have the
   * two disagree -- and in the direction that matters, a route appearing after a fallback address
   * was already chosen, the disagreement authorises shuffling exactly the kind of name this method
   * exists to protect.
   *
   * <p>Reading the fallback twice in one connect is safe for every fallback the driver builds:
   * {@code fallbackEndPoint} is final, and each of them is a {@link DefaultEndPoint} whose {@code
   * resolve()} is a field read. It is not safe by <i>type</i>, though -- the field is declared
   * {@link EndPoint}, and on the {@code system.local} path it holds whatever {@code
   * DefaultTopologyMonitor#buildNodeEndPoint} returned, which passes a subclass's endpoint through
   * unchanged. A fallback whose {@code resolve()} is not idempotent -- the shape {@link
   * SniEndPoint} itself had until contact points were kept unresolved, rotating through the proxy's
   * A-records on every call -- would answer differently here than it did there, and its own address
   * would then be reported as route-derived and spread across. Not reachable in tree; closing it
   * properly means having {@code resolve()} report which source it used, which is per-connect state
   * on an endpoint shared by every connect, so it is named here rather than claimed away.
   */
  @Override
  public boolean addressesAreInterchangeable(@NonNull SocketAddress resolvedAddress) {
    // Pinned: resolve() short-circuits on the pinned address, so that is what was handed out, and
    // it denotes the single server this endpoint is now fixed to. Mirrored here so the pinned case
    // does not consult the route cache at all -- resolve() has not done so since it was pinned.
    if (pinnedAddress != null) {
      return false;
    }
    // resolve() returns either the route's address or the fallback's, so anything that is not the
    // fallback's came from a route.
    return !resolvedAddress.equals(fallbackEndPoint.resolve())
        || (fallbackEndPoint instanceof PinnableEndPoint
            && ((PinnableEndPoint) fallbackEndPoint).addressesAreInterchangeable(resolvedAddress));
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ClientRoutesEndPoint) {
      ClientRoutesEndPoint that = (ClientRoutesEndPoint) other;
      return this.hostId.equals(that.hostId);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostId);
  }

  @Override
  public String toString() {
    return "ClientRoutesEndPoint(" + hostId + ")";
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(@Nullable InetAddress address, @NonNull UUID hostId) {
    if (address == null) {
      return hostId.toString();
    }
    // getHostAddress() returns clean IP without leading slash:
    //   IPv4: "127.0.0.1"   IPv6: "0:0:0:0:0:0:0:1"
    // Replace dots for IPv4; colons are kept for IPv6 (consistent with DefaultEndPoint)
    return address.getHostAddress().replace('.', '_') + '_' + hostId;
  }
}
