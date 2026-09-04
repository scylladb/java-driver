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
import java.net.SocketAddress;
import java.util.Objects;

/**
 * An {@link EndPoint} that can produce a copy of itself bound ("pinned") to one specific address.
 *
 * <p>An endpoint whose hostname maps to several IPs describes a <i>set</i> of candidate addresses,
 * but a channel is always connected to exactly one of them. {@link
 * com.datastax.oss.driver.internal.core.channel.ChannelFactory} pins the endpoint to the address it
 * actually used, and hands the pinned copy to the channel. That matters for two reasons:
 *
 * <ul>
 *   <li><b>Node identity.</b> Once the driver has learnt, over a given connection, that {@code
 *       host_id} X answers at a given IP, that node must keep reconnecting to <i>that</i> IP. If
 *       the node kept the multi-address endpoint, a later reconnect could land on a different node
 *       while still being treated as X (see {@code DefaultTopologyMonitor#buildNodeEndPoint} and
 *       {@code ControlConnection}, which skip identity re-resolution for nodes that already have a
 *       host id).
 *   <li><b>No re-resolution on the channel path.</b> Components handed the channel's endpoint call
 *       {@link EndPoint#resolve()} — SSL engine creation, GSSAPI service-name lookup, {@code
 *       DefaultTopologyMonitor#savePort}. On a pinned endpoint that is a field read, so it neither
 *       blocks on DNS (SSL setup runs on a Netty event loop) nor risks picking a different address
 *       than the one the channel is connected to.
 * </ul>
 *
 * <p>This is an internal extension point: {@code ChannelFactory} pins endpoints that implement it
 * and leaves any other implementation untouched, so third-party {@link EndPoint}s keep working
 * exactly as before.
 *
 * <p>Implementations must keep {@link Object#equals}, {@link Object#hashCode}, {@link
 * EndPoint#asMetricPrefix()} <b>and {@link Object#toString()}</b> identical to the unpinned
 * original: a pinned copy denotes the same node, and every one of those is part of how the node is
 * identified from the outside. Metric names in particular must not change depending on which IP a
 * connection happened to land on — and that includes {@code toString()}, which is what {@code
 * TaggingMetricIdGenerator} tags node metrics with, and what any third-party {@code
 * MetricIdGenerator} is equally free to use. Nodes do adopt pinned copies (see {@code
 * DefaultNode#setEndPoint}), so an identity that varied with the pin would silently re-tag a node's
 * metrics mid-session. Equality must also stay symmetric: {@code original.equals(pinned)} and
 * {@code pinned.equals(original)} must agree, since endpoints are used as set and map keys.
 *
 * <p>The pinned address is therefore observable only through {@link EndPoint#resolve()}. That is no
 * loss for diagnostics: the address a channel is actually connected to appears in the channel's own
 * {@code toString()}, which Netty builds from its remote address, and {@code ChannelFactory} logs
 * each candidate as it tries it.
 */
public interface PinnableEndPoint extends EndPoint {

  /**
   * Returns a copy of this endpoint that resolves to exactly {@code resolvedAddress}.
   *
   * <p>Implementations may return {@code this} when pinning does not apply (for example when the
   * address is not of a type they can hold on to), or when it would be a no-op because the endpoint
   * already resolves to exactly that address.
   *
   * @param resolvedAddress the address a connection was successfully established to; must not be
   *     null and must already be resolved.
   */
  @NonNull
  EndPoint pinTo(@NonNull SocketAddress resolvedAddress);

  /**
   * Whether the addresses this endpoint expands to are interchangeable, i.e. reaching any one of
   * them is reaching the same node.
   *
   * <p>{@code ChannelFactory} asks this once per connect and answers two questions with it: whether
   * it may spread connections across the addresses, and whether a rejection observed at one of them
   * settles the rest. The question itself is a property of <b>what the name denotes</b>, and it
   * splits the name-based endpoints in two:
   *
   * <ul>
   *   <li>A <b>front door</b> — an SNI proxy, a cloud private-endpoint route — publishes several
   *       addresses that all lead to the same node by construction: the proxy routes by server
   *       name, not by which of its own IPs the client picked. Spreading across them is the whole
   *       point of publishing more than one, and it is what the driver did before multi-address
   *       support, when {@code SniEndPoint#resolve()} rotated through the proxy's A-records on
   *       every call.
   *   <li>A name supplied by an {@code AddressTranslator} ({@code SubnetAddressTranslator} returns
   *       one by default, under {@code resolve-addresses = false}) carries no such guarantee: it
   *       may cover several hosts. Spreading one node's connections across those would land the
   *       channels of a single {@code Node} on different servers, while routing, shard awareness
   *       and per-node metrics all attribute them to that one node. Such an endpoint keeps the
   *       resolver's order, so a pool converges on one address and the rest serve as fallback.
   * </ul>
   *
   * <p>Asked for every endpoint, identified node or contact point alike, but combined with node
   * identity differently for each of the two questions {@code ChannelFactory} derives (see its
   * {@code spreadAcrossAddresses} and {@code sameServerAtEveryAddress}):
   *
   * <ul>
   *   <li><b>Spreading.</b> An unidentified contact point is spread across its addresses whatever
   *       this answers — they may well be different nodes, and there is no node identity to
   *       preserve yet. So this only ever <i>withholds</i> spreading, and only for an identified
   *       node.
   *   <li><b>Failure scope.</b> An identified node's addresses are all that node, so a rejection
   *       there is node-wide whatever this answers. For an unidentified contact point it is this
   *       method that decides: a front door means one server answered for all of them, a plain
   *       multi-record name means the next address is a different server and must still be tried.
   * </ul>
   *
   * <p>Which is why {@code false} is the safe default and is what {@link EndPoint} implementations
   * outside this interface get: it neither withholds spreading from a contact point nor lets one
   * address speak for the others.
   *
   * <p>Implementations should derive the answer from {@code resolvedAddress} — what {@link
   * #resolve()} just returned for this connect — rather than by asking the same source again. An
   * endpoint whose answer comes from mutable state consulted twice can be asked on either side of a
   * change and give two answers that describe different addresses, and the one that matters is the
   * address actually about to be dialled. {@code ClientRoutesEndPoint} is the case in point: a
   * route appearing between the two reads would authorise spreading for an address that came from
   * its <i>fallback</i> endpoint, which is the one thing this method exists to prevent.
   *
   * <p>Stated as guidance and not as a guarantee, because the driver's own implementation of it
   * does not fully hold: {@code ClientRoutesEndPoint#addressesAreInterchangeable} decides "not from
   * a route" by comparing against {@code fallbackEndPoint.resolve()}, which is asking a source
   * again. It is sound for every fallback the driver builds — each is a {@code DefaultEndPoint}
   * whose {@code resolve()} is a field read — and unsound by type, since the field is declared
   * {@link EndPoint}. That method's own javadoc sets out the case and the fix it would take (having
   * {@code resolve()} report which source it used); it is named here so that an implementer reading
   * this paragraph is not told the in-tree code satisfies something it does not.
   *
   * @param resolvedAddress the address {@link #resolve()} returned for this connect attempt.
   */
  default boolean addressesAreInterchangeable(@NonNull SocketAddress resolvedAddress) {
    return false;
  }

  /**
   * Whether two endpoints denote the same node <i>and</i> are indistinguishable to everything that
   * reads one: same runtime type, same {@linkplain EndPoint#asMetricPrefix() metric identity}, same
   * {@linkplain EndPoint#resolve() current address}.
   *
   * <p>Deliberately not {@link Object#equals}: {@code DefaultEndPoint#equals} resolves the
   * unresolved side of a mixed comparison, which would put a blocking DNS lookup on the admin
   * thread for every endpoint that is still a hostname — and contact points are now kept
   * unresolved, so that is reachable (see issue #1006). It is also narrower than {@code equals} in
   * one direction and wider in another, which is exactly what callers need:
   *
   * <ul>
   *   <li>Narrower: a pinned copy differs from its original only by the pin, and both {@code
   *       equals} and the metric identity ignore that by contract (above), so the {@code resolve()}
   *       comparison is what tells the two apart.
   *   <li>Wider: an unresolved hostname and the address it maps to compare <i>equal</i> under
   *       {@code DefaultEndPoint#equals} while their metric prefixes differ — the case a
   *       contact-point node hits when it adopts the endpoint built from its {@code system.local}
   *       row.
   * </ul>
   *
   * <p>The class check keeps a node from staying on a plain fallback endpoint when a dynamic one
   * ({@code ClientRoutesEndPoint}) with the same current address arrives.
   *
   * <p>{@code toString()} is not part of the test, even though {@code TaggingMetricIdGenerator}
   * tags metrics with it rather than with the prefix. It is not stable across equal instances:
   * {@code DefaultEndPoint} delegates it to {@code InetSocketAddress}, which renders {@code
   * InetAddress}'s <i>cached</i> {@code hostName} field, and that field is populated the first time
   * anything calls {@code getHostName()} — which {@code DefaultSslEngineFactory} does while
   * building an engine, under the default {@code
   * advanced.ssl-engine-factory.allow-dns-reverse-lookup-san = true}. Keying on it would report a
   * difference for every node on every topology refresh. The cost of leaving it out: a tagging
   * generator can keep reporting under an endpoint string the node no longer answers to, until
   * something else changes the prefix.
   */
  static boolean sameIdentity(@NonNull EndPoint first, @NonNull EndPoint second) {
    return first.getClass() == second.getClass()
        && first.asMetricPrefix().equals(second.asMetricPrefix())
        && Objects.equals(first.resolve(), second.resolve());
  }
}
