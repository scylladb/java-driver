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
import com.datastax.oss.driver.internal.core.util.AddressUtils;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultEndPoint implements PinnableEndPoint, Serializable {

  private static final long serialVersionUID = 1;

  private static final Logger LOG = LoggerFactory.getLogger(DefaultEndPoint.class);

  /** Static, so the warning below is emitted once per JVM rather than once per endpoint. */
  @VisibleForTesting
  static final AtomicBoolean LOGGED_MIXED_COMPARISON_WARNING = new AtomicBoolean();

  private final InetSocketAddress address;
  private final String metricPrefix;

  /**
   * The address this endpoint has been {@linkplain #pinTo(SocketAddress) pinned} to, or {@code
   * null} if it is not pinned. Deliberately excluded from {@link #equals}, {@link #hashCode} and
   * {@link #asMetricPrefix()}: a pinned copy denotes the same node as the original.
   */
  @Nullable private final InetSocketAddress pinnedAddress;

  public DefaultEndPoint(InetSocketAddress address) {
    this(address, null);
  }

  private DefaultEndPoint(InetSocketAddress address, @Nullable InetSocketAddress pinnedAddress) {
    this.address = Objects.requireNonNull(address, "address can't be null");
    this.metricPrefix = buildMetricPrefix(address);
    this.pinnedAddress = pinnedAddress;
  }

  /**
   * An endpoint <b>identified by</b> {@code identity} but <b>connected to</b> {@code target}:
   * {@link #asMetricPrefix()}, {@link #equals} and {@link #toString()} answer for {@code identity},
   * while {@link #resolve()} hands out {@code target}.
   *
   * <p>The two differ in their host-name label only — {@code target} is the address a connection
   * actually reached, {@code identity} is that same address with the label stripped (see {@code
   * AddressUtils#stripHostName}). Splitting them is what lets {@code
   * DefaultTopologyMonitor#buildNodeEndPoint} give the connected node an identity of its own
   * without changing anything about how connections to it are made:
   *
   * <ul>
   *   <li><b>Identity from the bytes.</b> A resolved address's host string is not fixed — it
   *       renders {@code InetAddress}'s cached {@code hostName}, which a TLS handshake fills in
   *       with a reverse-DNS name. Keying the node's metric prefix off it would make that prefix
   *       depend on whether TLS is enabled and on whether a PTR record happens to exist.
   *   <li><b>Target keeps the label.</b> {@code DefaultSslEngineFactory} derives the TLS peer host,
   *       and {@code DseGssApiAuthProviderBase} the Kerberos service name, from {@code resolve()}.
   *       A stripped address would send both to a reverse lookup on an event loop; keeping the
   *       label means they see the name the operator configured, with no lookup, exactly as they
   *       did before the node was re-identified.
   * </ul>
   *
   * <p>{@link #pinTo} cannot express this: a resolved {@code InetSocketAddress}'s equality ignores
   * host names, so it would see {@code target} as the address already held and return {@code this}.
   */
  static DefaultEndPoint identifiedBy(InetSocketAddress identity, InetSocketAddress target) {
    return new DefaultEndPoint(identity, target);
  }

  /**
   * Returns the address connections should be opened to: the {@linkplain #pinTo(SocketAddress)
   * pinned} one if this is a pinned copy, otherwise the stored address as-is.
   *
   * <p>This performs no name resolution. If the stored address is a hostname (i.e. {@linkplain
   * InetSocketAddress#isUnresolved() unresolved} — contact points are always kept unresolved, see
   * {@link com.datastax.oss.driver.api.core.session.SessionBuilder#addContactPoint}) it is returned
   * unresolved, and {@link com.datastax.oss.driver.internal.core.channel.ChannelFactory} expands it
   * to every IP it maps to through Netty's configured {@code AddressResolverGroup}. Resolving there
   * rather than here is deliberate: it keeps any custom resolver installed via {@link
   * com.datastax.oss.driver.internal.core.context.NettyOptions#afterBootstrapInitialized} in the
   * loop, which a direct {@code InetAddress.getAllByName()} call from here would bypass, and it
   * keeps this method non-blocking so it is safe to call from an event loop.
   */
  @NonNull
  @Override
  public InetSocketAddress resolve() {
    return pinnedAddress != null ? pinnedAddress : address;
  }

  @NonNull
  @Override
  public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
    Objects.requireNonNull(resolvedAddress, "resolvedAddress can't be null");
    if (!(resolvedAddress instanceof InetSocketAddress)
        // An unresolved address, as ClientRoutesEndPoint and SniEndPoint also refuse: this endpoint
        // hands a hostname over unresolved and ChannelFactory passes it straight back when the user
        // disabled the resolver or a custom one declines it. Pinning that would freeze resolve() on
        // a name that must re-expand on every connect.
        || ((InetSocketAddress) resolvedAddress).isUnresolved()
        || resolvedAddress.equals(this.pinnedAddress)
        // The address we already hold: pinning to it changes nothing, since resolve() and
        // toString() would keep yielding what they already do. Returning this rather than an equal
        // copy is load-bearing beyond sparing an allocation: {@code
        // DefaultTopologyMonitor#connectedNodeEndPoint} keeps the control node's endpoint {@code
        // ==}
        // to the channel's, which is what lets {@code refreshNode}'s control-node check settle on
        // the identity short-circuit in equals() instead of comparing addresses.
        || resolvedAddress.equals(this.address)) {
      return this;
    }
    return new DefaultEndPoint(address, (InetSocketAddress) resolvedAddress);
  }

  /**
   * Whether {@code other} denotes the same node: the stored addresses are compared, ignoring which
   * one either endpoint may be {@linkplain #pinTo(SocketAddress) pinned} to.
   *
   * <p><b>Comparing an unresolved <i>name</i> against a resolved address costs a DNS lookup</b>,
   * taken inline on the calling thread, because the unresolved side has to be resolved first. It is
   * also arbitrary: {@code new InetSocketAddress(name, port)} keeps only the <i>first</i> address
   * the name maps to, so for a multi-record name the answer is "equal iff this node is the one the
   * resolver happened to list first". And it does not agree with {@link #hashCode()}, which keys on
   * the stored address alone -- so a hostname and one of its IPs can be {@code equals} while
   * hashing differently, and a hash-based collection of endpoints (the contact-point {@code Set},
   * for one) never treats them as the same entry.
   *
   * <p>The first two do not apply when the unresolved side is an <b>IP literal</b>, which is the
   * ordinary case rather than an exotic one: contact points are stored unresolved whatever their
   * form (see {@code AddressUtils#extract}, which {@code SessionBuilder} always calls with {@code
   * resolve = false}), so a plain {@code 1.2.3.4:9042} reaches this branch on every comparison
   * against a resolved peer. Re-building it parses the literal with no resolver call and keeps the
   * only address it can denote. The warning below is gated on {@link AddressUtils#carriesName} for
   * that reason -- it answers name-versus-literal without a lookup, and neither the cost nor the
   * arbitrariness is worth reporting for a literal.
   *
   * <p>The third hazard does apply to literals, and the gate suppresses the warning for it too.
   * {@link #hashCode()} returns {@code address.hashCode()}, which is {@code hostname.hashCode() +
   * port} on the unresolved side and {@code addr.hashCode() + port} on the resolved one -- so a
   * literal and its resolved twin are {@code equals} while hashing differently, exactly as a
   * hostname and one of its IPs are. The consequence named above is reachable that way: {@code
   * ContactPoints#merge} de-duplicates through a {@code HashSet} and logs {@code "Duplicate contact
   * point"} on a rejected add, so a programmatic resolved {@code 1.2.3.4:9042} alongside a
   * config-file {@code "1.2.3.4:9042"} lands in two buckets, is kept twice, and warns about
   * nothing. Left as it is with the rest of this method -- see the issue below.
   *
   * <p>One more thing the gate gets wrong, in the other direction: {@code carriesName} calls the
   * JDK's shorthand literal forms names, because Guava's {@code isInetAddress} requires four dotted
   * parts. A contact point written {@code 127.1:9042} works end to end -- {@code
   * InetAddress.getAllByName("127.1")} answers {@code /127.0.0.1} -- but the first mixed comparison
   * against it logs the warning and burns the once-per-JVM latch, after which the hostname case the
   * canary exists for is never reported. {@code ChannelFactory#materializeLiteral} documents the
   * same misclassification. Widening the predicate is deferred: it is the shared grammar behind
   * {@code reattachHostname} and {@code materializeLiteral}, and those have to keep accepting
   * exactly the same strings.
   *
   * <p>The branch exists for {@link
   * com.datastax.oss.driver.api.core.metadata.Metadata#findNode(EndPoint)}, whose caller may hold
   * either form. The two forms do meet, under an {@code AddressTranslator} that hands back a name
   * -- {@code SubnetAddressTranslator} does, under its default {@code resolve-addresses = false},
   * and so now do {@code FixedHostNameAddressTranslator} and {@code
   * Ec2MultiRegionAddressTranslator}, which had to stop resolving so that a proxy name with several
   * A-records fails over -- because the control node's endpoint is resolved while its peers' are
   * not. Three driver-internal callers reach it that way, each on a thread worth naming:
   *
   * <ul>
   *   <li>{@code DefaultSchemaQueriesFactory} looks the channel's endpoint up on every schema
   *       refresh, on the <b>control channel's I/O loop</b> ({@code
   *       MetadataManager#startSchemaRequest} continues the agreement check with a plain {@code
   *       whenComplete}). A miss is not fatal -- the factory falls back to an arbitrary node -- but
   *       it is a lookup per node per DDL.
   *   <li>{@code DefaultTopologyMonitor#refreshNode}'s control-node short-circuit, on {@code
   *       MetadataManager}'s <b>admin thread</b>, once per node coming up. Under a translator that
   *       gives every node one name this can also answer "equal" for an unrelated node and skip
   *       that node's refresh; pre-PR it answered equal for <i>every</i> node, so the wrong answer
   *       is not new, only the lookup is.
   *   <li>{@code OptionalLocalDcHelper#inferDcFromControlConnection}, on the <b>policy-init admin
   *       thread</b>, once per node. Here a miss is silent and consequential: nothing matches, the
   *       candidate set stays empty and datacenter inference returns {@link
   *       java.util.Optional#empty()} rather than guessing.
   * </ul>
   *
   * <p>Replacing all three with {@code PinnableEndPoint#sameIdentity}, as this change did wherever
   * it controls the comparison, is deferred. It warns once per JVM meanwhile, as a canary for what
   * still depends on this -- see https://github.com/scylladb/java-driver/issues/1006.
   */
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DefaultEndPoint) {
      InetSocketAddress thisAddress = this.address;
      InetSocketAddress thatAddress = ((DefaultEndPoint) other).address;
      // If only one of the addresses is unresolved, resolve it. Otherwise (both resolved or both
      // unresolved), compare as-is.
      if (thisAddress.isUnresolved() != thatAddress.isUnresolved()) {
        if (AddressUtils.carriesName(thisAddress.isUnresolved() ? thisAddress : thatAddress)) {
          warnAboutMixedComparison(thisAddress, thatAddress);
        }
        if (thisAddress.isUnresolved()) {
          thisAddress = new InetSocketAddress(thisAddress.getHostName(), thisAddress.getPort());
        } else {
          thatAddress = new InetSocketAddress(thatAddress.getHostName(), thatAddress.getPort());
        }
      }
      return thisAddress.equals(thatAddress);
    } else {
      return false;
    }
  }

  private static void warnAboutMixedComparison(InetSocketAddress one, InetSocketAddress other) {
    if (LOGGED_MIXED_COMPARISON_WARNING.compareAndSet(false, true)) {
      LOG.warn(
          "Compared an unresolved host name against a resolved endpoint address ({} vs {}). This"
              + " performs a DNS lookup on the calling thread, only compares the first address the"
              + " name maps to, and does not agree with hashCode(); see"
              + " https://github.com/scylladb/java-driver/issues/1006. This message is logged once.",
          one,
          other);
    }
  }

  @Override
  public int hashCode() {
    return address.hashCode();
  }

  @Override
  public String toString() {
    // Deliberately identical for a pinned copy: see PinnableEndPoint. Which IP a given connection
    // landed on is in the channel's own toString(), which Netty builds from the actual remote
    // address.
    return address.toString();
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    return metricPrefix;
  }

  private static String buildMetricPrefix(InetSocketAddress address) {
    String hostString = address.getHostString();
    if (hostString == null) {
      throw new IllegalArgumentException(
          "Could not extract a host string from provided address " + address);
    }
    // Append the port since Cassandra 4 supports nodes with different ports
    return hostString.replace('.', '_') + ':' + address.getPort();
  }
}
