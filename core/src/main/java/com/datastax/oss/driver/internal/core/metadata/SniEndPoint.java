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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;

public class SniEndPoint implements PinnableEndPoint {

  private final InetSocketAddress proxyAddress;
  private final String serverName;

  /**
   * The proxy IP this endpoint has been {@linkplain #pinTo(SocketAddress) pinned} to, or {@code
   * null} if it is not pinned. Deliberately excluded from {@link #equals} and {@link #hashCode}: a
   * pinned copy denotes the same node as the original.
   */
  @Nullable private final InetSocketAddress pinnedAddress;

  /**
   * @param proxyAddress the address of the proxy. Stored {@linkplain
   *     InetSocketAddress#isUnresolved() unresolved}, whatever form it was supplied in, so that the
   *     driver expands a proxy hostname to all of its A-records at connection time and tries each
   *     of them — see {@link #storeUnresolved}.
   * @param serverName the SNI server name. In the context of Cloud, this is the string
   *     representation of the host id.
   */
  public SniEndPoint(InetSocketAddress proxyAddress, String serverName) {
    this(proxyAddress, serverName, null);
  }

  private SniEndPoint(
      InetSocketAddress proxyAddress,
      String serverName,
      @Nullable InetSocketAddress pinnedAddress) {
    this.proxyAddress =
        storeUnresolved(Objects.requireNonNull(proxyAddress, "SNI address cannot be null"));
    this.serverName = Objects.requireNonNull(serverName, "SNI Server name cannot be null");
    this.pinnedAddress = pinnedAddress;
  }

  /**
   * Stores the proxy address unresolved, whatever form it arrived in.
   *
   * <p>{@link #resolve()} hands the stored address to the connection layer as-is, and only an
   * unresolved one gets expanded and re-expanded there. A proxy hostname supplied already resolved
   * would therefore stay bound to whichever single IP its lookup happened to return, for the life
   * of the session: no spreading across the proxy's A-records, no fallback when that one IP stops
   * answering, and no pick-up of a DNS change. That is a real possibility for a hostname handed to
   * {@link
   * com.datastax.oss.driver.api.core.session.SessionBuilder#withCloudProxyAddress(InetSocketAddress)},
   * because the ordinary {@code InetSocketAddress(String, int)} constructor resolves eagerly.
   * ({@code CloudConfigFactory}, the usual path, already builds an unresolved address.)
   *
   * <p>An address that is already an IP literal is stored unresolved too, even though it has
   * nothing to expand, because that is what makes this endpoint's identity <b>stable</b>. A
   * resolved address's {@code getHostString()} is not fixed: it starts out as the IP literal and
   * begins reporting the reverse-DNS name as soon as anything calls {@code getHostName()} on the
   * underlying {@code InetAddress} — which {@code SniSslEngineFactory#newSslEngine} does, on this
   * very instance, under the default {@code
   * advanced.ssl-engine-factory.allow-dns-reverse-lookup-san = true}. Keying {@link #equals} and
   * {@link #asMetricPrefix()} off a string that can change underneath them would move a node's
   * metrics mid-session and make endpoints built before and after the first TLS handshake compare
   * unequal. An unresolved address has no such field to fill in: its host string is fixed at
   * construction, and {@code getHostName()} on it performs no lookup. SNI's reverse lookup still
   * happens, on the {@linkplain #pinTo(SocketAddress) pinned} copy that carries the IP the channel
   * actually reached.
   *
   * <p>What this cannot defend against is an instance the caller polluted before handing it over,
   * i.e. called {@code getHostName()} on themselves.
   *
   * <p>Normalizing here rather than at the call site keeps every {@code SniEndPoint} built from the
   * same proxy comparable — {@link #equals} keys on this field — and matches what this endpoint did
   * before resolution moved to the connection layer, when it re-resolved the proxy hostname on
   * every {@code resolve()} call.
   */
  private static InetSocketAddress storeUnresolved(InetSocketAddress proxyAddress) {
    return proxyAddress.isUnresolved()
        ? proxyAddress
        : InetSocketAddress.createUnresolved(proxyAddress.getHostString(), proxyAddress.getPort());
  }

  public String getServerName() {
    return serverName;
  }

  /**
   * Returns the proxy address connections should be opened to.
   *
   * <p>Unpinned, this is the stored proxy address as-is — always unresolved (see {@link
   * #storeUnresolved}), which {@link com.datastax.oss.driver.internal.core.channel.ChannelFactory}
   * expands to every proxy A-record, trying each in turn — so a single unreachable proxy IP no
   * longer fails the connection. Re-resolving here instead would block whichever event loop called
   * us, and would bypass a custom Netty resolver.
   *
   * <p>Once {@linkplain #pinTo(SocketAddress) pinned} this returns that one proxy IP. That is what
   * {@link com.datastax.oss.driver.internal.core.ssl.SniSslEngineFactory#newSslEngine} sees: it
   * runs inside Netty's channel initializer, so it gets the exact IP the channel is connected to
   * without a lookup on the event loop.
   */
  @NonNull
  @Override
  public InetSocketAddress resolve() {
    return pinnedAddress != null ? pinnedAddress : proxyAddress;
  }

  @NonNull
  @Override
  public EndPoint pinTo(@NonNull SocketAddress resolvedAddress) {
    Objects.requireNonNull(resolvedAddress, "resolvedAddress cannot be null");
    // Mirrors DefaultEndPoint and ClientRoutesEndPoint: an address this endpoint cannot hold in an
    // InetSocketAddress field skips pinning rather than failing the connection, and so does an
    // unresolved one. resolve() hands the proxy address over unresolved, and ChannelFactory passes
    // it straight back when the user disabled the resolver or a custom one declines it; pinning
    // that would freeze this endpoint on a name that must re-expand on every connect -- no address
    // stability gained, and the proxy's A-record fallback silenced for good.
    if (!(resolvedAddress instanceof InetSocketAddress)
        || ((InetSocketAddress) resolvedAddress).isUnresolved()
        || resolvedAddress.equals(this.pinnedAddress)) {
      return this;
    }
    return new SniEndPoint(proxyAddress, serverName, (InetSocketAddress) resolvedAddress);
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code true}: the proxy routes by server name, so every one of its A-records reaches this
   * same node, and connections may be spread across them. That restores what this endpoint did
   * itself before resolution moved to the connection layer, when {@code resolve()} sorted the proxy
   * A-records and rotated through them on every call.
   */
  @Override
  public boolean addressesAreInterchangeable() {
    return true;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof SniEndPoint) {
      SniEndPoint that = (SniEndPoint) other;
      return this.proxyAddress.equals(that.proxyAddress) && this.serverName.equals(that.serverName);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(proxyAddress, serverName);
  }

  @Override
  public String toString() {
    // Deliberately identical for a pinned copy: see PinnableEndPoint. Which proxy IP a given
    // connection landed on is in the channel's own toString(), which Netty builds from the actual
    // remote address.
    return proxyAddress + ":" + serverName;
  }

  @NonNull
  @Override
  public String asMetricPrefix() {
    String hostString = proxyAddress.getHostString();
    if (hostString == null) {
      throw new IllegalArgumentException(
          "Could not extract a host string from provided proxy address " + proxyAddress);
    }
    return hostString.replace('.', '_') + ':' + proxyAddress.getPort() + '_' + serverName;
  }
}
