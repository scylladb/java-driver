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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class AddressUtils {

  public static Set<InetSocketAddress> extract(String address, boolean resolve) {
    int separator = address.lastIndexOf(':');
    if (separator < 0) {
      throw new IllegalArgumentException("expecting format host:port");
    }

    String host = address.substring(0, separator);
    String portString = address.substring(separator + 1);
    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("expecting port to be a number, got " + portString, e);
    }
    if (!resolve) {
      return ImmutableSet.of(InetSocketAddress.createUnresolved(host, port));
    } else {
      InetAddress[] inetAddresses;
      try {
        inetAddresses = InetAddress.getAllByName(host);
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
      Set<InetSocketAddress> result = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        result.add(new InetSocketAddress(inetAddress, port));
      }
      return result;
    }
  }

  /**
   * Whether {@code address} denotes a host <b>name</b>, as opposed to an IP address written out in
   * literal form.
   *
   * <p>The distinction matters wherever a name is treated as something that can be resolved — and
   * re-resolved — while a literal is taken as the final answer. Both forms can appear resolved or
   * unresolved, so neither {@link InetSocketAddress#isUnresolved()} nor the presence of an {@link
   * InetAddress} tells them apart.
   *
   * <p>Performs no lookup of any kind.
   *
   * <p><b>The answer is only stable for an {@linkplain InetSocketAddress#isUnresolved() unresolved}
   * address.</b> A resolved one has no host string of its own: {@code getHostString()} renders the
   * {@link InetAddress}'s <i>cached</i> {@code hostName} field, which is empty until the first time
   * anything calls {@code getHostName()} on that instance and holds a reverse-DNS name afterwards —
   * and {@code DefaultSslEngineFactory} calls it while building an engine, under the default {@code
   * advanced.ssl-engine-factory.allow-dns-reverse-lookup-san = true}. So a resolved address built
   * over an IP literal answers {@code false} before the first TLS handshake to that node and {@code
   * true} after it, for the very same instance. Callers that must not flip with it either restrict
   * themselves to unresolved addresses (as {@code ChannelFactory#reattachHostname} does) or strip
   * the label first (see {@link #stripHostName}).
   *
   * <p>On the unresolved branch, a scoped IPv6 <i>literal</i> (say {@code fe80::1%eth0}) and a
   * bracketed one ({@code [2001:db8::5]}, the spelling {@link #extract} preserves) are both
   * correctly reported as literals. Callers that go on to parse the string must therefore be ready
   * for a zone <i>and</i> for brackets — {@link
   * com.datastax.oss.driver.shaded.guava.common.net.InetAddresses#forString} rejects the bracketed
   * form outright, and resolves a zone against the local interfaces, throwing {@link
   * IllegalArgumentException} when no interface matches (see {@code
   * ChannelFactory#reattachHostname}, which strips both before parsing).
   */
  public static boolean carriesName(InetSocketAddress address) {
    String hostString = address.getHostString();
    if (hostString == null) {
      return false;
    }
    // A resolved address is compared against the literal its own bytes produce, which is cheaper
    // and
    // stricter than parsing; an unresolved one has no bytes, so its string has to be parsed.
    InetAddress ip = address.getAddress();
    return ip != null ? !hostString.equals(ip.getHostAddress()) : !isLiteral(hostString);
  }

  /**
   * Whether an unresolved address's host string is an IP address in literal form.
   *
   * <p>Two spellings count. {@code InetAddresses#isInetAddress} accepts the bare form, zone
   * included ({@code 2001:db8::5}, {@code fe80::1%eth0}); only {@code
   * InetAddresses#isUriInetAddress} accepts the bracketed URI form ({@code [2001:db8::5]}).
   *
   * <p>The bracketed form is not hypothetical. {@link #extract} splits a contact point on its
   * <i>last</i> colon and keeps whatever precedes it verbatim, so {@code [2001:db8::5]:9042} yields
   * the host string {@code [2001:db8::5]}, and {@code InetAddress.getAllByName} accepts that
   * spelling — the configuration works end to end. Testing the bare form alone would report it as a
   * host name, which costs on both sides: {@code DefaultEndPoint#equals} would fire the mixed
   * unresolved/resolved warning and burn its once-per-JVM canary on a message whose stated hazards
   * are all false for a literal, and {@code ChannelFactory#reattachHostname} would take the
   * name-wins branch and relabel even a resolver-redirected candidate, bypassing the byte-equality
   * guard the literal branch exists for.
   */
  private static boolean isLiteral(String hostString) {
    return InetAddresses.isInetAddress(hostString) || InetAddresses.isUriInetAddress(hostString);
  }

  /**
   * Parses {@code hostString} as an IP address literal -- the same two spellings {@link #isLiteral}
   * recognises -- or returns {@code null} if it is not one. Performs no lookup.
   *
   * <p>Here, beside the recognition, because the two have to accept the same strings and nothing
   * but proximity makes them: a caller that recognises a literal with {@link #carriesName} and then
   * parses it with a grammar of its own has two grammars to keep in agreement, and only a comment
   * saying so.
   *
   * <p>Neither Guava predicate has a matching parser. {@code InetAddresses#forString} rejects the
   * bracketed URI form outright, and it resolves an IPv6 zone against the local interfaces,
   * throwing when the zone names none -- it rejects even {@code fe80::1%lo} on a host that has an
   * {@code lo} interface. So the brackets come off and the zone is split away before the parse.
   *
   * <p>Brackets first, then the zone: {@link #extract} splits a contact point on its <i>last</i>
   * colon and keeps them, so {@code [fe80::1%eth0]:9042} arrives here as {@code [fe80::1%eth0]} --
   * splitting on {@code '%'} before unwrapping would leave the closing bracket inside the zone and
   * the opening one inside the literal, and neither part would parse.
   *
   * <p>The zone is <b>dropped</b> rather than resolved, so the result carries the literal's bytes
   * and nothing else. A caller comparing it is therefore scope-blind -- which {@link
   * InetAddress#equals} is anyway -- and one that needs the zone should keep the original string.
   */
  @Nullable
  public static InetAddress parseLiteral(String hostString) {
    String bare = hostString;
    if (bare.length() > 2 && bare.charAt(0) == '[' && bare.charAt(bare.length() - 1) == ']') {
      bare = bare.substring(1, bare.length() - 1);
    }
    int zoneSeparator = bare.indexOf('%');
    String literalPart = zoneSeparator < 0 ? bare : bare.substring(0, zoneSeparator);
    try {
      return InetAddresses.forString(literalPart);
    } catch (IllegalArgumentException notALiteral) {
      return null;
    }
  }

  /**
   * Returns a copy of {@code ip} labelled with {@code hostName}, or with no label at all when
   * {@code hostName} is {@code null}, preserving an IPv6 zone if there is one.
   *
   * <p>{@link InetAddress#getByAddress(String, byte[])} cannot carry a zone, and dropping one would
   * change where the address actually points — a link-local address is only meaningful together
   * with its zone. {@link Inet6Address#getByAddress(String, byte[], int)} carries the zone as its
   * numeric id, which is what the connect itself goes on.
   *
   * <p>That overload is used <b>only</b> for an address that really has a zone. {@code
   * Inet6AddressHolder.init} treats any {@code scope_id >= 0} as zone-present, so handing it the
   * {@code 0} an unscoped address reports produces a spurious {@code %0} suffix — verified on JDK
   * 11.0.30: {@code Inet6Address.getByAddress("db.example.com", bytes, 0).getHostAddress()} is
   * {@code "2001:db8:0:0:0:0:0:5%0"}, while the two-arg overload yields the clean form. That suffix
   * would reach node metric tags through an endpoint's {@code toString()}, and would break {@link
   * #carriesName}'s resolved branch, which needs the host string and the literal to compare equal.
   *
   * <p>The sibling overload taking a {@link java.net.NetworkInterface} is deliberately not used: it
   * re-derives the numeric zone by searching that interface for an address of the same local type,
   * and throws {@code UnknownHostException("no scope_id found")} when it finds none — so it can
   * fail for an address that was legitimately built from an interface in the first place. All that
   * is lost by going numeric is the interface <i>name</i>, which surfaces in {@code toString()} and
   * nowhere else.
   *
   * <p>Performs no lookup.
   */
  public static InetAddress withHostName(@Nullable String hostName, InetAddress ip)
      throws UnknownHostException {
    if (ip instanceof Inet6Address) {
      int scopeId = ((Inet6Address) ip).getScopeId();
      if (scopeId != 0) {
        return Inet6Address.getByAddress(hostName, ip.getAddress(), scopeId);
      }
    }
    return InetAddress.getByAddress(hostName, ip.getAddress());
  }

  /**
   * Returns {@code address} with its host-name label removed, so that {@code getHostString()}
   * reports the IP literal and cannot start reporting something else later, or {@code null} if
   * {@code address} carries no {@link InetAddress} to strip.
   *
   * <p>Anything deriving a durable <b>identity</b> from a resolved address's host string has to do
   * this first: that string renders a mutable field on the shared {@code InetAddress} (see {@link
   * #carriesName}), so an identity keyed off it moves the first time the node is connected to over
   * TLS. Stripping makes the identity a function of the address bytes alone.
   *
   * <p>Performs no lookup.
   */
  @Nullable
  public static InetSocketAddress stripHostName(InetSocketAddress address) {
    InetAddress ip = address.getAddress();
    if (ip == null) {
      return null;
    }
    try {
      return new InetSocketAddress(withHostName(null, ip), address.getPort());
    } catch (UnknownHostException impossible) {
      // getByAddress only rejects illegal byte lengths, and these bytes come from a real
      // InetAddress.
      return null;
    }
  }
}
