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
package com.datastax.oss.driver.api.core.metadata;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.SocketAddress;

/**
 * Encapsulates the information needed to open connections to a node.
 *
 * <p>By default, the driver assumes plain TCP connections, and this is just a wrapper around an
 * {@link java.net.InetSocketAddress}. However, more complex deployment scenarios might use a custom
 * implementation that contains additional information; for example, if the nodes are accessed
 * through a proxy with SNI routing, an SNI server name is needed in addition to the proxy address.
 */
public interface EndPoint {

  /**
   * Resolves this instance to the socket address connections should be opened to.
   *
   * <p>This will be called each time the driver opens a new connection to the node. The returned
   * address cannot be null.
   *
   * <p><b>Returning a hostname is fine, and is how multi-address support works.</b> The returned
   * address need not be resolved: an {@linkplain java.net.InetSocketAddress#isUnresolved()
   * unresolved} {@link java.net.InetSocketAddress} is expanded by the driver to <b>every</b>
   * address the name maps to, and each one is tried in turn until a connection succeeds. That is
   * what {@code DefaultEndPoint} does for contact points backed by a hostname, so a single
   * unreachable IP behind a multi-record name no longer fails the connection.
   *
   * <p><b>Implementations must not resolve names themselves, and must not block.</b> The driver
   * calls this from its admin event loop, and it performs the expansion through Netty's configured
   * {@code AddressResolverGroup} — the same resolver an unresolved address reaches when it is
   * handed to {@code Bootstrap.connect()}. Looking the name up here instead (for example with
   * {@link java.net.InetAddress#getAllByName(String)}) would both block that loop and bypass a
   * custom resolver installed via {@code NettyOptions#afterBootstrapInitialized(Bootstrap)}.
   *
   * <p><b>Callers must not assume the returned address is resolved.</b> It is for a node discovered
   * from {@code system.peers} (built from that node's physical broadcast RPC address) and for the
   * node the control connection is on (bound to the address that connection reached). It is
   * <b>not</b> for a node reached through the Cloud SNI proxy, or through a cloud private-endpoint
   * client route: there the address is the configured hostname, and {@link
   * java.net.InetSocketAddress#getAddress()} returns {@code null}. Read the host with {@link
   * java.net.InetSocketAddress#getHostString()}, which yields whichever of the two the address
   * carries and never triggers a reverse lookup.
   *
   * @apiNote <b>Timeout note:</b> when a name expands to several addresses they are tried in
   *     sequence, so the worst-case time before the node is declared unreachable is N times a full
   *     attempt — and an attempt is more than a connect. Each address that accepts the TCP
   *     connection then runs the init handshake, whose steps each arm their own {@code
   *     advanced.connection.init-query-timeout}; those add up rather than sharing one deadline. An
   *     address that stalls after accepting the connection can therefore burn {@code
   *     advanced.connection.connect-timeout} plus several times {@code
   *     advanced.connection.init-query-timeout} on its own. In practice DNS round-robin entries
   *     have only a small number of records, so this is rarely a concern, but it is worth bearing
   *     in mind when configuring timeouts — note also that session initialization has no overall
   *     deadline of its own.
   */
  @NonNull
  SocketAddress resolve();

  /**
   * Returns an alternate string representation for use in node-level metric names.
   *
   * <p>Because metrics names are path-like, dot-separated strings, raw IP addresses don't make very
   * good identifiers. So this method will typically replace the dots by another character, for
   * example {@code 127_0_0_1_9042}.
   */
  @NonNull
  String asMetricPrefix();
}
