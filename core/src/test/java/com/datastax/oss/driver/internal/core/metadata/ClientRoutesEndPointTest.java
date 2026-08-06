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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import io.netty.channel.local.LocalAddress;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClientRoutesEndPointTest {

  @Mock private ClientRoutesTopologyMonitor topologyMonitor;
  @Mock private EndPoint fallbackEndPoint;

  // ---- resolve() ----------------------------------------------------------

  @Test
  public void should_resolve_via_topology_monitor() throws UnknownHostException {
    UUID hostId = UUID.randomUUID();
    InetSocketAddress expected = new InetSocketAddress("127.0.0.1", 9042);
    when(topologyMonitor.resolve(hostId)).thenReturn(expected);

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.resolve()).isEqualTo(expected);
  }

  @Test
  public void should_fallback_when_resolve_returns_null() throws UnknownHostException {
    UUID hostId = UUID.randomUUID();
    InetSocketAddress fallbackAddr = new InetSocketAddress("10.0.0.1", 9042);
    when(topologyMonitor.resolve(hostId)).thenReturn(null);
    when(fallbackEndPoint.resolve()).thenReturn(fallbackAddr);

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.resolve()).isEqualTo(fallbackAddr);
  }

  @Test
  public void should_return_the_route_address_unresolved() {
    // The route hostname is handed over unresolved on purpose: ChannelFactory resolves it through
    // Netty's AddressResolverGroup, so a custom resolver applies to client routes too and no DNS
    // lookup runs on the admin event loop that connect() is called from.
    UUID hostId = UUID.randomUUID();
    InetSocketAddress route = InetSocketAddress.createUnresolved("route.example.com", 9042);
    when(topologyMonitor.resolve(hostId)).thenReturn(route);

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.resolve()).isSameAs(route);
    assertThat(((InetSocketAddress) ep.resolve()).isUnresolved()).isTrue();
  }

  @Test
  public void should_reflect_route_changes_on_subsequent_resolve() {
    UUID hostId = UUID.randomUUID();
    InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 9042);
    InetSocketAddress addr2 = new InetSocketAddress("10.0.0.1", 9043);
    when(topologyMonitor.resolve(hostId)).thenReturn(addr1);

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.resolve()).isEqualTo(addr1);

    // Simulate route update in the topology monitor
    when(topologyMonitor.resolve(hostId)).thenReturn(addr2);

    assertThat(ep.resolve()).isEqualTo(addr2);
  }

  // ---- pinTo() ------------------------------------------------------------

  @Test
  public void pin_to_should_stop_consulting_the_topology_monitor() {
    UUID hostId = UUID.randomUUID();
    InetSocketAddress pinnedTo = new InetSocketAddress("127.0.0.1", 9042);

    ClientRoutesEndPoint original =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);
    EndPoint pinned = original.pinTo(pinnedTo);

    assertThat(pinned.resolve()).isEqualTo(pinnedTo);
    // No lookup at all -- that is the point: DefaultTopologyMonitor#savePort and the SSL factories
    // read the channel's endpoint, and must not trigger a blocking re-resolution there.
    verify(topologyMonitor, never()).resolve(hostId);
    // Identity is keyed off the host id, so the pinned copy is still the same node.
    assertThat(pinned).isEqualTo(original);
    assertThat(original).isEqualTo(pinned);
    assertThat(pinned.asMetricPrefix()).isEqualTo(original.asMetricPrefix());
  }

  @Test
  public void pin_to_should_return_same_instance_when_already_pinned_to_that_address() {
    ClientRoutesEndPoint original =
        new ClientRoutesEndPoint(topologyMonitor, UUID.randomUUID(), null, fallbackEndPoint);
    InetSocketAddress pinnedTo = new InetSocketAddress("127.0.0.1", 9042);

    EndPoint pinned = original.pinTo(pinnedTo);

    assertThat(((ClientRoutesEndPoint) pinned).pinTo(pinnedTo)).isSameAs(pinned);
  }

  @Test
  public void pin_to_should_be_a_no_op_for_an_unresolved_address() {
    // resolve() hands out the route's hostname unresolved, and ChannelFactory passes an address
    // straight back when the user disabled the resolver or a custom one declines it -- so that
    // hostname can come back here. Pinning it would freeze the endpoint on a name that still
    // re-expands on every connect, and silence the route lookup for good, since resolve()
    // short-circuits once pinned. Both siblings return `this` for the same input.
    UUID hostId = UUID.randomUUID();
    InetSocketAddress route = InetSocketAddress.createUnresolved("route.example.com", 9042);
    when(topologyMonitor.resolve(hostId)).thenReturn(route);
    ClientRoutesEndPoint endPoint =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(endPoint.pinTo(route)).isSameAs(endPoint);
    // Still asking the monitor, which is what would have been lost.
    assertThat(endPoint.resolve()).isEqualTo(route);
    verify(topologyMonitor, atLeastOnce()).resolve(hostId);
  }

  @Test
  public void pin_to_should_be_a_no_op_for_a_non_inet_address() {
    // Mirror DefaultEndPoint: an address that cannot be held in an InetSocketAddress field (e.g.
    // the local transport used by unit tests) skips pinning rather than failing the connection.
    ClientRoutesEndPoint endPoint =
        new ClientRoutesEndPoint(topologyMonitor, UUID.randomUUID(), null, fallbackEndPoint);

    assertThat(endPoint.pinTo(new LocalAddress("some-id"))).isSameAs(endPoint);
  }

  // ---- equals / hashCode --------------------------------------------------

  @Test
  public void should_be_equal_when_same_host_id() {
    UUID hostId = UUID.randomUUID();
    ClientRoutesEndPoint ep1 =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);
    ClientRoutesEndPoint ep2 =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep1).isEqualTo(ep2);
    assertThat(ep1.hashCode()).isEqualTo(ep2.hashCode());
  }

  @Test
  public void should_not_be_equal_when_different_host_id() {
    ClientRoutesEndPoint ep1 =
        new ClientRoutesEndPoint(topologyMonitor, UUID.randomUUID(), null, fallbackEndPoint);
    ClientRoutesEndPoint ep2 =
        new ClientRoutesEndPoint(topologyMonitor, UUID.randomUUID(), null, fallbackEndPoint);

    assertThat(ep1).isNotEqualTo(ep2);
  }

  @Test
  public void should_not_be_equal_to_non_client_routes_endpoint() {
    UUID hostId = UUID.randomUUID();
    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep).isNotEqualTo("not an endpoint");
    assertThat(ep).isNotEqualTo(null);
  }

  // ---- asMetricPrefix() ---------------------------------------------------

  @Test
  public void should_use_host_id_as_metric_prefix_when_address_is_null() {
    UUID hostId = UUID.randomUUID();
    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.asMetricPrefix()).isEqualTo(hostId.toString());
  }

  @Test
  public void should_format_ipv4_metric_prefix() throws Exception {
    UUID hostId = UUID.randomUUID();
    InetAddress ipv4 = InetAddress.getByAddress(new byte[] {10, 0, 0, 1});
    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, ipv4, fallbackEndPoint);

    assertThat(ep.asMetricPrefix()).isEqualTo("10_0_0_1_" + hostId);
  }

  @Test
  public void should_format_ipv6_metric_prefix() throws Exception {
    UUID hostId = UUID.randomUUID();
    InetAddress ipv6 =
        InetAddress.getByAddress(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, ipv6, fallbackEndPoint);

    // IPv6 keeps colons (consistent with DefaultEndPoint), dots replaced by underscores
    assertThat(ep.asMetricPrefix()).isEqualTo("0:0:0:0:0:0:0:1_" + hostId);
  }

  // ---- addressesAreInterchangeable() --------------------------------------

  @Test
  public void should_report_a_routes_addresses_as_interchangeable() {
    // The proxy routes by server name, so every address the route's hostname maps to reaches this
    // same node and connections may be spread across them.
    UUID hostId = UUID.randomUUID();
    when(topologyMonitor.resolve(hostId))
        .thenReturn(InetSocketAddress.createUnresolved("route.example.com", 9042));

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.addressesAreInterchangeable()).isTrue();
  }

  @Test
  public void should_defer_to_the_fallback_when_there_is_no_route() {
    // Without a route, resolve() is the fallback endpoint's answer, so whether *those* addresses
    // are
    // interchangeable is not this class's to claim. The usual fallback is a DefaultEndPoint built
    // from a translated broadcast address, which says no -- and an AddressTranslator that returns a
    // name (SubnetAddressTranslator does by default) is exactly the case where spreading would land
    // one node's channels on different hosts while routing and metrics attribute them to one node.
    UUID hostId = UUID.randomUUID();
    when(topologyMonitor.resolve(hostId)).thenReturn(null);
    EndPoint staticFallback =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("peer.example.com", 9042));

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, staticFallback);

    assertThat(ep.addressesAreInterchangeable()).isFalse();
  }

  @Test
  public void should_defer_to_a_fallback_that_is_itself_interchangeable() {
    UUID hostId = UUID.randomUUID();
    when(topologyMonitor.resolve(hostId)).thenReturn(null);
    EndPoint sniFallback =
        new SniEndPoint(InetSocketAddress.createUnresolved("proxy.example.com", 9042), "server");

    ClientRoutesEndPoint ep = new ClientRoutesEndPoint(topologyMonitor, hostId, null, sniFallback);

    assertThat(ep.addressesAreInterchangeable()).isTrue();
  }

  @Test
  public void should_not_throw_from_addresses_are_interchangeable_when_the_monitor_is_closed() {
    // Same reasoning as resolve(): a closed monitor means "no route", not a failed connect. This is
    // asked on the connect path, so throwing here would fail the attempt outright.
    UUID hostId = UUID.randomUUID();
    when(topologyMonitor.resolve(hostId))
        .thenThrow(new IllegalStateException("Topology monitor is closed"));

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(
            topologyMonitor,
            hostId,
            null,
            new DefaultEndPoint(InetSocketAddress.createUnresolved("peer.example.com", 9042)));

    assertThat(ep.addressesAreInterchangeable()).isFalse();
  }

  // ---- toString() ---------------------------------------------------------

  @Test
  public void should_return_host_id_as_string() {
    UUID hostId = UUID.randomUUID();
    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.toString()).isEqualTo("ClientRoutesEndPoint(" + hostId + ")");
  }
}
