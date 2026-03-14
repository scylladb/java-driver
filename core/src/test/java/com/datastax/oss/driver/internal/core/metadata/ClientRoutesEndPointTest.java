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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.io.UncheckedIOException;
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
  public void should_wrap_io_exceptions_in_unchecked_io_exception() throws UnknownHostException {
    UUID hostId = UUID.randomUUID();
    when(topologyMonitor.resolve(hostId)).thenThrow(new UnknownHostException("no-such-host"));

    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThatThrownBy(ep::resolve)
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseInstanceOf(UnknownHostException.class);
  }

  @Test
  public void should_reflect_route_changes_on_subsequent_resolve() throws UnknownHostException {
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

  // ---- toString() ---------------------------------------------------------

  @Test
  public void should_return_host_id_as_string() {
    UUID hostId = UUID.randomUUID();
    ClientRoutesEndPoint ep =
        new ClientRoutesEndPoint(topologyMonitor, hostId, null, fallbackEndPoint);

    assertThat(ep.toString()).isEqualTo("ClientRoutesEndPoint(" + hostId + ")");
  }
}
