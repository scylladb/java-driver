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
import static org.assertj.core.api.Assumptions.assumeThat;

import io.netty.channel.local.LocalAddress;
import java.net.InetSocketAddress;
import org.junit.Test;

public class SniEndPointTest {

  @Test
  public void resolve_returns_the_proxy_address_as_is_without_looking_it_up() {
    // The proxy address is a hostname (that is how CloudConfigFactory builds it) and this endpoint
    // must not resolve it: ChannelFactory expands it through Netty's AddressResolverGroup, which is
    // what makes a custom resolver apply to the SNI proxy too, and what keeps resolve() safe to
    // call
    // from an event loop -- SniSslEngineFactory#newSslEngine does exactly that.
    InetSocketAddress proxy = InetSocketAddress.createUnresolved("proxy.example.com", 9042);
    SniEndPoint endPoint = new SniEndPoint(proxy, "test-server-name");

    assertThat(endPoint.resolve()).isSameAs(proxy);
    assertThat(endPoint.resolve().isUnresolved()).isTrue();
  }

  @Test
  public void should_keep_a_resolved_proxy_hostname_unresolved() {
    // InetSocketAddress(String, int) resolves eagerly, so a hostname passed to
    // withCloudProxyAddress() arrives here already bound to one of its IPs. Storing it that way
    // would freeze every Cloud connection on that IP for the life of the session: resolve() hands
    // the stored address straight to the connection layer, which only expands unresolved ones.
    InetSocketAddress resolvedProxy = new InetSocketAddress("localhost", 9042);
    assertThat(resolvedProxy.isUnresolved()).isFalse();

    SniEndPoint endPoint = new SniEndPoint(resolvedProxy, "test-server-name");

    assertThat(endPoint.resolve().isUnresolved()).isTrue();
    assertThat(endPoint.resolve().getHostString()).isEqualTo("localhost");
    assertThat(endPoint.resolve().getPort()).isEqualTo(9042);
    // Normalization is unconditional, so endpoints built from either form of the same proxy still
    // denote the same node -- equals() keys on the stored address.
    assertThat(endPoint)
        .isEqualTo(
            new SniEndPoint(
                InetSocketAddress.createUnresolved("localhost", 9042), "test-server-name"));
    // The metric prefix is unaffected either way: it was already built from the host string.
    assertThat(endPoint.asMetricPrefix()).isEqualTo("localhost:9042_test-server-name");
  }

  @Test
  public void should_store_a_proxy_given_as_an_ip_address_unresolved_too() {
    // An IP literal has nothing to expand, but it is stored unresolved all the same, because that
    // is what makes this endpoint's identity immune to drift: see the test below.
    InetSocketAddress ipProxy = new InetSocketAddress("127.0.0.1", 9042);

    SniEndPoint endPoint = new SniEndPoint(ipProxy, "test-server-name");

    assertThat(endPoint.resolve().isUnresolved()).isTrue();
    assertThat(endPoint.resolve().getHostString()).isEqualTo("127.0.0.1");
    assertThat(endPoint.resolve().getPort()).isEqualTo(9042);
    assertThat(endPoint.asMetricPrefix()).isEqualTo("127_0_0_1:9042_test-server-name");
  }

  @Test
  public void identity_should_not_drift_when_the_source_address_grows_a_reverse_dns_name() {
    // SniSslEngineFactory#newSslEngine calls getHostName() on the address resolve() hands it, under
    // the default allow-dns-reverse-lookup-san = true. On a *resolved* address that caches the
    // reverse-DNS name on the underlying InetAddress, and getHostString() reports it from then on.
    // An endpoint keyed on such an instance would silently change its metric prefix mid-session,
    // moving a node's metrics; storing the address unresolved removes the field that changes.
    InetSocketAddress ipProxy = new InetSocketAddress("127.0.0.1", 9042);
    SniEndPoint endPoint = new SniEndPoint(ipProxy, "test-server-name");
    InetSocketAddress storedBefore = endPoint.resolve();

    // What the SSL engine factory does. 127.0.0.1 has a PTR record on essentially every machine, so
    // this really does change the source instance's host string. Assumed rather than asserted --
    // it is this test's precondition, not its subject, and a host without a loopback PTR entry (a
    // minimal container image, nsswitch without `files`) would otherwise report SniEndPoint as
    // broken over an environment fact. Same treatment as CloudTopologyMonitorTest and
    // AddressUtilsTest give it.
    ipProxy.getHostName();
    assumeThat(ipProxy.getHostString())
        .as("127.0.0.1 has no reverse-DNS name here, so the drift this test guards cannot occur")
        .isNotEqualTo("127.0.0.1");

    assertThat(endPoint.resolve()).isEqualTo(storedBefore);
    assertThat(endPoint.asMetricPrefix()).isEqualTo("127_0_0_1:9042_test-server-name");
    assertThat(endPoint).isEqualTo(new SniEndPoint(storedBefore, "test-server-name"));

    // And the driver no longer pollutes the address it holds in the first place: getHostName() on
    // an
    // unresolved address returns its stored host string without looking anything up, so the call
    // above cannot happen to *this* instance. (What the SSL engine actually sees is the pinned
    // copy,
    // whose address is excluded from equals() and asMetricPrefix() by contract.)
    assertThat(endPoint.resolve().getHostName()).isEqualTo("127.0.0.1");
    assertThat(endPoint.asMetricPrefix()).isEqualTo("127_0_0_1:9042_test-server-name");
  }

  @Test
  public void resolve_does_not_throw_for_unresolvable_proxy_hostname() {
    // No lookup happens here, so an unresolvable name only fails later, at connect time.
    SniEndPoint endPoint =
        new SniEndPoint(
            InetSocketAddress.createUnresolved("this-host-does-not-exist.invalid", 9042),
            "test-server-name");

    assertThat(endPoint.resolve().getHostString()).isEqualTo("this-host-does-not-exist.invalid");
  }

  @Test
  public void pin_to_should_make_resolve_return_the_connected_proxy_ip_and_preserve_identity() {
    // Pinning is what lets SniSslEngineFactory#newSslEngine -- which runs inside Netty's channel
    // initializer -- see the very proxy IP the channel is connected to, rather than the hostname or
    // another A-record.
    SniEndPoint original =
        new SniEndPoint(
            InetSocketAddress.createUnresolved("proxy.example.com", 9042), "test-server-name");
    InetSocketAddress pinnedTo = new InetSocketAddress("127.0.0.1", 9042);

    SniEndPoint pinned = (SniEndPoint) original.pinTo(pinnedTo);

    assertThat(pinned.resolve()).isEqualTo(pinnedTo);
    assertThat(pinned.resolve().isUnresolved()).isFalse();
    // The original is untouched.
    assertThat(original.resolve().isUnresolved()).isTrue();

    // The pinned copy still denotes the same node, down to every string it is identified by: the
    // tagging MetricIdGenerator tags node metrics with the endpoint's toString(), and nodes do
    // adopt
    // pinned copies.
    assertThat(pinned.getServerName()).isEqualTo(original.getServerName());
    assertThat(pinned.asMetricPrefix()).isEqualTo(original.asMetricPrefix());
    assertThat(pinned.toString()).isEqualTo(original.toString());
    assertThat(pinned).isEqualTo(original);
    assertThat(original).isEqualTo(pinned);
    assertThat(pinned.hashCode()).isEqualTo(original.hashCode());
  }

  @Test
  public void pin_to_should_return_same_instance_when_already_pinned_to_that_address() {
    SniEndPoint original =
        new SniEndPoint(
            InetSocketAddress.createUnresolved("proxy.example.com", 9042), "test-server-name");
    InetSocketAddress pinnedTo = new InetSocketAddress("127.0.0.1", 9042);

    SniEndPoint pinned = (SniEndPoint) original.pinTo(pinnedTo);

    assertThat(pinned.pinTo(pinnedTo)).isSameAs(pinned);
  }

  @Test
  public void pin_to_should_be_a_no_op_for_an_unresolved_address() {
    // ChannelFactory hands the address straight back when the user disabled Netty's resolver or a
    // custom one declines it. Pinning that would freeze resolve() on a name that must re-expand on
    // every connect, silencing the proxy's A-record fallback for good.
    SniEndPoint endPoint =
        new SniEndPoint(
            InetSocketAddress.createUnresolved("proxy.example.com", 9042), "test-server-name");

    assertThat(endPoint.pinTo(InetSocketAddress.createUnresolved("proxy.example.com", 9042)))
        .isSameAs(endPoint);
  }

  @Test
  public void pin_to_should_be_a_no_op_for_a_non_inet_address() {
    SniEndPoint endPoint =
        new SniEndPoint(
            InetSocketAddress.createUnresolved("proxy.example.com", 9042), "test-server-name");

    assertThat(endPoint.pinTo(new LocalAddress("test"))).isSameAs(endPoint);
  }
}
