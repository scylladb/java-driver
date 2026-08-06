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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.MockedDriverContextFactory;
import com.datastax.oss.driver.internal.core.metrics.AbstractMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.DefaultMetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.DropwizardNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class DefaultNodeTest {

  private final String uuidStr = "1e4687e6-f94e-432e-a792-216f89ef265f";
  private final UUID hostId = UUID.fromString(uuidStr);
  private final EndPoint endPoint = new DefaultEndPoint(new InetSocketAddress("localhost", 9042));

  @Test
  public void should_have_expected_string_representation() {

    DefaultNode node = new DefaultNode(endPoint, MockedDriverContextFactory.defaultDriverContext());
    node.hostId = hostId;

    String expected =
        String.format(
            "Node(endPoint=localhost/127.0.0.1:9042, hostId=1e4687e6-f94e-432e-a792-216f89ef265f, hashCode=%x)",
            node.hashCode());
    assertThat(node.toString()).isEqualTo(expected);
  }

  @Test
  public void should_have_expected_string_representation_if_hostid_is_null() {

    DefaultNode node = new DefaultNode(endPoint, MockedDriverContextFactory.defaultDriverContext());
    node.hostId = null;

    String expected =
        String.format(
            "Node(endPoint=localhost/127.0.0.1:9042, hostId=null, hashCode=%x)", node.hashCode());
    assertThat(node.toString()).isEqualTo(expected);
  }

  @Test
  public void should_adopt_a_newer_endpoint_that_only_differs_by_its_pinned_address() {
    // A PinnableEndPoint copy compares equal to the original -- pinnedAddress is excluded from
    // equals() by contract, so that a pinned copy still denotes the same node. setEndPoint() must
    // therefore not use equals() to decide whether to adopt it: the pinned address is the one every
    // subsequent connection to this node will use, so refusing the newer instance would freeze the
    // node on the first address it ever connected to, even after the control connection has moved
    // and told us about it.
    InternalDriverContext context = MockedDriverContextFactory.defaultDriverContext();
    DefaultNode node = new DefaultNode(endPoint, context);

    EndPoint pinnedToFirst =
        ((PinnableEndPoint) endPoint).pinTo(new InetSocketAddress("127.0.0.2", 9042));
    node.setEndPoint(pinnedToFirst, context);
    assertThat(node.getEndPoint()).isSameAs(pinnedToFirst);

    EndPoint pinnedToSecond =
        ((PinnableEndPoint) endPoint).pinTo(new InetSocketAddress("127.0.0.3", 9042));
    // Same node by equals(), different pinned address.
    assertThat(pinnedToSecond).isEqualTo(pinnedToFirst);
    node.setEndPoint(pinnedToSecond, context);

    assertThat(node.getEndPoint()).isSameAs(pinnedToSecond);
    assertThat(node.getEndPoint().resolve()).isEqualTo(new InetSocketAddress("127.0.0.3", 9042));
  }

  @Test
  public void should_keep_the_endpoint_instance_it_already_holds_when_nothing_would_differ() {
    // Every full topology refresh mints a fresh endpoint over a fresh InetSocketAddress for every
    // node, and for an unchanged node it denotes exactly what this one already does. Adopting it
    // would discard the reverse-DNS name that an earlier TLS handshake cached on the InetAddress
    // this
    // node holds (DefaultSslEngineFactory calls getHostName() under the default
    // allow-dns-reverse-lookup-san = true), so the next connection would repeat that blocking
    // lookup
    // on an I/O event loop -- once per node per refresh instead of once per node per session.
    InternalDriverContext context = MockedDriverContextFactory.defaultDriverContext();
    EndPoint held = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    DefaultNode node = new DefaultNode(held, context);

    EndPoint equivalent = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    assertThat(equivalent).isNotSameAs(held);
    node.setEndPoint(equivalent, context);

    assertThat(node.getEndPoint()).isSameAs(held);
  }

  @Test
  public void should_adopt_an_endpoint_of_a_different_kind_that_resolves_to_the_same_address() {
    // A dynamic endpoint (ClientRoutesEndPoint) resolves through its topology monitor on every
    // call,
    // so it is not interchangeable with a static one that happens to point at the same address
    // today.
    // The early return must not keep the static one in that case.
    InternalDriverContext context = MockedDriverContextFactory.defaultDriverContext();
    EndPoint stat1c = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    DefaultNode node = new DefaultNode(stat1c, context);

    EndPoint dynamic = new RenamingEndPoint("/127.0.0.1:9042");
    assertThat(dynamic.resolve()).isEqualTo(stat1c.resolve());
    assertThat(dynamic.asMetricPrefix()).isEqualTo(stat1c.asMetricPrefix());

    node.setEndPoint(dynamic, context);

    assertThat(node.getEndPoint()).isSameAs(dynamic);
  }

  @Test
  public void should_not_rebuild_the_metric_updater_for_a_pin_only_change() {
    // A pinned copy is identified exactly like the original -- same asMetricPrefix(), same
    // toString() -- so rebuilding would clear and re-register metrics under identical names, and
    // reset their values along the way.
    MetricsFactory metricsFactory = mock(MetricsFactory.class);
    InternalDriverContext context = contextWith(metricsFactory);
    NodeMetricUpdater first = mock(NodeMetricUpdater.class);
    NodeMetricUpdater second = mock(NodeMetricUpdater.class);
    when(metricsFactory.newNodeUpdater(any())).thenReturn(first, second);
    DefaultNode node = new DefaultNode(endPoint, context);
    assertThat(node.getMetricUpdater()).isSameAs(first);

    node.setEndPoint(
        ((PinnableEndPoint) endPoint).pinTo(new InetSocketAddress("127.0.0.2", 9042)), context);

    assertThat(node.getMetricUpdater()).isSameAs(first);
    verify(first, never()).clearMetrics();
  }

  @Test
  public void should_not_rebuild_the_metric_updater_when_only_the_endpoints_string_form_differs() {
    // toString() is not usable as an identity key because it is not stable across instances that
    // denote the same thing: DefaultEndPoint delegates it to InetSocketAddress, which renders
    // InetAddress's cached hostName field, and that field is filled in the first time anything
    // calls getHostName() -- which DefaultSslEngineFactory does, on this node's own endpoint
    // instance, under the default allow-dns-reverse-lookup-san = true. Keying on it would clear and
    // re-register every node's metrics on every topology refresh, since each refresh decodes a
    // fresh endpoint whose hostName has not been filled in yet.
    MetricsFactory metricsFactory = mock(MetricsFactory.class);
    InternalDriverContext context = contextWith(metricsFactory);
    NodeMetricUpdater first = mock(NodeMetricUpdater.class);
    NodeMetricUpdater second = mock(NodeMetricUpdater.class);
    when(metricsFactory.newNodeUpdater(any())).thenReturn(first, second);

    EndPoint beforeLookup = new RenamingEndPoint("/127.0.0.1:9042");
    EndPoint afterLookup = new RenamingEndPoint("host.example.com/127.0.0.1:9042");
    assertThat(beforeLookup.asMetricPrefix()).isEqualTo(afterLookup.asMetricPrefix());
    assertThat(beforeLookup.toString()).isNotEqualTo(afterLookup.toString());

    DefaultNode node = new DefaultNode(beforeLookup, context);
    assertThat(node.getMetricUpdater()).isSameAs(first);

    node.setEndPoint(afterLookup, context);

    assertThat(node.getMetricUpdater()).isSameAs(first);
    verify(first, never()).clearMetrics();
  }

  /** An endpoint that always reports the same metric prefix but renders differently. */
  private static class RenamingEndPoint implements EndPoint {
    private final String stringForm;

    RenamingEndPoint(String stringForm) {
      this.stringForm = stringForm;
    }

    @NonNull
    @Override
    public SocketAddress resolve() {
      return new InetSocketAddress("127.0.0.1", 9042);
    }

    @NonNull
    @Override
    public String asMetricPrefix() {
      return "127_0_0_1:9042";
    }

    @Override
    public String toString() {
      return stringForm;
    }
  }

  @Test
  public void should_rebuild_the_metric_updater_when_an_equal_endpoint_renames_the_metrics() {
    // An unresolved hostname and the address it resolves to compare *equal* (see
    // DefaultEndPoint#equals) but do not produce the same metric prefix. That is exactly what
    // happens when a contact-point node adopts the endpoint built from its system.local row, so
    // deciding on equals() alone would leave the node's metrics registered under the hostname while
    // asMetricPrefix() had moved on to the IP.
    MetricsFactory metricsFactory = mock(MetricsFactory.class);
    InternalDriverContext context = contextWith(metricsFactory);
    NodeMetricUpdater first = mock(NodeMetricUpdater.class);
    NodeMetricUpdater second = mock(NodeMetricUpdater.class);
    when(metricsFactory.newNodeUpdater(any())).thenReturn(first, second);

    EndPoint asHostname =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 9042));
    EndPoint asAddress = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    assertThat(asHostname).isEqualTo(asAddress);
    assertThat(asHostname.asMetricPrefix()).isNotEqualTo(asAddress.asMetricPrefix());

    DefaultNode node = new DefaultNode(asHostname, context);
    assertThat(node.getMetricUpdater()).isSameAs(first);

    node.setEndPoint(asAddress, context);

    assertThat(node.getMetricUpdater()).isSameAs(second);
    verify(first).clearMetrics();
  }

  @Test
  public void should_rebuild_the_metric_updater_without_wiping_the_metrics_it_registers() {
    // The two tests above use mocks, which cannot see *when* clearMetrics() runs relative to the
    // endpoint swap -- and that order is what decides whether the rebuild works. Dropwizard and
    // MicroProfile do not remember the ids they registered under: clearMetrics() recomputes each
    // one
    // from the node's endpoint as it stands at that moment. Clearing after the swap therefore
    // removes exactly the series the new updater has just registered, and leaves the old ones in
    // the
    // registry with nothing writing to them. So this one drives a real registry.
    MetricRegistry registry = new MetricRegistry();
    NodeMetric metric = DefaultNodeMetric.UNSENT_REQUESTS;
    InternalDriverContext context = dropwizardContext(registry, Collections.singleton(metric));

    EndPoint asHostname =
        new DefaultEndPoint(InetSocketAddress.createUnresolved("localhost", 9042));
    EndPoint asAddress = new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042));
    String underHostname = "s.nodes." + asHostname.asMetricPrefix() + '.' + metric.getPath();
    String underAddress = "s.nodes." + asAddress.asMetricPrefix() + '.' + metric.getPath();
    assertThat(underHostname).isNotEqualTo(underAddress);

    DefaultNode node = new DefaultNode(asHostname, context);
    assertThat(registry.getNames()).containsExactly(underHostname);

    node.setEndPoint(asAddress, context);

    assertThat(registry.getNames()).containsExactly(underAddress);
  }

  /** A context wired to a real Dropwizard registry, enough for {@link DefaultNode} to use it. */
  private static InternalDriverContext dropwizardContext(
      MetricRegistry registry, Set<NodeMetric> enabledMetrics) {
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(context.getSessionName()).thenReturn("s");
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(AbstractMetricUpdater.MIN_EXPIRE_AFTER);
    when(profile.getString(DefaultDriverOption.METRICS_ID_GENERATOR_PREFIX, "")).thenReturn("");
    // Built outside the when(): the generator's constructor reads back from the context, and
    // Mockito rejects a nested call on a stubbing that is still open.
    DefaultMetricIdGenerator idGenerator = new DefaultMetricIdGenerator(context);
    when(context.getMetricIdGenerator()).thenReturn(idGenerator);

    MetricsFactory metricsFactory = mock(MetricsFactory.class);
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    // Built on demand rather than up front: an updater registers its metrics from its constructor,
    // under the names the node's endpoint yields at that point.
    when(metricsFactory.newNodeUpdater(any()))
        .thenAnswer(
            invocation ->
                new DropwizardNodeMetricUpdater(
                    invocation.getArgument(0), context, enabledMetrics, registry));
    return context;
  }

  /** A context whose only stubbed behaviour is the metrics factory {@code DefaultNode} asks for. */
  private static InternalDriverContext contextWith(MetricsFactory metricsFactory) {
    InternalDriverContext context = mock(InternalDriverContext.class);
    when(context.getMetricsFactory()).thenReturn(metricsFactory);
    return context;
  }
}
