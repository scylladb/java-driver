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

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.protocol.ShardingInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

/**
 * Implementation note: (almost) all the mutable state in this class is read concurrently, but only
 * mutated from {@link MetadataManager}'s admin thread. Node's ShardingInfo is an exception.
 */
@ThreadSafe
public class DefaultNode implements Node, Serializable {

  private static final long serialVersionUID = 1;

  private volatile EndPoint endPoint;
  // A deserialized node is not attached to a session anymore, so we don't need to retain this
  private transient volatile NodeMetricUpdater metricUpdater;

  volatile InetSocketAddress broadcastRpcAddress;
  volatile InetSocketAddress broadcastAddress;
  volatile InetSocketAddress listenAddress;
  volatile String datacenter;
  volatile String rack;
  volatile Version cassandraVersion;
  // Keep a copy of the raw tokens, to detect if they have changed when we refresh the node
  volatile Set<String> rawTokens;
  volatile Map<String, Object> extras;
  volatile UUID hostId;
  volatile UUID schemaVersion;

  // These 4 fields are read concurrently, but only mutated on NodeStateManager's admin thread
  volatile NodeState state;
  volatile int openConnections;
  volatile int reconnections;
  volatile long upSinceMillis;

  volatile NodeDistance distance;

  // Initially null. A copy of ShardingInfo. Updated with values by DriverChannel during pool
  // initialization.
  private volatile ShardingInfo shardingInfo;

  public DefaultNode(EndPoint endPoint, InternalDriverContext context) {
    this(endPoint, context, true);
  }

  private DefaultNode(EndPoint endPoint, InternalDriverContext context, boolean metrics) {
    this.endPoint = endPoint;
    this.state = NodeState.UNKNOWN;
    this.distance = NodeDistance.IGNORED;
    this.rawTokens = Collections.emptySet();
    this.extras = Collections.emptyMap();
    this.metricUpdater =
        metrics ? context.getMetricsFactory().newNodeUpdater(this) : NoopNodeMetricUpdater.INSTANCE;
    this.upSinceMillis = -1;
    this.shardingInfo = null;
  }

  /** Creates a contact point node without registering metrics. */
  public static DefaultNode newContactPoint(EndPoint endPoint, InternalDriverContext context) {
    return new DefaultNode(endPoint, context, false);
  }

  @NonNull
  @Override
  public EndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(@NonNull EndPoint newEndPoint, @NonNull InternalDriverContext context) {
    // Nothing downstream can tell the two instances apart, so keep the one already held. Not merely
    // an optimization: the instance this node holds may carry a reverse-DNS name cached on its
    // InetAddress by an earlier TLS handshake (DefaultSslEngineFactory calls getHostName() under
    // the
    // default advanced.ssl-engine-factory.allow-dns-reverse-lookup-san = true), and every full
    // topology refresh mints a brand-new endpoint over a brand-new InetSocketAddress for every
    // node.
    // Adopting each one unconditionally would throw that name away and make the next connection to
    // the node repeat the blocking reverse lookup on a Netty I/O loop -- once per node per refresh
    // instead of once per node per session, during exactly the refresh-then-reconnect storms this
    // feature exists for.
    //
    // Deliberately not equals(): see PinnableEndPoint#sameIdentity, which is also what
    // ControlConnection uses when it decides whether the control channel should adopt a node's
    // endpoint.
    if (PinnableEndPoint.sameIdentity(newEndPoint, endPoint)) {
      return;
    }

    // Metrics are registered under names derived from the endpoint, so they have to be
    // re-registered
    // whenever those names change -- which is not the same question as whether this is a different
    // node. It is narrower in one direction: a PinnableEndPoint copy differs from the original only
    // by the address it is pinned to, and both equals() and the metric identity ignore that by
    // contract (see PinnableEndPoint). And it is wider in the other: an unresolved hostname and the
    // resolved address it maps to compare *equal* (see DefaultEndPoint#equals) while their metric
    // prefixes differ, which is exactly what happens when a contact-point node adopts the endpoint
    // built from its system.local row.
    //
    // asMetricPrefix() alone, deliberately, even though the tagging MetricIdGenerator tags metrics
    // with the endpoint's toString() rather than its prefix. toString() cannot be used as an
    // identity key because it is not stable across equal instances: DefaultEndPoint delegates it to
    // InetSocketAddress, which renders InetAddress's *cached* hostName field, and that field is
    // populated the first time anything calls getHostName(). DefaultSslEngineFactory does exactly
    // that while building an engine, under the default advanced.ssl-engine-factory
    // .allow-dns-reverse-lookup-san = true -- on the very instance this node holds, since an
    // already-resolved endpoint is passed through unchanged by resolveCandidates() and pin(). So
    // after the first channel this node's endpoint renders as "host/1.2.3.4:9042" while the one the
    // next refresh decodes from system.peers renders as "/1.2.3.4:9042", and keying on that would
    // clear and re-register every node's metrics on every topology refresh -- widening the
    // clear/rebuild race described below from "once per endpoint change" to "always".
    //
    // The cost of leaving it out: a tagging generator can keep reporting under an endpoint string
    // the node no longer answers to, until something else changes the prefix.
    boolean differentMetricIdentity =
        !newEndPoint.asMetricPrefix().equals(endPoint.asMetricPrefix());
    // metricUpdater is transient, so it can be null on deserialized nodes.
    NodeMetricUpdater previousMetricUpdater = metricUpdater;
    boolean rebuildMetricUpdater =
        differentMetricIdentity
            && previousMetricUpdater != null
            && !(previousMetricUpdater instanceof NoopNodeMetricUpdater);

    // Clearing comes *before* the swap. Dropwizard and MicroProfile do not remember the ids they
    // registered under; clearMetrics() recomputes each one from this node's current endpoint (see
    // DropwizardMetricUpdater#clearMetrics and MetricIdGenerator#nodeMetricId). Clearing after the
    // swap would therefore delete the series the new updater had just registered and leave the old
    // ones behind, under a name nothing writes to any more. Micrometer removes the Meter instances
    // it holds and does not care either way.
    //
    // The three steps are not atomic with respect to concurrent metric writes: metricUpdater is
    // volatile and read from I/O threads, so a write landing between the clear and the rebuild
    // goes through the updater that was just cleared, and Dropwizard re-registers on demand
    // (getOrCreateCounterFor -> registry.counter(getMetricId(m))). That resurrects one series,
    // named from whichever endpoint this node holds at that instant.
    //
    // Note the window is narrow but its effect is not transient: the resurrected metric is cached
    // in the old updater's map, and ChannelFactory snapshots node.getMetricUpdater() once per
    // connection and hands it to the traffic meters, which hold it for the channel's life. So a
    // mark that lands here keeps reporting under the old endpoint's name until every channel open
    // at that moment has been recycled. Nothing throws -- registry.counter() is get-or-create --
    // and the request path re-reads getMetricUpdater() per request, so the misreporting is
    // confined to the byte counters. Closing it properly means having clearMetrics() remove the
    // ids it registered under rather than recomputing them from the current endpoint, which is a
    // change to every metrics implementation and would also fix a second problem: two nodes can
    // briefly share a metric prefix (a control node's endpoint is its contact point's), and then
    // this clear deletes the series the other node just registered.
    if (rebuildMetricUpdater) {
      previousMetricUpdater.clearMetrics();
    }

    // Adopt the newest instance even when it compares equal: a pinned copy carries the address
    // every
    // subsequent connection to this node will use, so refusing it would freeze the node on the
    // first
    // address it ever connected to, even after the control connection moved to another one and told
    // us about it. (The early return above lets through exactly the instances that differ in that
    // address, or in metric identity, or in kind.)
    endPoint = newEndPoint;

    // And building comes *after* it: the updaters register every enabled metric from their
    // constructor, deriving the names from the endpoint this node holds at that moment.
    if (rebuildMetricUpdater) {
      NodeMetricUpdater newMetricUpdater = context.getMetricsFactory().newNodeUpdater(this);
      // Carry over any pending metrics expiration before publishing the replacement: the factories
      // arm and cancel it through node.getMetricUpdater(), so from here on they would only ever
      // reach the new one, leaving the old one's timer pending on an object nothing refers to.
      //
      // Which leaves a window of its own, in the other direction. The hand-over arms the
      // replacement while getMetricUpdater() still answers with the old one, so an UP event landing
      // in between cancels a countdown that is already gone and misses the live one -- and the node
      // comes back healthy with an expiration still ticking, clearing every one of its series an
      // hour later with nothing to re-register them until it next goes down and up. The two writers
      // really are on different threads: MetadataManager and DropwizardMetricsFactory each take
      // their own adminEventExecutorGroup().next(), and advanced.netty.admin-group.size is 2 by
      // default.
      //
      // Ordering cannot close it -- publishing first only moves the window, since a cancel arriving
      // before the hand-over is a no-op on an updater that is not armed yet and the hand-over then
      // arms it anyway. Only folding the timeout and the expired flag into a single atomic would,
      // which is the same conclusion AbstractMetricUpdater#newTimeout reaches about its own
      // hand-over race. Reaching this needs an endpoint change to interleave with an UP event
      // within a few instructions, on a node that was down with an expiration pending, so the
      // window is named rather than claimed away (issue #1010).
      newMetricUpdater.adoptExpirationFrom(previousMetricUpdater);
      metricUpdater = newMetricUpdater;
    }
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getBroadcastRpcAddress() {
    return Optional.ofNullable(broadcastRpcAddress);
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getBroadcastAddress() {
    return Optional.ofNullable(broadcastAddress);
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getListenAddress() {
    return Optional.ofNullable(listenAddress);
  }

  @Nullable
  @Override
  public String getDatacenter() {
    return datacenter;
  }

  @Nullable
  @Override
  public String getRack() {
    return rack;
  }

  @Nullable
  @Override
  public Version getCassandraVersion() {
    return cassandraVersion;
  }

  @Nullable
  @Override
  public UUID getHostId() {
    return hostId;
  }

  @Nullable
  @Override
  public UUID getSchemaVersion() {
    return schemaVersion;
  }

  @NonNull
  @Override
  public Map<String, Object> getExtras() {
    return extras;
  }

  @NonNull
  @Override
  public NodeState getState() {
    return state;
  }

  @Override
  public long getUpSinceMillis() {
    return upSinceMillis;
  }

  @Override
  public int getOpenConnections() {
    return openConnections;
  }

  @Override
  public boolean isReconnecting() {
    return reconnections > 0;
  }

  @NonNull
  @Override
  public NodeDistance getDistance() {
    return distance;
  }

  public NodeMetricUpdater getMetricUpdater() {
    return metricUpdater;
  }

  @Override
  public String toString() {
    // Include the hash code because this class uses reference equality
    return String.format(
        "Node(endPoint=%s, hostId=%s, hashCode=%x)", getEndPoint(), getHostId(), hashCode());
  }

  /** Note: deliberately not exposed by the public interface. */
  public Set<String> getRawTokens() {
    return rawTokens;
  }

  @Nullable
  @Override
  public ShardingInfo getShardingInfo() {
    return shardingInfo;
  }

  public void setShardingInfo(ShardingInfo shardingInfo) {
    this.shardingInfo = shardingInfo;
  }
}
