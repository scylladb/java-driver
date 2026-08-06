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

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Monitors the state of the Cassandra cluster.
 *
 * <p>It can either push {@link TopologyEvent topology events} to the rest of the driver (to do
 * that, retrieve the {@link EventBus}) from the {@link InternalDriverContext}), or receive requests
 * to refresh data about the nodes.
 *
 * <p>The default implementation uses the control connection: {@code TOPOLOGY_CHANGE} and {@code
 * STATUS_CHANGE} events on the connection are converted into {@code TopologyEvent}s, and node
 * refreshes are done with queries to system tables. If you prefer to rely on an external monitoring
 * tool, this can be completely overridden.
 */
public interface TopologyMonitor extends AsyncAutoCloseable {

  /**
   * Triggers the initialization of the monitor.
   *
   * <p>The completion of the future returned by this method marks the point when the driver
   * considers itself "connected" to the cluster, and proceeds with the rest of the initialization:
   * refreshing the list of nodes and the metadata, opening connection pools, etc. By then, the
   * topology monitor should be ready to accept calls to its other methods; in particular, {@link
   * #refreshNodeList()} will be called shortly after the completion of the future, to load the
   * initial list of nodes to connect to.
   *
   * <p>If {@code advanced.reconnect-on-init = true} in the configuration, this method is
   * responsible for handling reconnection. That is, if the initial attempt to "connect" to the
   * cluster fails, it must schedule reattempts, and only complete the returned future when
   * connection eventually succeeds. If the user cancels the returned future, then the reconnection
   * attempts should stop.
   *
   * <p>If this method is called multiple times, it should trigger initialization only once, and
   * return the same future on subsequent invocations.
   */
  CompletionStage<Void> init();

  /**
   * The future returned by {@link #init()}.
   *
   * <p>Note that this method may be called before {@link #init()}; at that stage, the future should
   * already exist, but be incomplete.
   */
  CompletionStage<Void> initFuture();

  /**
   * Invoked when the driver needs to refresh the information about an existing node. This is called
   * when the node was back and comes back up.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param node the node to refresh.
   * @return a future that completes with the information. If the monitor can't fulfill the request
   *     at this time, it should reply with {@link Optional#empty()}, and the driver will carry on
   *     with its current information.
   */
  CompletionStage<Optional<NodeInfo>> refreshNode(Node node);

  /**
   * Invoked when the driver needs to get information about a newly discovered node.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param broadcastRpcAddress the node's broadcast RPC address,.
   * @return a future that completes with the information. If the monitor doesn't know any node with
   *     this address, it should reply with {@link Optional#empty()}; the new node will be ignored.
   * @see Node#getBroadcastRpcAddress()
   */
  CompletionStage<Optional<NodeInfo>> getNewNodeInfo(InetSocketAddress broadcastRpcAddress);

  /**
   * Invoked when the driver needs to refresh information about all the nodes.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * <p>The driver calls this at initialization, and uses the result to initialize the {@link
   * LoadBalancingPolicy}; successful initialization of the {@link Session} object depends on that
   * initial call succeeding.
   *
   * @return a future that completes with the information. We assume that the full node list will
   *     always be returned in a single message (no paging).
   */
  CompletionStage<Iterable<NodeInfo>> refreshNodeList();

  /**
   * Resolves the full identity and metadata of the node at the other end of the given channel by
   * querying system.local. This is used by the control connection after establishing a channel to
   * resolve the contact point's full identity (hostId, datacenter, rack, endpoint, etc.).
   *
   * @param channel the channel to query system.local on.
   * @return a future that completes with the resolved node info.
   */
  CompletionStage<NodeInfo> getChannelNodeInfo(DriverChannel channel);

  /**
   * Checks whether the nodes in the cluster agree on a common schema version.
   *
   * <p>This should typically be implemented with a few retries and a timeout, as the schema can
   * take a while to replicate across nodes.
   */
  CompletionStage<Boolean> checkSchemaAgreement();

  /**
   * Resets any cached column name sets learned from previous system table query responses.
   *
   * <p>Called by the control connection on reconnect so that the next topology refresh re-learns
   * the available columns via {@code SELECT *} instead of reusing a potentially stale projection.
   *
   * <p>The default implementation is a no-op; implementations that cache column names (such as
   * {@link DefaultTopologyMonitor}) should override this method.
   */
  default void resetColumnCaches() {}

  /**
   * Whether this monitor re-resolves node addresses dynamically on every connection attempt (for
   * example by re-resolving a proxy hostname each time), rather than relying on an endpoint address
   * captured once at node-registration time.
   *
   * <p>When this returns {@code true}, the control connection's reconnection query plan must not
   * append the original contact points as a DNS re-resolution fallback (see {@code
   * advanced.control-connection.reconnection.fallback-to-original-contact-points}): the monitor
   * already keeps addresses fresh, and appending raw contact points could resurrect nodes that the
   * monitor has authoritatively removed.
   *
   * <p>The default implementation returns {@code false}, which is correct for {@link
   * DefaultTopologyMonitor}: the peer nodes it registers hold a {@code DefaultEndPoint} built from
   * the broadcast RPC address in {@code system.peers}, an already-resolved physical IP that never
   * needs re-resolving.
   *
   * <p>Unless the configured {@code AddressTranslator} hands back a name -- {@code
   * SubnetAddressTranslator} does, since its {@code resolve-addresses} option defaults to {@code
   * false}. Such a peer endpoint <b>is</b> re-expanded per connection attempt by {@code
   * ChannelFactory}, and if that name maps to more than one host, one {@code Node}'s connections
   * can land on different ones while routing, shard awareness and per-node metrics all attribute
   * them to that single node. The candidate loop keeps such addresses in resolver order rather than
   * shuffling them -- not because the node is identified, but because {@code DefaultEndPoint}
   * reports its addresses as not interchangeable (see {@code
   * PinnableEndPoint#addressesAreInterchangeable()} and {@code ChannelFactory#shuffleAndLimit}) --
   * so a pool stays on one host in practice, but the driver has no way to verify the premise. That
   * is a property of the translator's output, not of this monitor, so it does not change what this
   * flag reports.
   *
   * <p>The connected node's own {@code EndPoint} is a different case again. It originates from the
   * contact point the control connection used, and {@code ChannelFactory} binds it to the single
   * address that connection reached (see {@code PinnableEndPoint}), so it does <b>not</b> re-expand
   * on later connection attempts. Recovering from an address change for that node therefore depends
   * on this flag being {@code false}, i.e. on the contact-point fallback described above.
   *
   * <p>Proxy-based monitors that re-resolve per call should override this to return {@code true}.
   */
  default boolean reresolvesNodeAddresses() {
    return false;
  }
}
