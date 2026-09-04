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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class AddNodeRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(AddNodeRefresh.class);

  @VisibleForTesting final NodeInfo newNodeInfo;

  AddNodeRefresh(NodeInfo newNodeInfo) {
    this.newNodeInfo = newNodeInfo;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {
    Map<UUID, Node> oldNodes = oldMetadata.getNodes();
    Node existing = oldNodes.get(newNodeInfo.getHostId());
    if (existing == null) {
      DefaultNode newNode = new DefaultNode(newNodeInfo.getEndPoint(), context);
      copyInfos(newNodeInfo, newNode, context);
      Map<UUID, Node> newNodes =
          ImmutableMap.<UUID, Node>builder()
              .putAll(oldNodes)
              .put(newNode.getHostId(), newNode)
              .build();
      return new Result(
          oldMetadata.withNodes(newNodes, tokenMapEnabled, false, null, context),
          ImmutableList.of(NodeStateEvent.added(newNode)));
    } else {
      // If a node is restarted after changing its broadcast RPC address, Cassandra considers that
      // an addition, even though the host_id hasn't changed :(
      // Update the existing instance and emit an UP event to trigger a pool reconnection.
      //
      // Asked of the broadcast RPC address, not of the endpoint. The endpoint is *derived* from
      // that address by the configured AddressTranslator, so for a translator that hands back the
      // address it was given the two questions are the same one -- but where they differ, the
      // endpoint answers wrongly in both directions and the address answers correctly:
      //
      // - A translator that returns a name (SubnetAddressTranslator does, under its default
      //   resolve-addresses = false; so do FixedHostNameAddressTranslator and
      //   Ec2MultiRegionAddressTranslator now) maps every node it covers to the same endpoint, so
      //   a node that really did move compares equal and its pool is never told.
      // - The same address yields two endpoint representations depending on which system table it
      //   was read from: DefaultTopologyMonitor#connectedNodeEndPoint gives the control node an
      //   identity of its own, while its peers row gives it the translator's output. A NEW_NODE
      //   event for the current control node is therefore a comparison between those two forms,
      //   and it reports a change on every such event even though nothing moved -- copying the
      //   peers-derived endpoint in, flipping the metric identity and clearing the node's series
      //   (see DefaultNode#setEndPoint), then flipping back on the next refresh.
      //
      // Neither EndPoint#equals nor PinnableEndPoint#sameIdentity can separate those: the first
      // resolves the unresolved side of a mixed pair, which is a blocking DNS lookup on the admin
      // event loop whose answer depends on which address the resolver lists first (issue #1006),
      // and the second compares metric identity, which is exactly what the second case changes
      // while the addressing stays put. Both system-table addresses are already resolved, so this
      // needs no lookup either way.
      //
      // An absent address on the *existing* node counts as a change, as the endpoint comparison
      // did for a node that had never carried one.
      //
      // What this does not see is the reverse, and it costs more than the endpoint alone: the
      // translator's answer changing while the address does not. The endpoint is a function of
      // (address, translator), and two of the translators named above re-derive per call --
      // Ec2MultiRegionAddressTranslator from a live PTR lookup,
      // FixedHostNameAddressTranslator from config -- and getNewNodeInfo builds the NodeInfo
      // through translate() on every event. So an instance replaced behind an unchanged broadcast
      // RPC address keeps the old name here and its pool goes on dialling it. Bounded rather than
      // permanent: FullNodeListRefresh runs copyInfos over the existing nodes, so the next full
      // refresh -- every control-connection reconnect, and every topology-driven one -- picks the
      // new endpoint up.
      //
      // And copyInfos carries more than the endpoint -- datacenter, rack, host id, schema and
      // Cassandra version, tokens, extras, broadcast and listen address -- so for such a node none
      // of those are refreshed by this event either, and nothing else in this class re-establishes
      // them. Bounded by the same full refresh. The endpoint comparison caught that case and lost
      // the two above, which are both unconditional; this trade is the deliberate one.
      Optional<InetSocketAddress> newRpcAddress = newNodeInfo.getBroadcastRpcAddress();
      // Checked rather than asserted, because the get() below is only safe if it holds and an
      // assert is not there in production. Always present in practice -- a NEW_NODE event is
      // answered from the peers table, and findInPeers builds no NodeInfo without one -- but
      // TopologyMonitor#getNewNodeInfo is an extension point, and an absent address would satisfy
      // the inequality against a present one and then throw NoSuchElementException out of
      // MetadataRefresh#compute, which MetadataManager#apply does not catch. Nothing to update
      // towards and nothing to raise an event about, so the refresh is a no-op.
      if (!newRpcAddress.isPresent()) {
        LOG.warn(
            "[{}] Ignoring node addition for {}: the new node info carries no broadcast RPC "
                + "address, so there is nothing to compare against the existing node",
            context.getSessionName(),
            existing);
        return new Result(oldMetadata);
      }
      if (!newRpcAddress.equals(existing.getBroadcastRpcAddress())) {
        copyInfos(newNodeInfo, ((DefaultNode) existing), context);
        return new Result(
            oldMetadata, ImmutableList.of(TopologyEvent.suggestUp(newRpcAddress.get())));
      } else {
        return new Result(oldMetadata);
      }
    }
  }
}
