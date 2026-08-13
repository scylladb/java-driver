/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.driver.core.policies;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.google.common.annotations.Beta;
import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A wrapper load balancing policy that adds token awareness to a child policy.
 *
 * <p>This policy encapsulates another policy. The resulting policy works in the following way:
 *
 * <ul>
 *   <li>the {@code distance} method is inherited from the child policy.
 *   <li>the iterator returned by the {@code newQueryPlan} method will first return the {@link
 *       HostDistance#LOCAL LOCAL} replicas for the query <em>if possible</em> (i.e. if the query's
 *       {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} is not
 *       {@code null} and if the {@linkplain Metadata#getReplicasList(String, String, Token.Factory,
 *       ByteBuffer) set of replicas} for that partition key is not empty). If no local replica can
 *       be either found or successfully contacted, the rest of the query plan will fall back to the
 *       child policy's one.
 * </ul>
 *
 * The exact order in which local replicas are returned is dictated by the {@linkplain
 * ReplicaOrdering strategy} provided at instantiation.
 *
 * <p>Do note that only replicas for which the child policy's {@linkplain
 * LoadBalancingPolicy#distance(Host) distance} method returns {@link HostDistance#LOCAL LOCAL} will
 * be considered having priority. For example, if you wrap {@link DCAwareRoundRobinPolicy} with this
 * token aware policy, replicas from remote data centers may only be returned after all the hosts of
 * the local data center.
 *
 * <h3>Lightweight Transaction (LWT) Routing</h3>
 *
 * <p>For {@linkplain Statement#isLWT() lightweight transaction} queries, this policy provides
 * specialized routing to optimize LWT performance and avoid contention. When LWT routing is enabled
 * (the default), the query plan prioritizes replicas for the target partition, ordered by
 * datacenter locality, followed by non-replica nodes for failover:
 *
 * <ul>
 *   <li>Local replicas first: replicas for which the child policy reports {@link HostDistance#LOCAL
 *       LOCAL} distance are returned first, in the order provided by cluster metadata (preserving
 *       primary replica ordering from the token ring).
 *   <li>Remote replicas second: remaining replicas (typically in remote datacenters) are appended,
 *       but only if they are up and not ignored by the child policy.
 *   <li>Non-replica nodes: remaining nodes from the child policy's query plan are appended after
 *       all replicas, ensuring the query plan always includes all available nodes for failover.
 * </ul>
 *
 * <p><strong>Rack awareness</strong> is intentionally <em>not</em> applied to LWT replica ordering.
 * All local replicas are treated equally within the local datacenter to avoid rack-based contention
 * hotspots during Paxos consensus phases.
 *
 * <p><strong>Requirements for LWT replica-only routing:</strong>
 *
 * <ul>
 *   <li>The statement's {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing
 *       key} must be available (use {@linkplain PreparedStatement prepared statements} or manually
 *       set the routing key).
 *   <li>The effective keyspace must be known (set on the statement or session).
 *   <li>Cluster metadata must contain replica information for the target partition.
 *   <li>A child policy that correctly reports datacenter locality via {@link
 *       LoadBalancingPolicy#distance(Host)} (e.g., {@link DCAwareRoundRobinPolicy}) must be
 *       configured.
 * </ul>
 *
 * <p><strong>Fallback behavior:</strong> If routing key, keyspace, or replica metadata is
 * unavailable, the driver falls back to the child policy's normal query plan. In this case, the
 * query plan may include non-replica hosts, and LWT may incur additional coordinator forwarding
 * latency. This fallback is a pragmatic safety net to preserve availability when routing
 * information is incomplete.
 *
 * <p>LWT routing can be configured via {@link
 * QueryOptions#setLoadBalancingLwtRequestRoutingMethod(QueryOptions.RequestRoutingMethod)}. The
 * default preserves replica order for optimal LWT performance.
 *
 * @see DCAwareRoundRobinPolicy
 * @see QueryOptions.RequestRoutingMethod
 */
public class TokenAwarePolicy implements ChainableLoadBalancingPolicy {

  /** Strategies for replica ordering. */
  public enum ReplicaOrdering {

    /**
     * Order replicas by token ring topology, i.e. always return the "primary" replica first, then
     * the second, etc., according to the placement of replicas around the token ring.
     *
     * <p>This strategy is the only one guaranteed to order replicas in a deterministic and constant
     * way. This increases the effectiveness of server-side row caching (especially at consistency
     * level ONE), but is more heavily impacted by hotspots, since the primary replica is always
     * tried first.
     */
    TOPOLOGICAL,

    /**
     * Return replicas in a different, random order for each query plan. This is the default
     * strategy.
     *
     * <p>This strategy fans out writes and thus can alleviate hotspots caused by "fat" partitions,
     * but its randomness makes server-side caching less efficient.
     */
    RANDOM,

    /**
     * Return the replicas in the exact same order in which they appear in the child policy's query
     * plan.
     *
     * <p>This is the only strategy that fully respects the child policy's replica ordering. Use it
     * when it is important to keep that order intact (e.g. when using the {@link
     * LatencyAwarePolicy}).
     */
    NEUTRAL
  }

  /**
   * An iterator that returns local replicas first (in the order provided by the child policy), then
   * the remaining hosts.
   */
  private class NeutralHostIterator extends AbstractIterator<Host> {

    private final Iterator<Host> childIterator;
    private final List<Host> replicas;
    private List<Host> nonReplicas;
    private Iterator<Host> nonReplicasIterator;

    public NeutralHostIterator(Iterator<Host> childIterator, List<Host> replicas) {
      this.childIterator = childIterator;
      this.replicas = replicas;
    }

    @Override
    protected Host computeNext() {

      while (childIterator.hasNext()) {

        Host host = childIterator.next();

        if (host.isUp()
            && replicas.contains(host)
            && childPolicy.distance(host) == HostDistance.LOCAL) {
          // UP replicas should be prioritized, retaining order from childPolicy
          return host;
        } else {
          // save for later
          if (nonReplicas == null) nonReplicas = new ArrayList<>();
          nonReplicas.add(host);
        }
      }

      // This should only engage if all local replicas are DOWN
      if (nonReplicas != null) {

        if (nonReplicasIterator == null) nonReplicasIterator = nonReplicas.iterator();

        if (nonReplicasIterator.hasNext()) return nonReplicasIterator.next();
      }

      return endOfData();
    }
  }

  /**
   * An iterator that returns local replicas first (in either random or topological order, as
   * specified at instantiation), then the remaining hosts.
   */
  private class RandomOrTopologicalHostIterator extends AbstractIterator<Host> {

    private final Iterator<Host> replicasIterator;
    private final String keyspace;
    private final Statement statement;
    private final List<Host> replicas;
    private Iterator<Host> childIterator;

    public RandomOrTopologicalHostIterator(
        String keyspace,
        Statement statement,
        Iterator<Host> replicasIterator,
        List<Host> replicas) {
      this.replicasIterator = replicasIterator;
      this.keyspace = keyspace;
      this.statement = statement;
      this.replicas = replicas;
    }

    @Override
    protected Host computeNext() {
      while (replicasIterator.hasNext()) {
        Host host = replicasIterator.next();
        if (host.isUp() && childPolicy.distance(host) == HostDistance.LOCAL) return host;
      }

      if (childIterator == null) childIterator = childPolicy.newQueryPlan(keyspace, statement);

      while (childIterator.hasNext()) {
        Host host = childIterator.next();
        // Skip it if it was already a local replica
        if (!replicas.contains(host) || childPolicy.distance(host) != HostDistance.LOCAL)
          return host;
      }
      return endOfData();
    }
  }

  /**
   * An iterator that returns replicas first, with local replicas prioritized (preserving primary
   * replica order), then remote replicas, then non-replica nodes from the child policy. DOWN and
   * IGNORED hosts are filtered out from replicas.
   *
   * <p>Query plan follows a four-pass strategy:
   *
   * <ol>
   *   <li><strong>Local replicas:</strong> Returns UP replicas marked as LOCAL by the child policy,
   *       in the order provided by cluster metadata (preserving primary replica order).
   *   <li><strong>Remote replicas:</strong> Returns UP replicas marked as REMOTE by the child
   *       policy.
   *   <li><strong>Non-replica nodes:</strong> Returns remaining nodes from the child policy's query
   *       plan, skipping any hosts already returned as replicas. This ensures all available nodes
   *       are included in the query plan for failover.
   *   <li><strong>Child policy fallback:</strong> If no suitable replicas were returned at all (for
   *       example, all are DOWN or IGNORED), falls back to the child policy's full query plan.
   * </ol>
   */
  private class PreserveReplicaOrderIterator extends AbstractIterator<Host> {
    private final Iterator<Host> replicasIterator;
    private final List<Host> replicas;
    private final String keyspace;
    private final Statement statement;
    private List<Host> nonLocalReplicas;
    private Iterator<Host> nonLocalReplicasIterator;
    private Set<Host> returnedHosts;
    private Iterator<Host> childIterator;

    public PreserveReplicaOrderIterator(String keyspace, Statement statement, List<Host> replicas) {
      this.keyspace = keyspace;
      this.statement = statement;
      this.replicas = replicas;
      this.replicasIterator = replicas.iterator();
    }

    @Override
    protected Host computeNext() {
      // First pass: return local replicas that are UP
      while (replicasIterator.hasNext()) {
        Host host = replicasIterator.next();
        HostDistance distance = childPolicy.distance(host);

        if (!host.isUp()) {
          // Skip DOWN hosts entirely
          continue;
        }

        switch (distance) {
          case LOCAL:
            if (returnedHosts == null) returnedHosts = new HashSet<>();
            returnedHosts.add(host);
            return host;
          case REMOTE:
            // Collect remote replicas for second pass
            if (nonLocalReplicas == null) nonLocalReplicas = new ArrayList<>();
            nonLocalReplicas.add(host);
            break;
          case IGNORED: // Skip IGNORED hosts entirely
          default: // For safety, treat any unexpected distance as IGNORED
            break;
        }
      }

      // Second pass: return remote replicas that are UP and not IGNORED
      if (nonLocalReplicas != null) {
        if (nonLocalReplicasIterator == null) {
          nonLocalReplicasIterator = nonLocalReplicas.iterator();
        }
        while (nonLocalReplicasIterator.hasNext()) {
          Host host = nonLocalReplicasIterator.next();
          if (returnedHosts == null) returnedHosts = new HashSet<>();
          returnedHosts.add(host);
          return host;
        }
      }

      // Third pass: return remaining nodes from child policy
      if (childIterator == null) {
        childIterator = childPolicy.newQueryPlan(keyspace, statement);
      }
      while (childIterator.hasNext()) {
        Host host = childIterator.next();
        // Skip hosts we already returned as replicas
        if (returnedHosts != null && returnedHosts.contains(host)) {
          continue;
        }
        // If we returned some replicas, skip remaining replicas from child policy
        // to avoid duplicates. If no replicas were returned (all DOWN/IGNORED),
        // allow full child policy fallback including replica hosts.
        if (returnedHosts != null && replicas.contains(host)) {
          continue;
        }
        return host;
      }

      return endOfData();
    }
  }

  private final LoadBalancingPolicy childPolicy;
  private final ReplicaOrdering replicaOrdering;
  private volatile Metadata clusterMetadata;
  private volatile ProtocolVersion protocolVersion;
  private volatile CodecRegistry codecRegistry;
  private volatile QueryOptions queryOptions;
  private volatile QueryOptions.RequestRoutingMethod defaultLwtRequestRoutingMethod;

  /**
   * Creates a new {@code TokenAware} policy.
   *
   * @param childPolicy the load balancing policy to wrap with token awareness.
   * @param replicaOrdering the strategy to use to order replicas.
   */
  public TokenAwarePolicy(LoadBalancingPolicy childPolicy, ReplicaOrdering replicaOrdering) {
    this.childPolicy = childPolicy;
    this.replicaOrdering = replicaOrdering;
  }

  /**
   * Creates a new {@code TokenAware} policy.
   *
   * @param childPolicy the load balancing policy to wrap with token awareness.
   * @param shuffleReplicas whether or not to shuffle the replicas. If {@code true}, then the {@link
   *     ReplicaOrdering#RANDOM RANDOM} strategy will be used, otherwise the {@link
   *     ReplicaOrdering#TOPOLOGICAL TOPOLOGICAL} one will be used.
   * @deprecated Use {@link #TokenAwarePolicy(LoadBalancingPolicy, ReplicaOrdering)} instead. This
   *     constructor will be removed in the next major release.
   */
  @Deprecated
  public TokenAwarePolicy(LoadBalancingPolicy childPolicy, boolean shuffleReplicas) {
    this(childPolicy, shuffleReplicas ? ReplicaOrdering.RANDOM : ReplicaOrdering.TOPOLOGICAL);
  }

  /**
   * Creates a new {@code TokenAware} policy with {@link ReplicaOrdering#RANDOM RANDOM} replica
   * ordering.
   *
   * @param childPolicy the load balancing policy to wrap with token awareness.
   */
  public TokenAwarePolicy(LoadBalancingPolicy childPolicy) {
    this(childPolicy, ReplicaOrdering.RANDOM);
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
  }

  /**
   * The strategy this policy uses to order the replicas of a query's partition, as specified at
   * instantiation.
   *
   * @return the replica ordering strategy.
   */
  @Beta
  public ReplicaOrdering getReplicaOrdering() {
    return replicaOrdering;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    clusterMetadata = cluster.getMetadata();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    queryOptions = cluster.getConfiguration().getQueryOptions();
    defaultLwtRequestRoutingMethod = queryOptions.getLoadBalancingLwtRequestRoutingMethod();
    childPolicy.init(cluster, hosts);
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation always returns distances as reported by the wrapped policy.
   */
  @Override
  public HostDistance distance(Host host) {
    return childPolicy.distance(host);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The returned plan will first return local replicas for the query (i.e. replicas whose
   * {@linkplain HostDistance distance} according to the child policy is {@code LOCAL}), if it can
   * determine them (i.e. mainly if the statement's {@linkplain
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} is not {@code null}), and
   * ordered according to the {@linkplain ReplicaOrdering ordering strategy} specified at
   * instantiation; following what it will return the rest of the child policy's original query
   * plan.
   */
  @Override
  public Iterator<Host> newQueryPlan(final String loggedKeyspace, final Statement statement) {
    ByteBuffer partitionKey = statement.getRoutingKey(protocolVersion, codecRegistry);
    String keyspace = statement.getKeyspace();
    if (keyspace == null) keyspace = loggedKeyspace;

    if (partitionKey == null || keyspace == null)
      return childPolicy.newQueryPlan(keyspace, statement);

    String tableName = getRoutingTable(statement);

    final List<Host> replicas =
        clusterMetadata.getReplicasList(
            Metadata.quote(keyspace), tableName, statement.getPartitioner(), partitionKey);

    QueryOptions.RequestRoutingMethod routing =
        defaultLwtRequestRoutingMethod != null
            ? getRequestRouting(statement)
            : QueryOptions.RequestRoutingMethod.REGULAR;
    switch (routing) {
      case PRESERVE_REPLICA_ORDER:
        return newQueryPlanPreserveReplicaOrder(keyspace, statement, replicas);
      case REGULAR:
      default:
        return newQueryPlanRegular(keyspace, statement, replicas);
    }
  }

  private String getRoutingTable(Statement statement) {
    ColumnDefinitions defs = getRoutingVariables(statement);
    return (defs == null || defs.size() == 0) ? null : defs.getTable(0);
  }

  private ColumnDefinitions getRoutingVariables(Statement statement) {
    Statement target = statement;
    if (statement instanceof BatchStatement) {
      target = ((BatchStatement) statement).getRoutingStatement(protocolVersion, codecRegistry);
      if (target == null) {
        return null;
      }
    }

    if (target instanceof BoundStatement) {
      return ((BoundStatement) target).preparedStatement().getVariables();
    } else if (target instanceof PreparedStatement) {
      return ((PreparedStatement) target).getVariables();
    }
    return null;
  }

  private QueryOptions.RequestRoutingMethod getRequestRouting(Statement statement) {
    if (statement.isLWT()) {
      return defaultLwtRequestRoutingMethod;
    }
    ConsistencyLevel cl =
        statement.getConsistencyLevel() != null
            ? statement.getConsistencyLevel()
            : queryOptions.getConsistencyLevel();
    if (cl != null && cl.isSerial()) {
      return defaultLwtRequestRoutingMethod;
    }
    return QueryOptions.RequestRoutingMethod.REGULAR;
  }

  private Iterator<Host> newQueryPlanRegular(
      String keyspace, Statement statement, List<Host> replicas) {
    if (replicas.isEmpty()) {
      return childPolicy.newQueryPlan(keyspace, statement);
    }

    if (replicaOrdering == ReplicaOrdering.NEUTRAL) {

      final Iterator<Host> childIterator = childPolicy.newQueryPlan(keyspace, statement);

      return new NeutralHostIterator(childIterator, replicas);

    } else {

      final Iterator<Host> replicasIterator;

      if (replicaOrdering == ReplicaOrdering.RANDOM) {
        ArrayList<Host> replicasCopy = new ArrayList<>(replicas);
        Collections.shuffle(replicasCopy, ThreadLocalRandom.current());
        replicasIterator = replicasCopy.iterator();
      } else {
        replicasIterator = replicas.iterator();
      }

      return new RandomOrTopologicalHostIterator(keyspace, statement, replicasIterator, replicas);
    }
  }

  private Iterator<Host> newQueryPlanPreserveReplicaOrder(
      String keyspace, Statement statement, List<Host> replicas) {
    if (replicas.isEmpty()) {
      return childPolicy.newQueryPlan(keyspace, statement);
    }
    return new PreserveReplicaOrderIterator(keyspace, statement, replicas);
  }

  @Override
  public void onUp(Host host) {
    childPolicy.onUp(host);
  }

  @Override
  public void onDown(Host host) {
    childPolicy.onDown(host);
  }

  @Override
  public void onAdd(Host host) {
    childPolicy.onAdd(host);
  }

  @Override
  public void onRemove(Host host) {
    childPolicy.onRemove(host);
  }

  @Override
  public void close() {
    childPolicy.close();
  }
}
