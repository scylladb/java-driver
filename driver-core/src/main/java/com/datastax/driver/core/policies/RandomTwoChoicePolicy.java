package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper load balancing policy that adds "Power of 2 Choice" algorithm to a child policy.
 *
 * <p>This policy encapsulates another policy. The resulting policy works in the following way:
 *
 * <ul>
 *   <li>the {@code distance} method is inherited from the child policy.
 *   <li>the {@code newQueryPlan} method will compare first two hosts (by number of inflight
 *       requests) returned from the {@code newQueryPlan} method of the child policy, and the host
 *       with fewer number of inflight requests will be returned the first. It will allow to always
 *       avoid the worst option (comparing by number of inflight requests).
 *   <li>besides the first two hosts returned by the child policy's {@code newQueryPlan} method, the
 *       ordering of the rest of the hosts will remain the same.
 * </ul>
 *
 * <p>If you wrap the {@code RandomTwoChoicePolicy} policy with {@code TokenAwarePolicy}, it will
 * compare the first two replicas by the number of inflight requests, and the worse option will
 * always be avoided. In that case, it is recommended to use the TokenAwarePolicy with {@code
 * ReplicaOrdering.RANDOM strategy}, which will return the replicas in a shuffled order and thus
 * will make the "Power of 2 Choice" algorithm more efficient.
 */
public class RandomTwoChoicePolicy implements ChainableLoadBalancingPolicy {
  private final LoadBalancingPolicy childPolicy;
  private volatile Metrics metrics;
  private volatile Metadata clusterMetadata;
  private volatile ProtocolVersion protocolVersion;
  private volatile CodecRegistry codecRegistry;

  /**
   * Creates a new {@code RandomTwoChoicePolicy}.
   *
   * @param childPolicy the load balancing policy to wrap with "Power of 2 Choice" algorithm.
   */
  public RandomTwoChoicePolicy(LoadBalancingPolicy childPolicy) {
    this.childPolicy = childPolicy;
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    this.metrics = cluster.getMetrics();
    this.clusterMetadata = cluster.getMetadata();
    this.protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    this.codecRegistry = cluster.getConfiguration().getCodecRegistry();
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
   * <p>The returned plan will compare (by the number of inflight requests) the first 2 hosts
   * returned by the child policy's {@code newQueryPlan} method, and the host with fewer inflight
   * requests will be returned the first. The rest of the child policy's query plan will be left
   * intact.
   */
  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    String keyspace = statement.getKeyspace();
    if (keyspace == null) keyspace = loggedKeyspace;

    ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);
    if (routingKey == null || keyspace == null) {
      return childPolicy.newQueryPlan(loggedKeyspace, statement);
    }

    final Token t = clusterMetadata.newToken(statement.getPartitioner(), routingKey);
    final Iterator<Host> childIterator = childPolicy.newQueryPlan(keyspace, statement);

    final Host host1 = childIterator.hasNext() ? childIterator.next() : null;
    final Host host2 = childIterator.hasNext() ? childIterator.next() : null;

    final AtomicInteger host1ShardInflightRequests = new AtomicInteger(0);
    final AtomicInteger host2ShardInflightRequests = new AtomicInteger(0);

    if (host1 != null) {
      final int host1ShardId = host1.getShardingInfo().shardId(t);
      host1ShardInflightRequests.set(
          metrics.getPerShardInflightRequestInfo().getValue().get(host1).get(host1ShardId));
    }

    if (host2 != null) {
      final int host2ShardId = host2.getShardingInfo().shardId(t);
      host2ShardInflightRequests.set(
          metrics.getPerShardInflightRequestInfo().getValue().get(host2).get(host2ShardId));
    }

    return new AbstractIterator<Host>() {
      private final Host firstChosenHost =
          host1ShardInflightRequests.get() < host2ShardInflightRequests.get() ? host1 : host2;
      private final Host secondChosenHost =
          host1ShardInflightRequests.get() < host2ShardInflightRequests.get() ? host2 : host1;
      private int index = 0;

      @Override
      protected Host computeNext() {
        if (index == 0) {
          index++;
          return firstChosenHost;
        } else if (index == 1) {
          index++;
          return secondChosenHost;
        } else if (childIterator.hasNext()) {
          return childIterator.next();
        }

        return endOfData();
      }
    };
  }

  @Override
  public void onAdd(Host host) {
    childPolicy.onAdd(host);
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
  public void onRemove(Host host) {
    childPolicy.onRemove(host);
  }

  @Override
  public void close() {
    childPolicy.close();
  }
}
