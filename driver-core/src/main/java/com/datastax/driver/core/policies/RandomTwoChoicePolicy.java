package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomTwoChoicePolicy implements LoadBalancingPolicy {
  private volatile Metrics metrics;
  private volatile Metadata clusterMetadata;
  private volatile ProtocolVersion protocolVersion;
  private volatile CodecRegistry codecRegistry;
  private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();
  private final AtomicInteger roundRobinIndex = new AtomicInteger();

  public RandomTwoChoicePolicy() {}

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    this.metrics = cluster.getMetrics();
    this.clusterMetadata = cluster.getMetadata();
    this.protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    this.codecRegistry = cluster.getConfiguration().getCodecRegistry();
    this.liveHosts.addAll(hosts);
    this.roundRobinIndex.set(new Random().nextInt(Math.max(1, hosts.size())));
  }

  @Override
  public HostDistance distance(Host host) {
    return HostDistance.LOCAL;
  }

  private Iterator<Host> getRoundRobinIteratorOnLiveHosts() {
    @SuppressWarnings("unchecked")
    final List<Host> hosts = (List<Host>) liveHosts.clone();
    final int startIndex = roundRobinIndex.getAndIncrement();

    // Overflow protection; not theoretically thread safe but should be good enough
    if (startIndex > Integer.MAX_VALUE - 10000) roundRobinIndex.set(0);

    return new AbstractIterator<Host>() {
      private int index = startIndex;
      private int remainingSize = hosts.size();

      @Override
      protected Host computeNext() {
        if (remainingSize <= 0) return endOfData();

        // Round-robin policy
        remainingSize--;
        int c = index++ % hosts.size();
        if (c < 0) c += hosts.size();
        return hosts.get(c);
      }
    };
  }

  private void swap(Object[] elements, int i, int j) {
    if (i != j) {
      Object tmp = elements[i];
      elements[i] = elements[j];
      elements[j] = tmp;
    }
  }

  private void bubbleUp(Object[] elements, int sourceIndex, int targetIndex) {
    for (int i = sourceIndex; i > targetIndex; i--) {
      swap(elements, i, i - 1);
    }
  }

  private void shuffleFirstN(Object[] elements, int n) {
    assert n <= elements.length;

    if (n > 1) {
      for (int i = n - 1; i > 0; i--) {
        int j = new Random().nextInt(i + 1);
        swap(elements, i, j);
      }
    }
  }

  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    String keyspace = statement.getKeyspace();
    if (keyspace == null) keyspace = loggedKeyspace;

    ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);

    if (routingKey == null || keyspace == null) {
      return getRoundRobinIteratorOnLiveHosts();
    }

    final Set<Host> replicaSet =
        clusterMetadata.getReplicas(
            Metadata.quote(keyspace), statement.getPartitioner(), routingKey);

    if (replicaSet.isEmpty()) {
      return getRoundRobinIteratorOnLiveHosts();
    }

    final Object[] currentNodes = this.liveHosts.toArray();
    int replicaCount = 0;

    // Move all replica nodes to the front of the array
    for (int i = 0; i < currentNodes.length; i++) {
      Host host = (Host) currentNodes[i];
      if (replicaSet.contains(host)) {
        bubbleUp(currentNodes, i, replicaCount);
        replicaCount++;
      }
    }

    // Shuffle only replica nodes
    shuffleFirstN(currentNodes, replicaCount);

    if (replicaCount >= 2) {
      final Token t = this.clusterMetadata.newToken(statement.getPartitioner(), routingKey);
      final Host host1 = (Host) currentNodes[0];
      final Host host2 = (Host) currentNodes[1];
      final int host1ShardId = host1.getShardingInfo().shardId(t);
      final int host2ShardId = host2.getShardingInfo().shardId(t);
      final int host1ShardInflightRequests =
          this.metrics.getPerShardInflightRequestInfo().getValue().get(host1).get(host1ShardId);
      final int host2ShardInflightRequests =
          this.metrics.getPerShardInflightRequestInfo().getValue().get(host2).get(host2ShardId);

      // First choose the host out of the first 2 hosts with the least inflight requests
      if (host2ShardInflightRequests < host1ShardInflightRequests) {
        swap(currentNodes, 0, 1);
      }
    }

    return new AbstractIterator<Host>() {
      private int index = 0;

      @Override
      protected Host computeNext() {
        if (index >= currentNodes.length) return endOfData();

        return (Host) currentNodes[index++];
      }
    };
  }

  @Override
  public void onAdd(Host host) {
    onUp(host);
  }

  @Override
  public void onUp(Host host) {
    this.liveHosts.addIfAbsent(host);
  }

  @Override
  public void onDown(Host host) {
    this.liveHosts.remove(host);
  }

  @Override
  public void onRemove(Host host) {
    onDown(host);
  }

  @Override
  public void close() {
    // Nothing to do
  }
}
