package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.google.common.collect.AbstractIterator;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class InFlightRequestPolicy implements LoadBalancingPolicy {
  private volatile Metrics metrics;
  private volatile Metadata clusterMetadata;
  private volatile ProtocolVersion protocolVersion;
  private volatile CodecRegistry codecRegistry;
  private final CopyOnWriteArrayList<Host> liveHosts = new CopyOnWriteArrayList<Host>();
  private final AtomicInteger liveHostsIndex = new AtomicInteger();
  private final AtomicInteger replicasStartIndex = new AtomicInteger();

  /** Creates a new {@code InflightRequest} policy. */
  public InFlightRequestPolicy() {}

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    this.metrics = cluster.getMetrics();
    this.clusterMetadata = cluster.getMetadata();
    this.protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    this.codecRegistry = cluster.getConfiguration().getCodecRegistry();
    this.liveHosts.addAll(hosts);
    this.liveHostsIndex.set(new Random().nextInt(Math.max(hosts.size(), 1)));
  }

  /**
   * Return the HostDistance for the provided host.
   *
   * <p>This policy consider all nodes as local. TODO: make it a DC aware policy
   *
   * @param host the host of which to return the distance of.
   * @return the HostDistance to {@code host}.
   */
  @Override
  public HostDistance distance(Host host) {
    return HostDistance.LOCAL;
  }

  private Iterator<Host> getRoundRobinIteratorOnLiveHosts() {
    @SuppressWarnings("unchecked")
    final List<Host> hosts = (List<Host>) liveHosts.clone();
    final int startIndex = liveHostsIndex.getAndIncrement();

    // Overflow protection; not theoretically thread safe but should be good enough
    if (startIndex > Integer.MAX_VALUE - 10000) liveHostsIndex.set(0);

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

  /**
   * This policy considers inflight request metric for each node/shard for creating a query plan.
   * The query plan will first return replicas with the target shard having the least amount of
   * inflight requests. It will round-robin over the replicas with equal inflight requests. The
   * policy considers two replicas to be equal if the target shards of both replicas have inflight
   * requests that may differ by at most 32. If the set of replicas for {@linkplain Statement} is
   * empty or the statement's {@linkplain Statement#getRoutingKey(ProtocolVersion, CodecRegistry)
   * routing key} is {@code null} then the query plan will cycle over all live hosts of the cluster
   * in a round-robin fashion.
   *
   * @param loggedKeyspace the currently logged keyspace (the one set through either {@link
   *     Cluster#connect(String)} or by manually doing a {@code USE} query) for the session on which
   *     this plan need to be built. This can be {@code null} if the corresponding session has no
   *     keyspace logged in.
   * @param statement the query for which to build a plan.
   */
  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, final Statement statement) {
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

    final Host[] replicas = replicaSet.toArray(new Host[0]);

    final Token t = this.clusterMetadata.newToken(statement.getPartitioner(), routingKey);
    Arrays.sort(
        replicas,
        new Comparator<Host>() {
          @Override
          public int compare(Host host1, Host host2) {
            int host1ShardId = host1.getShardingInfo().shardId(t);
            int host2ShardId = host2.getShardingInfo().shardId(t);
            int host1ShardInflightRequests =
                metrics.getPerShardInflightRequestInfo().getValue().get(host1).get(host1ShardId);
            int host2ShardInflightRequests =
                metrics.getPerShardInflightRequestInfo().getValue().get(host2).get(host2ShardId);

            return host1ShardInflightRequests - host2ShardInflightRequests;
          }
        });

    final Host[] replicasWithEqualRequests = new Host[replicas.length];

    int minInflightRequestReplicaShardId = replicas[0].getShardingInfo().shardId(t);
    int minInflightRequests =
        metrics
            .getPerShardInflightRequestInfo()
            .getValue()
            .get(replicas[0])
            .get(minInflightRequestReplicaShardId);
    replicasWithEqualRequests[0] = replicas[0];
    int replicasWithEqualRequestsSize = 1;
    for (int i = 1; i < replicas.length; i++) {
      int shardId = replicas[i].getShardingInfo().shardId(t);
      int inflightRequests =
          metrics.getPerShardInflightRequestInfo().getValue().get(replicas[i]).get(shardId);
      if (Math.abs(minInflightRequests - inflightRequests) <= 32) {
        replicasWithEqualRequests[i] = replicas[i];
        replicasWithEqualRequestsSize++;
      } else {
        break;
      }
    }

    this.replicasStartIndex.set(new Random().nextInt(replicasWithEqualRequestsSize));
    final int finalReplicasSize = replicasWithEqualRequestsSize;

    return new AbstractIterator<Host>() {
      private int index = InFlightRequestPolicy.this.replicasStartIndex.get();
      private int remainingSize = finalReplicasSize;

      @Override
      protected Host computeNext() {
        if (remainingSize <= 0) return endOfData();

        remainingSize--;
        int c = index++ % finalReplicasSize;
        if (c < 0) c += finalReplicasSize;
        return replicasWithEqualRequests[c];
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
