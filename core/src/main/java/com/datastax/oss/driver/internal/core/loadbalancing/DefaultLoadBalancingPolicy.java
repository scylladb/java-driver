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
package com.datastax.oss.driver.internal.core.loadbalancing;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.MandatoryLocalDcHelper;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.MapMaker;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default load balancing policy implementation.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = DefaultLoadBalancingPolicy
 *     local-datacenter = datacenter1
 *   }
 * }
 * </pre>
 *
 * <p>See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation requires a local datacenter to be defined,
 * otherwise it will throw an {@link IllegalStateException}. A local datacenter can be supplied
 * either:
 *
 * <ol>
 *   <li>Programmatically with {@link
 *       com.datastax.oss.driver.api.core.session.SessionBuilder#withLocalDatacenter(String)
 *       SessionBuilder#withLocalDatacenter(String)};
 *   <li>Through configuration, by defining the option {@link
 *       DefaultDriverOption#LOAD_BALANCING_LOCAL_DATACENTER
 *       basic.load-balancing-policy.local-datacenter};
 *   <li>Or implicitly, if and only if no explicit contact points were provided: in this case this
 *       implementation will infer the local datacenter from the implicit contact point (localhost).
 * </ol>
 *
 * <p><b>Query plan</b>: This implementation prioritizes replica nodes over non-replica ones; if
 * more than one replica is available, the replicas will be shuffled; if more than 2 replicas are
 * available, they will be ordered from most healthy to least healthy ("Power of 2 choices" or busy
 * node avoidance algorithm). Non-replica nodes will be included in a round-robin fashion. If the
 * local datacenter is defined (see above), query plans will only include local nodes, never remote
 * ones; if it is unspecified however, query plans may contain nodes from different datacenters.
 */
@ThreadSafe
public class DefaultLoadBalancingPolicy extends BasicLoadBalancingPolicy implements RequestTracker {

  public enum RequestRoutingMethod {
    REGULAR,
    PRESERVE_REPLICA_ORDER
  }

  private static final Logger LOG = LoggerFactory.getLogger(DefaultLoadBalancingPolicy.class);

  private static final long NEWLY_UP_INTERVAL_NANOS = MINUTES.toNanos(1);
  private static final int MAX_IN_FLIGHT_THRESHOLD = 10;
  private static final long RESPONSE_COUNT_RESET_INTERVAL_NANOS = MILLISECONDS.toNanos(200);

  protected final ConcurrentMap<Node, NodeResponseRateSample> responseTimes;
  protected final Map<Node, Long> upTimes = new ConcurrentHashMap<>();
  private final boolean avoidSlowReplicas;
  private final RequestRoutingMethod lwtRequestRoutingMethod;

  public DefaultLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    super(context, profileName);
    this.avoidSlowReplicas =
        profile.getBoolean(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true);
    this.lwtRequestRoutingMethod = getDefaultLWTRequestRoutingMethod();
    this.responseTimes = new MapMaker().weakKeys().makeMap();
  }

  @NonNull
  private RequestRoutingMethod getDefaultLWTRequestRoutingMethod() {
    String methodString =
        profile.getString(DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD);
    try {
      return RequestRoutingMethod.valueOf(methodString.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "[{}] Unknown request routing method '{}', defaulting to PRESERVE_REPLICA_ORDER",
          logPrefix,
          methodString);
      return RequestRoutingMethod.PRESERVE_REPLICA_ORDER;
    }
  }

  @NonNull
  @Override
  public Optional<RequestTracker> getRequestTracker() {
    if (avoidSlowReplicas) {
      return Optional.of(this);
    } else {
      return Optional.empty();
    }
  }

  @NonNull
  @Override
  protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    return new MandatoryLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
  }

  @NonNull
  public RequestRoutingMethod getDefaultLWTRequestRoutingMethod(@Nullable Request request) {
    if (request == null) {
      return RequestRoutingMethod.REGULAR;
    }
    switch (request.getRequestRoutingType()) {
      case LWT:
        return lwtRequestRoutingMethod;
      case REGULAR:
      default:
        return RequestRoutingMethod.REGULAR;
    }
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    switch (getDefaultLWTRequestRoutingMethod(request)) {
      case PRESERVE_REPLICA_ORDER:
        return newQueryPlanPreserveReplicas(request, session);
      default:
        return newQueryPlanRegular(request, session);
    }
  }

  /**
   * Builds a query plan that preserves the replica order as returned by the load balancing
   * strategy, while pushing non-local replicas after local ones.
   */
  @NonNull
  public Queue<Node> newQueryPlanPreserveReplicas(
      @Nullable Request request, @Nullable Session session) {
    List<Node> replicas = getReplicas(request, session);
    String localDc = getLocalDatacenter();
    if (localDc == null || replicas.isEmpty()) {
      return new SimpleQueryPlan(replicas.toArray());
    }

    return new SimpleQueryPlan(moveNonLocalReplicasToTheEnd(replicas, localDc));
  }

  /**
   * Builds a query plan that prioritizes local replicas, shuffles them for balance, and then
   * round-robins the remaining local nodes.
   */
  @NonNull
  public Queue<Node> newQueryPlanRegular(@Nullable Request request, @Nullable Session session) {
    List<Node> replicas = getReplicas(request, session);
    Object[] currentNodes = getLiveNodes().dc(getLocalDatacenter()).toArray();
    int replicaCount = 0; // in currentNodes
    if (!replicas.isEmpty()) {
      Pair<Integer, Integer> counts = moveReplicasToFront(currentNodes, replicas);
      replicaCount = counts.getLeft();

      int localRackReplicaCount = counts.getRight(); // in currentNodes

      if (replicaCount > 1) {
        shuffleLocalRackReplicasAndReplicas(currentNodes, replicaCount, localRackReplicaCount);

        if (replicaCount > 2 && avoidSlowReplicas) {
          avoidSlowReplicas(Objects.requireNonNull(session), currentNodes, replicaCount);
        }
      }
    }

    LOG.trace("[{}] Prioritizing {} local replicas", logPrefix, replicaCount);

    // Round-robin the remaining nodes
    ArrayUtils.rotate(
        currentNodes,
        replicaCount,
        currentNodes.length - replicaCount,
        roundRobinAmount.getAndUpdate(INCREMENT));

    QueryPlan plan = currentNodes.length == 0 ? QueryPlan.EMPTY : new SimpleQueryPlan(currentNodes);
    return maybeAddDcFailover(request, plan);
  }

  /**
   * Returns a replica array with local-datacenter replicas first and remote replicas preserved at
   * the end.
   */
  private static Object[] moveNonLocalReplicasToTheEnd(List<Node> replicas, String localDc) {
    Object[] orderedReplicas = new Object[replicas.size()];
    int index = 0;
    for (Node replica : replicas) {
      if (Objects.equals(replica.getDatacenter(), localDc)) {
        orderedReplicas[index++] = replica;
      }
    }
    for (Node replica : replicas) {
      if (!Objects.equals(replica.getDatacenter(), localDc)) {
        orderedReplicas[index++] = replica;
      }
    }
    return orderedReplicas;
  }

  private Pair<Integer, Integer> moveReplicasToFront(
      Object[] currentNodes, List<Node> allReplicas) {
    int replicaCount = 0, localRackReplicaCount = 0;
    for (int i = 0; i < currentNodes.length; i++) {
      Node node = (Node) currentNodes[i];
      if (allReplicas.contains(node)) {
        if (Objects.equals(node.getRack(), getLocalRack())
            && Objects.equals(node.getDatacenter(), getLocalDatacenter())) {
          ArrayUtils.bubbleUp(currentNodes, i, localRackReplicaCount);
          localRackReplicaCount++;
        } else {
          ArrayUtils.bubbleUp(currentNodes, i, replicaCount);
        }
        replicaCount++;
      }
    }
    return Pair.of(replicaCount, localRackReplicaCount);
  }

  private void shuffleLocalRackReplicasAndReplicas(
      Object[] currentNodes, int replicaCount, int localRackReplicaCount) {
    if (getLocalRack() != null && localRackReplicaCount > 0) {
      // Shuffle only replicas that are in the local rack
      shuffleHead(currentNodes, localRackReplicaCount);
      // Shuffles only replicas that are not in local rack
      shuffleInRange(currentNodes, localRackReplicaCount, replicaCount - 1);
    } else {
      shuffleHead(currentNodes, replicaCount);
    }
  }

  private void avoidSlowReplicas(
      @NonNull Session session, Object[] currentNodes, int replicaCount) {
    // Test replicas health
    Node newestUpReplica = null;
    BitSet unhealthyReplicas = null; // bit mask storing indices of unhealthy replicas
    long mostRecentUpTimeNanos = -1;
    long now = nanoTime();
    for (int i = 0; i < replicaCount; i++) {
      Node node = (Node) currentNodes[i];
      assert node != null;
      Long upTimeNanos = upTimes.get(node);
      if (upTimeNanos != null
          && now - upTimeNanos - NEWLY_UP_INTERVAL_NANOS < 0
          && upTimeNanos - mostRecentUpTimeNanos > 0) {
        newestUpReplica = node;
        mostRecentUpTimeNanos = upTimeNanos;
      }
      if (newestUpReplica == null && isUnhealthy(node, session, now)) {
        if (unhealthyReplicas == null) {
          unhealthyReplicas = new BitSet(replicaCount);
        }
        unhealthyReplicas.set(i);
      }
    }

    // When:
    // - there isn't any newly UP replica and
    // - there is one or more unhealthy replicas and
    // - there is a majority of healthy replicas
    int unhealthyReplicasCount = unhealthyReplicas == null ? 0 : unhealthyReplicas.cardinality();
    if (newestUpReplica == null
        && unhealthyReplicasCount > 0
        && unhealthyReplicasCount < (replicaCount / 2.0)) {

      // Reorder the unhealthy replicas to the back of the list
      // Start from the back of the replicas, then move backwards;
      // stop once all unhealthy replicas are moved to the back.
      int counter = 0;
      for (int i = replicaCount - 1; i >= 0 && counter < unhealthyReplicasCount; i--) {
        if (unhealthyReplicas.get(i)) {
          ArrayUtils.bubbleDown(currentNodes, i, replicaCount - 1 - counter);
          counter++;
        }
      }
    }

    // When:
    // - there is a newly UP replica and
    // - the replica in first or second position is the most recent replica marked as UP and
    // - dice roll 1d4 != 1
    else if ((newestUpReplica == currentNodes[0] || newestUpReplica == currentNodes[1])
        && diceRoll1d4() != 1) {

      // Send it to the back of the replicas
      ArrayUtils.bubbleDown(
          currentNodes, newestUpReplica == currentNodes[0] ? 0 : 1, replicaCount - 1);
    }

    // Reorder the first two replicas in the shuffled list based on the number of
    // in-flight requests
    if (getInFlight((Node) currentNodes[0], session)
        > getInFlight((Node) currentNodes[1], session)) {
      ArrayUtils.swap(currentNodes, 0, 1);
    }
  }

  @Override
  public void onNodeSuccess(
      @NonNull Request request,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    updateResponseTimes(node);
  }

  @Override
  public void onNodeError(
      @NonNull Request request,
      @NonNull Throwable error,
      long latencyNanos,
      @NonNull DriverExecutionProfile executionProfile,
      @NonNull Node node,
      @NonNull String logPrefix) {
    updateResponseTimes(node);
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected long nanoTime() {
    return System.nanoTime();
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected int diceRoll1d4() {
    return ThreadLocalRandom.current().nextInt(4);
  }

  protected boolean isUnhealthy(@NonNull Node node, @NonNull Session session, long now) {
    return isBusy(node, session) && isResponseRateInsufficient(node, now);
  }

  protected boolean isBusy(@NonNull Node node, @NonNull Session session) {
    return getInFlight(node, session) >= MAX_IN_FLIGHT_THRESHOLD;
  }

  protected boolean isResponseRateInsufficient(@NonNull Node node, long now) {
    NodeResponseRateSample sample = responseTimes.get(node);
    return !(sample == null || sample.hasSufficientResponses(now));
  }

  /**
   * Synchronously updates the response times for the given node. It is synchronous because the
   * {@link #DefaultLoadBalancingPolicy(com.datastax.oss.driver.api.core.context.DriverContext,
   * java.lang.String) CacheLoader.load} assigned is synchronous.
   *
   * @param node The node to update.
   */
  protected void updateResponseTimes(@NonNull Node node) {
    this.responseTimes.compute(node, (k, v) -> v == null ? new NodeResponseRateSample() : v.next());
  }

  protected int getInFlight(@NonNull Node node, @NonNull Session session) {
    // The cast will always succeed because there's no way to replace the internal session impl
    ChannelPool pool = ((DefaultSession) session).getPools().get(node);
    // Note: getInFlight() includes orphaned ids, which is what we want as we need to account
    // for requests that were cancelled or timed out (since the node is likely to still be
    // processing them).
    return (pool == null) ? 0 : pool.getInFlight();
  }

  protected class NodeResponseRateSample {

    @VisibleForTesting protected final long oldest;
    @VisibleForTesting protected final OptionalLong newest;

    private NodeResponseRateSample() {
      long now = nanoTime();
      this.oldest = now;
      this.newest = OptionalLong.empty();
    }

    private NodeResponseRateSample(long oldestSample) {
      this(oldestSample, nanoTime());
    }

    private NodeResponseRateSample(long oldestSample, long newestSample) {
      this.oldest = oldestSample;
      this.newest = OptionalLong.of(newestSample);
    }

    @VisibleForTesting
    protected NodeResponseRateSample(AtomicLongArray times) {
      assert times.length() >= 1;
      this.oldest = times.get(0);
      this.newest = (times.length() > 1) ? OptionalLong.of(times.get(1)) : OptionalLong.empty();
    }

    // Our newest sample becomes the oldest in the next generation
    private NodeResponseRateSample next() {
      return new NodeResponseRateSample(this.getNewestValidSample(), nanoTime());
    }

    // If we have a pair of values return the newest, otherwise we have just one value... so just
    // return it
    private long getNewestValidSample() {
      return this.newest.orElse(this.oldest);
    }

    // response rate is considered insufficient when less than 2 responses were obtained in
    // the past interval delimited by RESPONSE_COUNT_RESET_INTERVAL_NANOS.
    private boolean hasSufficientResponses(long now) {
      // If we only have one sample it's an automatic failure
      if (!this.newest.isPresent()) return true;
      long threshold = now - RESPONSE_COUNT_RESET_INTERVAL_NANOS;
      return this.oldest - threshold >= 0;
    }
  }
}
