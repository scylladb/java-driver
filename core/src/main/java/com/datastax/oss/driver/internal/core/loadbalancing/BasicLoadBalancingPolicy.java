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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.DefaultNodeDistanceEvaluatorHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.OptionalLocalDcHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.OptionalLocalRackHelper;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.DcAgnosticNodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.MultiDcNodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.NodeSet;
import com.datastax.oss.driver.internal.core.loadbalancing.nodeset.SingleDcNodeSet;
import com.datastax.oss.driver.internal.core.metadata.token.TokenLong64;
import com.datastax.oss.driver.internal.core.util.ArrayUtils;
import com.datastax.oss.driver.internal.core.util.collection.CompositeQueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.LazyQueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.base.Predicates;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A basic implementation of {@link LoadBalancingPolicy} that can serve as a building block for more
 * advanced use cases.
 *
 * <p>To activate this policy, modify the {@code basic.load-balancing-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   basic.load-balancing-policy {
 *     class = BasicLoadBalancingPolicy
 *     local-datacenter = datacenter1 # optional
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 *
 * <p><b>Local datacenter</b>: This implementation will only define a local datacenter if it is
 * explicitly set either through configuration or programmatically; if the local datacenter is
 * unspecified, this implementation will effectively act as a datacenter-agnostic load balancing
 * policy and will consider all nodes in the cluster when creating query plans, regardless of their
 * datacenter.
 *
 * <p><b>Query plan</b>: This implementation prioritizes replica nodes over non-replica ones; if
 * more than one replica is available, the replicas will be shuffled. Non-replica nodes will be
 * included in a round-robin fashion. If the local datacenter is defined (see above), query plans
 * will only include local nodes, never remote ones; if it is unspecified however, query plans may
 * contain nodes from different datacenters.
 *
 * <p><b>This class is not recommended for normal users who should always prefer {@link
 * DefaultLoadBalancingPolicy}</b>.
 */
@ThreadSafe
public class BasicLoadBalancingPolicy implements LoadBalancingPolicy {

  public enum RequestRoutingMethod {
    REGULAR,
    PRESERVE_REPLICA_ORDER
  }

  private static final Logger LOG = LoggerFactory.getLogger(BasicLoadBalancingPolicy.class);

  protected static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;
  private static final Object[] EMPTY_NODES = new Object[0];

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  protected final AtomicInteger roundRobinAmount = new AtomicInteger();

  private final int maxNodesPerRemoteDc;
  private final boolean allowDcFailoverForLocalCl;
  private final ConsistencyLevel defaultConsistencyLevel;
  private final RequestRoutingMethod lwtRequestRoutingMethod;

  // private because they should be set in init() and never be modified after
  private volatile DistanceReporter distanceReporter;
  private volatile NodeDistanceEvaluator nodeDistanceEvaluator;
  private volatile String localDc;
  private volatile String localRack;
  private volatile NodeSet liveNodes;
  private final LinkedHashSet<String> preferredRemoteDcs;

  public BasicLoadBalancingPolicy(@NonNull DriverContext context, @NonNull String profileName) {
    this.context = (InternalDriverContext) context;
    profile = context.getConfig().getProfile(profileName);
    logPrefix = context.getSessionName() + "|" + profileName;
    maxNodesPerRemoteDc =
        profile.getInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC);
    allowDcFailoverForLocalCl =
        profile.getBoolean(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS);
    defaultConsistencyLevel =
        this.context
            .getConsistencyLevelRegistry()
            .nameToLevel(profile.getString(DefaultDriverOption.REQUEST_CONSISTENCY));

    preferredRemoteDcs =
        new LinkedHashSet<>(
            profile.getStringList(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_PREFERRED_REMOTE_DCS));
    this.lwtRequestRoutingMethod = parseLwtRequestRoutingMethod();
  }

  @NonNull
  private RequestRoutingMethod parseLwtRequestRoutingMethod() {
    String methodString =
        profile.getString(DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD);
    if (methodString == null) {
      return RequestRoutingMethod.PRESERVE_REPLICA_ORDER;
    }
    try {
      // ROOT, not the default locale: in a Turkish JVM the i of "preserve_replica_order" folds
      // to a dotted capital, so a valid setting would be warned about and silently dropped.
      return RequestRoutingMethod.valueOf(methodString.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "[{}] Unknown request routing method '{}', defaulting to PRESERVE_REPLICA_ORDER",
          logPrefix,
          methodString);
      return RequestRoutingMethod.PRESERVE_REPLICA_ORDER;
    }
  }

  @NonNull
  public RequestRoutingMethod getRequestRoutingMethod(@Nullable Request request) {
    if (request == null) {
      return RequestRoutingMethod.REGULAR;
    }
    RequestRoutingType requestRoutingType = request.getRequestRoutingType();
    if (requestRoutingType == RequestRoutingType.LWT
        || (requestRoutingType == null && hasSerialConsistency(request))) {
      return lwtRequestRoutingMethod;
    } else {
      return RequestRoutingMethod.REGULAR;
    }
  }

  private boolean hasSerialConsistency(@NonNull Request request) {
    if (!(request instanceof Statement)) {
      return false;
    }

    return getEffectiveConsistency((Statement<?>) request).isSerial();
  }

  @NonNull
  private Optional<DriverExecutionProfile> getRequestProfile(@NonNull Request request) {
    DriverExecutionProfile requestProfile = request.getExecutionProfile();
    if (requestProfile != null) {
      return Optional.of(requestProfile);
    }

    String profileName = request.getExecutionProfileName();
    if (profileName != null && !profileName.isEmpty()) {
      return Optional.of(context.getConfig().getProfile(profileName));
    }
    return Optional.of(profile);
  }

  /**
   * Returns the local datacenter name, if known; empty otherwise.
   *
   * <p>When this method returns null, then datacenter awareness is completely disabled. All
   * non-ignored nodes will be considered "local" regardless of their actual datacenters, and will
   * have equal chances of being selected for query plans.
   *
   * <p>After the policy is {@linkplain #init(Map, DistanceReporter) initialized} this method will
   * return the local datacenter that was discovered by calling {@link #discoverLocalDc(Map)}.
   * Before initialization, this method always returns null.
   */
  @Nullable
  public String getLocalDatacenter() {
    return localDc;
  }

  @Nullable
  public String getLocalRack() {
    return localRack;
  }

  /**
   * Returns the maximum number of nodes per remote datacenter this policy will append to a query
   * plan when failing over, as it was configured when the policy was built.
   *
   * <p>Read from the profile in the constructor and never re-read, so a configuration reload does
   * not reach the running policy. Exposed for {@code DRIVER_CONFIG} reporting, which must describe
   * the failover behavior actually in force rather than what the profile currently says.
   *
   * <p>Note this is only the first of the three conditions {@link #maybeAddDcFailover} requires: it
   * also needs a local datacenter, and a request that {@link #isDcFailoverAllowedForRequest}
   * admits.
   */
  public int getMaxNodesPerRemoteDc() {
    return maxNodesPerRemoteDc;
  }

  /** @return The nodes currently considered as live. */
  protected NodeSet getLiveNodes() {
    return liveNodes;
  }

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.distanceReporter = distanceReporter;
    localDc = discoverLocalDc(nodes).orElse(null);
    // If local datacenter is not provided then the rack awareness is disabled
    localRack = localDc != null ? discoverLocalRack(nodes).orElse(null) : null;
    nodeDistanceEvaluator = createNodeDistanceEvaluator(localDc, nodes);
    liveNodes =
        localDc == null
            ? new DcAgnosticNodeSet()
            : maxNodesPerRemoteDc <= 0 ? new SingleDcNodeSet(localDc) : new MultiDcNodeSet();
    for (Node node : nodes.values()) {
      NodeDistance distance = computeNodeDistance(node);
      distanceReporter.setDistance(node, distance);
      if (distance != NodeDistance.IGNORED && node.getState() != NodeState.DOWN) {
        // This includes state == UNKNOWN. If the node turns out to be unreachable, this will be
        // detected when we try to open a pool to it, it will get marked down and this will be
        // signaled back to this policy, which will then remove it from the live set.
        liveNodes.add(node);
      }
    }
  }

  /**
   * Returns the local datacenter, if it can be discovered, or returns {@link Optional#empty empty}
   * otherwise.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * LoadBalancingPolicy.DistanceReporter) initialization}.
   *
   * <p>Implementors may choose to throw {@link IllegalStateException} instead of returning {@link
   * Optional#empty empty}, if they require a local datacenter to be defined in order to operate
   * properly.
   *
   * <p>If this method returns empty, then datacenter awareness will be completely disabled. All
   * non-ignored nodes will be considered "local" regardless of their actual datacenters, and will
   * have equal chances of being selected for query plans.
   *
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to discover the local datacenter.
   * @return The local datacenter, or {@link Optional#empty empty} if none found.
   * @throws IllegalStateException if the local datacenter could not be discovered, and this policy
   *     cannot operate without it.
   */
  @NonNull
  protected Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    return new OptionalLocalDcHelper(context, profile, logPrefix).discoverLocalDc(nodes);
  }

  @NonNull
  protected Optional<String> discoverLocalRack(@NonNull Map<UUID, Node> nodes) {
    return new OptionalLocalRackHelper(profile, logPrefix).discoverLocalRack(nodes);
  }

  /**
   * Creates a new node distance evaluator to use with this policy.
   *
   * <p>This method is called only once, during {@linkplain LoadBalancingPolicy#init(Map,
   * LoadBalancingPolicy.DistanceReporter) initialization}, and only after local datacenter
   * discovery has been attempted.
   *
   * @param localDc The local datacenter that was just discovered, or null if none found.
   * @param nodes All the nodes that were known to exist in the cluster (regardless of their state)
   *     when the load balancing policy was initialized. This argument is provided in case
   *     implementors need to inspect the cluster topology to create the evaluator.
   * @return the distance evaluator to use.
   */
  @NonNull
  protected NodeDistanceEvaluator createNodeDistanceEvaluator(
      @Nullable String localDc, @NonNull Map<UUID, Node> nodes) {
    return new DefaultNodeDistanceEvaluatorHelper(context, profile, logPrefix)
        .createNodeDistanceEvaluator(localDc, nodes);
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    switch (getRequestRoutingMethod(request)) {
      case PRESERVE_REPLICA_ORDER:
        return newQueryPlanPreserveReplicas(request, session);
      case REGULAR:
      default:
        return newQueryPlanRegular(request, session);
    }
  }

  @NonNull
  protected Queue<Node> newQueryPlanRegular(@Nullable Request request, @Nullable Session session) {
    // Take a snapshot since the set is concurrent:
    Object[] currentNodes = liveNodes.dc(localDc).toArray();

    List<Node> allReplicas = getReplicas(request, session);
    int replicaCount = 0; // in currentNodes

    if (!allReplicas.isEmpty()) {
      // Move replicas to the beginning
      for (int i = 0; i < currentNodes.length; i++) {
        Node node = (Node) currentNodes[i];
        if (allReplicas.contains(node)) {
          ArrayUtils.bubbleUp(currentNodes, i, replicaCount);
          replicaCount += 1;
        }
      }

      if (replicaCount > 1) {
        shuffleHead(currentNodes, replicaCount);
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
   * Builds a query plan that preserves replica order: local replicas, remote replicas, local
   * non-replicas (rotated), remote non-replicas (rotated).
   */
  @NonNull
  protected Queue<Node> newQueryPlanPreserveReplicas(
      @Nullable Request request, @Nullable Session session) {
    List<Node> replicas = getReplicas(request, session);
    String localDc = getLocalDatacenter();
    List<Node> queryPlan = new ArrayList<>();

    if (localDc == null) {
      // No local DC: all replicas first, then rotated non-replicas
      List<Node> allNodes = new ArrayList<>();
      for (Object obj : getLiveNodes().dc(null).toArray()) {
        allNodes.add((Node) obj);
      }
      replicas = filterNodesIn(replicas, new LinkedHashSet<>(allNodes));
      queryPlan.addAll(replicas);
      addRotatedNonReplicas(queryPlan, allNodes, replicas, request);
    } else {
      boolean includeRemoteDcs = isDcFailoverAllowedForRequest(request);
      Map<String, List<Node>> nodesByDc =
          includeRemoteDcs
              ? getAllNodesByDc()
              : Collections.singletonMap(localDc, dcNodeList(localDc));
      Set<Node> liveNodesForPlan =
          nodesByDc.values().stream()
              .flatMap(List::stream)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      replicas = filterNodesIn(replicas, liveNodesForPlan);
      addReplicasByDc(queryPlan, replicas, localDc);
      addNonReplicasByDc(queryPlan, nodesByDc, replicas, localDc, request);
    }

    return new SimpleQueryPlan(queryPlan.toArray());
  }

  private List<Node> filterNodesIn(List<Node> nodes, Set<Node> nodesToKeep) {
    return nodes.stream().filter(nodesToKeep::contains).collect(Collectors.toList());
  }

  /** Collect all live nodes grouped by DC, with preferred remote DCs ordered first. */
  private Map<String, List<Node>> getAllNodesByDc() {
    Map<String, List<Node>> nodesByDc = new LinkedHashMap<>();
    Set<String> allDcs = getLiveNodes().dcs();
    // Add preferred remote DCs first (in configured order)
    for (String dc : preferredRemoteDcs) {
      if (allDcs.contains(dc)) {
        nodesByDc.put(dc, dcNodeList(dc));
      }
    }
    // Add remaining DCs (sorted for deterministic ordering)
    allDcs.stream()
        .sorted()
        .filter(dc -> !nodesByDc.containsKey(dc))
        .forEach(dc -> nodesByDc.put(dc, dcNodeList(dc)));
    return nodesByDc;
  }

  private List<Node> dcNodeList(String dc) {
    List<Node> dcNodes = new ArrayList<>();
    for (Object obj : getLiveNodes().dc(dc).toArray()) {
      dcNodes.add((Node) obj);
    }
    return dcNodes;
  }

  /** Add replicas with local DC first, then remote DCs. */
  private void addReplicasByDc(List<Node> queryPlan, List<Node> replicas, String localDc) {
    replicas.stream()
        .filter(r -> Objects.equals(r.getDatacenter(), localDc))
        .forEach(queryPlan::add);
    replicas.stream()
        .filter(r -> !Objects.equals(r.getDatacenter(), localDc))
        .forEach(queryPlan::add);
  }

  /** Add non-replicas with local DC first, then remote DCs (all rotated). */
  private void addNonReplicasByDc(
      List<Node> queryPlan,
      Map<String, List<Node>> nodesByDc,
      List<Node> replicas,
      String localDc,
      Request request) {
    // Local DC non-replicas first
    List<Node> localNodes = nodesByDc.get(localDc);
    if (localNodes != null) {
      addRotatedNonReplicas(queryPlan, localNodes, replicas, request);
    }
    // Remote DC non-replicas
    for (Map.Entry<String, List<Node>> entry : nodesByDc.entrySet()) {
      if (!Objects.equals(entry.getKey(), localDc)) {
        addRotatedNonReplicas(queryPlan, entry.getValue(), replicas, request);
      }
    }
  }

  /** Add non-replica nodes from given list with rotation. */
  private void addRotatedNonReplicas(
      List<Node> queryPlan, List<Node> nodes, List<Node> replicas, Request request) {
    List<Node> nonReplicas =
        nodes.stream().filter(n -> !replicas.contains(n)).collect(Collectors.toList());
    if (!nonReplicas.isEmpty()) {
      rotateNonReplicas(nonReplicas, request);
      queryPlan.addAll(nonReplicas);
    }
  }

  /** Rotates nodes based on routing key (consistent) or randomly. */
  private void rotateNonReplicas(List<Node> nodes, @Nullable Request request) {
    if (nodes.size() <= 1) return;

    int rotationAmount =
        (request != null && request.getRoutingKey() != null)
            ? (request.getRoutingKey().hashCode() & 0x7fffffff) % nodes.size()
            : randomNextInt(nodes.size());

    if (rotationAmount > 0) {
      Collections.rotate(nodes, -rotationAmount);
    }
  }

  @NonNull
  protected List<Node> getReplicas(@Nullable Request request, @Nullable Session session) {
    if (request == null || session == null) {
      return ImmutableList.of();
    }

    Optional<TokenMap> maybeTokenMap = context.getMetadataManager().getMetadata().getTokenMap();
    Optional<TabletMap> maybeTabletMap = context.getMetadataManager().getMetadata().getTabletMap();

    // Note: we're on the hot path and the getXxx methods are potentially more than simple getters,
    // so we only call each method when strictly necessary (which is why the code below looks a bit
    // weird).
    CqlIdentifier keyspace;
    CqlIdentifier table;
    Token token;
    ByteBuffer key;
    Partitioner partitioner;

    try {
      keyspace = request.getKeyspace();
      if (keyspace == null) {
        keyspace = request.getRoutingKeyspace();
      }
      if (keyspace == null && session.getKeyspace().isPresent()) {
        keyspace = session.getKeyspace().get();
      }
      if (keyspace == null) {
        return ImmutableList.of();
      }

      table = request.getRoutingTable();

      token = request.getRoutingToken();
      key = (token == null) ? request.getRoutingKey() : null;
      if (token == null && key == null) {
        return ImmutableList.of();
      }

      partitioner = request.getPartitioner();
      if (partitioner == null && maybeTokenMap.isPresent()) {
        partitioner = maybeTokenMap.get().getPartitioner();
      }
    } catch (Exception e) {
      // Protect against poorly-implemented Request instances
      LOG.error("Unexpected error while trying to compute query plan", e);
      return ImmutableList.of();
    }

    if (token == null && partitioner != null) {
      token = partitioner.hash(key);
    }

    Optional<KeyspaceMetadata> ksMetadata =
        context.getMetadataManager().getMetadata().getKeyspace(keyspace);
    if (ksMetadata.isPresent() && ksMetadata.get().isUsingTablets() && maybeTabletMap.isPresent()) {
      if (table == null) {
        return ImmutableList.of();
      }
      if (token instanceof TokenLong64) {
        Tablet targetTablet =
            maybeTabletMap.get().getTablet(keyspace, table, ((TokenLong64) token).getValue());
        if (targetTablet != null) {
          return targetTablet.getReplicaNodesList();
        }
      }
      return ImmutableList.of();
    }

    if (!maybeTokenMap.isPresent()) {
      return ImmutableList.of();
    }
    TokenMap tokenMap = maybeTokenMap.get();
    return token != null
        ? tokenMap.getReplicasList(keyspace, token)
        : tokenMap.getReplicasList(keyspace, partitioner, key);
  }

  @NonNull
  protected Queue<Node> maybeAddDcFailover(@Nullable Request request, @NonNull Queue<Node> local) {
    if (maxNodesPerRemoteDc <= 0 || localDc == null) {
      return local;
    }
    if (!isDcFailoverAllowedForRequest(request)) {
      return local;
    }
    if (preferredRemoteDcs.isEmpty()) {
      return new CompositeQueryPlan(local, buildRemoteQueryPlanAll());
    }
    return new CompositeQueryPlan(local, buildRemoteQueryPlanPreferred());
  }

  private boolean isDcFailoverAllowedForRequest(@Nullable Request request) {
    if (!allowDcFailoverForLocalCl && request instanceof Statement) {
      return !getEffectiveConsistency((Statement<?>) request).isDcLocal();
    }
    return true;
  }

  @NonNull
  private ConsistencyLevel getEffectiveConsistency(@NonNull Statement<?> statement) {
    ConsistencyLevel consistency = statement.getConsistencyLevel();
    if (consistency != null) {
      return consistency;
    }

    return getRequestProfile(statement)
        .map(
            requestProfile ->
                context
                    .getConsistencyLevelRegistry()
                    .nameToLevel(requestProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY)))
        .orElse(defaultConsistencyLevel);
  }

  private QueryPlan buildRemoteQueryPlanAll() {

    return new LazyQueryPlan() {
      @Override
      protected Object[] computeNodes() {

        Object[] remoteNodes =
            liveNodes.dcs().stream()
                .filter(Predicates.not(Predicates.equalTo(localDc)))
                .flatMap(dc -> liveNodes.dc(dc).stream().limit(maxNodesPerRemoteDc))
                .toArray();
        if (remoteNodes.length == 0) {
          return EMPTY_NODES;
        }
        shuffleHead(remoteNodes, remoteNodes.length);
        return remoteNodes;
      }
    };
  }

  private QueryPlan buildRemoteQueryPlanPreferred() {

    Set<String> dcs = liveNodes.dcs();
    List<String> orderedDcs = Lists.newArrayListWithCapacity(dcs.size());
    orderedDcs.addAll(preferredRemoteDcs);
    orderedDcs.addAll(Sets.difference(dcs, preferredRemoteDcs));

    QueryPlan[] queryPlans =
        orderedDcs.stream()
            .filter(Predicates.not(Predicates.equalTo(localDc)))
            .map(
                (dc) -> {
                  return new LazyQueryPlan() {
                    @Override
                    protected Object[] computeNodes() {
                      Object[] rv = liveNodes.dc(dc).stream().limit(maxNodesPerRemoteDc).toArray();
                      if (rv.length == 0) {
                        return EMPTY_NODES;
                      }
                      shuffleHead(rv, rv.length);
                      return rv;
                    }
                  };
                })
            .toArray(QueryPlan[]::new);

    return new CompositeQueryPlan(queryPlans);
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected int randomNextInt(int bound) {
    return ThreadLocalRandom.current().nextInt(bound);
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected void shuffleHead(Object[] currentNodes, int headLength) {
    ArrayUtils.shuffleHead(currentNodes, headLength);
  }

  /** Exposed as a protected method so that it can be accessed by tests */
  protected void shuffleInRange(Object[] currentNodes, int startIndex, int endIndex) {
    ArrayUtils.shuffleInRange(currentNodes, startIndex, endIndex);
  }

  @Override
  public void onAdd(@NonNull Node node) {
    NodeDistance distance = computeNodeDistance(node);
    // Setting to a non-ignored distance triggers the session to open a pool, which will in turn
    // set the node UP when the first channel gets opened, then #onUp will be called, and the
    // node will be eventually added to the live set.
    distanceReporter.setDistance(node, distance);
    LOG.debug("[{}] {} was added, setting distance to {}", logPrefix, node, distance);
  }

  @Override
  public void onUp(@NonNull Node node) {
    NodeDistance distance = computeNodeDistance(node);
    if (node.getDistance() != distance) {
      distanceReporter.setDistance(node, distance);
    }
    if (distance != NodeDistance.IGNORED && liveNodes.add(node)) {
      LOG.debug("[{}] {} came back UP, added to live set", logPrefix, node);
    }
  }

  @Override
  public void onDown(@NonNull Node node) {
    if (liveNodes.remove(node)) {
      LOG.debug("[{}] {} went DOWN, removed from live set", logPrefix, node);
    }
  }

  @Override
  public void onRemove(@NonNull Node node) {
    if (liveNodes.remove(node)) {
      LOG.debug("[{}] {} was removed, removed from live set", logPrefix, node);
    }
  }

  /**
   * Computes the distance of the given node.
   *
   * <p>This method is called during {@linkplain #init(Map, DistanceReporter) initialization}, when
   * a node {@linkplain #onAdd(Node) is added}, and when a node {@linkplain #onUp(Node) is back UP}.
   */
  protected NodeDistance computeNodeDistance(@NonNull Node node) {
    // We interrogate the custom evaluator every time since it could be dynamic
    // and change its verdict between two invocations of this method.
    NodeDistance distance = nodeDistanceEvaluator.evaluateDistance(node, localDc);
    if (distance != null) {
      return distance;
    }
    // no local DC defined: all nodes are considered LOCAL.
    if (localDc == null) {
      return NodeDistance.LOCAL;
    }
    // otherwise, the node is LOCAL if its datacenter is the local datacenter.
    if (Objects.equals(node.getDatacenter(), localDc)) {
      return NodeDistance.LOCAL;
    }
    // otherwise, the node will be either REMOTE or IGNORED, depending
    // on how many remote nodes we accept per DC.
    if (maxNodesPerRemoteDc > 0) {
      Object[] remoteNodes = liveNodes.dc(node.getDatacenter()).toArray();
      for (int i = 0; i < maxNodesPerRemoteDc; i++) {
        if (i == remoteNodes.length) {
          // there is still room for one more REMOTE node in this DC
          return NodeDistance.REMOTE;
        } else if (remoteNodes[i] == node) {
          return NodeDistance.REMOTE;
        }
      }
    }
    return NodeDistance.IGNORED;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
