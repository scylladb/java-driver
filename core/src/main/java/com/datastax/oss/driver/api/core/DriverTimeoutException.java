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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Thrown when a driver request timed out.
 *
 * <p>When thrown from the request execution path the exception carries a per-node diagnostic
 * snapshot captured at the moment the timer fires (see {@link #getNodeDiagnostics()}). This
 * information is also embedded in the exception message for easy log-based diagnosis.
 */
public class DriverTimeoutException extends DriverException {

  /**
   * Sentinel value used in {@link NodeDiagnostics} fields when the corresponding data was not
   * available at the time the exception was created (e.g. the pool had already been removed).
   */
  public static final int UNAVAILABLE = -1;

  /**
   * Per-node diagnostic snapshot captured at timeout time.
   *
   * <p>Fields:
   *
   * <ul>
   *   <li>{@link #getNodeState()}: the state of the node (UP, DOWN, etc.) at timeout time.
   *   <li>{@link #getNodeDistance()}: the distance assigned to the node by the load-balancing
   *       policy (LOCAL, REMOTE, or IGNORED).
   *   <li>{@link #getDatacenter()}: the datacenter the node belongs to.
   *   <li>{@link #getChannelInFlight()}: requests currently awaiting a response on the specific
   *       connection used for this request.
   *   <li>{@link #getPoolSize()}: number of active connections in the pool ({@link #UNAVAILABLE} if
   *       the pool was already removed).
   *   <li>{@link #getPoolInFlight()}: total in-flight across all connections to this host ({@link
   *       #UNAVAILABLE} if the pool was already removed).
   *   <li>{@link #getPoolAvailableIds()}: remaining stream IDs available to send new requests; a
   *       low value indicates pool contention ({@link #UNAVAILABLE} if pool was already removed).
   *   <li>{@link #getPoolOrphanedIds()}: stream IDs from previously timed-out or cancelled requests
   *       that cannot be released yet; a high value indicates stale stream ID accumulation ({@link
   *       #UNAVAILABLE} if pool was already removed).
   * </ul>
   *
   * <p><b>Diagnosing failure modes:</b>
   *
   * <ul>
   *   <li>{@code poolAvailableIds} near zero → pool contention; requests queuing inside the driver
   *       before reaching the server.
   *   <li>{@code poolAvailableIds} normal + high {@code channelInFlight} → server is slow; requests
   *       were sent but not answered within the timeout.
   *   <li>High {@code poolOrphanedIds} → previous timeouts consumed stream IDs that the driver is
   *       still waiting to reclaim.
   *   <li>{@code poolSize} below expected → pool is degraded; some connections have been lost.
   *   <li>{@code nodeState} DOWN or FORCED_DOWN → node is known to be unavailable.
   * </ul>
   */
  public static final class NodeDiagnostics {

    @NonNull private final EndPoint endPoint;
    @Nullable private final NodeState nodeState;
    @Nullable private final NodeDistance nodeDistance;
    @Nullable private final String datacenter;
    private final int channelInFlight;
    private final int poolSize;
    private final int poolInFlight;
    private final int poolAvailableIds;
    private final int poolOrphanedIds;

    /**
     * Creates a full diagnostic snapshot (pool was available at timeout time).
     *
     * @param endPoint the endpoint of the node.
     * @param nodeState the state of the node at timeout time.
     * @param nodeDistance the distance assigned to the node by the load-balancing policy.
     * @param datacenter the datacenter the node belongs to.
     * @param channelInFlight in-flight count on the specific channel.
     * @param poolSize number of active connections in the pool.
     * @param poolInFlight total in-flight across the pool for this host.
     * @param poolAvailableIds remaining stream IDs available in the pool.
     * @param poolOrphanedIds orphaned stream IDs in the pool.
     */
    public NodeDiagnostics(
        @NonNull EndPoint endPoint,
        @Nullable NodeState nodeState,
        @Nullable NodeDistance nodeDistance,
        @Nullable String datacenter,
        int channelInFlight,
        int poolSize,
        int poolInFlight,
        int poolAvailableIds,
        int poolOrphanedIds) {
      this.endPoint = endPoint;
      this.nodeState = nodeState;
      this.nodeDistance = nodeDistance;
      this.datacenter = datacenter;
      this.channelInFlight = channelInFlight;
      this.poolSize = poolSize;
      this.poolInFlight = poolInFlight;
      this.poolAvailableIds = poolAvailableIds;
      this.poolOrphanedIds = poolOrphanedIds;
    }

    /**
     * Creates a partial diagnostic snapshot for when the pool was unavailable at timeout time. The
     * pool-related fields ({@link #getPoolSize()}, {@link #getPoolInFlight()}, {@link
     * #getPoolAvailableIds()}, {@link #getPoolOrphanedIds()}) will be {@link
     * DriverTimeoutException#UNAVAILABLE}.
     *
     * @param endPoint the endpoint of the node.
     * @param nodeState the state of the node at timeout time.
     * @param nodeDistance the distance assigned to the node by the load-balancing policy.
     * @param datacenter the datacenter the node belongs to.
     * @param channelInFlight in-flight count on the specific channel.
     */
    public NodeDiagnostics(
        @NonNull EndPoint endPoint,
        @Nullable NodeState nodeState,
        @Nullable NodeDistance nodeDistance,
        @Nullable String datacenter,
        int channelInFlight) {
      this(
          endPoint,
          nodeState,
          nodeDistance,
          datacenter,
          channelInFlight,
          UNAVAILABLE,
          UNAVAILABLE,
          UNAVAILABLE,
          UNAVAILABLE);
    }

    /**
     * Creates a diagnostic snapshot using pre-computed pool stats. Pass {@link
     * DriverTimeoutException#UNAVAILABLE} for pool fields when the pool was not available at
     * timeout time.
     *
     * @param endPoint the endpoint of the node.
     * @param nodeState the state of the node at timeout time.
     * @param nodeDistance the distance assigned to the node by the load-balancing policy.
     * @param datacenter the datacenter the node belongs to.
     * @param channelInFlight in-flight count on the specific channel.
     * @param poolSize number of active connections in the pool, or {@link
     *     DriverTimeoutException#UNAVAILABLE}.
     * @param poolInFlight total in-flight across the pool, or {@link
     *     DriverTimeoutException#UNAVAILABLE}.
     * @param poolAvailableIds remaining stream IDs in the pool, or {@link
     *     DriverTimeoutException#UNAVAILABLE}.
     * @param poolOrphanedIds orphaned stream IDs in the pool, or {@link
     *     DriverTimeoutException#UNAVAILABLE}.
     */
    @NonNull
    public static NodeDiagnostics of(
        @NonNull EndPoint endPoint,
        @Nullable NodeState nodeState,
        @Nullable NodeDistance nodeDistance,
        @Nullable String datacenter,
        int channelInFlight,
        int poolSize,
        int poolInFlight,
        int poolAvailableIds,
        int poolOrphanedIds) {
      return new NodeDiagnostics(
          endPoint,
          nodeState,
          nodeDistance,
          datacenter,
          channelInFlight,
          poolSize,
          poolInFlight,
          poolAvailableIds,
          poolOrphanedIds);
    }

    /** Returns the endpoint of the node that had in-flight requests at timeout time. */
    @NonNull
    public EndPoint getEndPoint() {
      return endPoint;
    }

    /**
     * Returns the state of the node at timeout time (e.g. UP, DOWN, FORCED_DOWN), or {@code null}
     * if not available.
     */
    @Nullable
    public NodeState getNodeState() {
      return nodeState;
    }

    /**
     * Returns the distance assigned to this node by the load-balancing policy at timeout time (e.g.
     * LOCAL, REMOTE, IGNORED), or {@code null} if not available.
     */
    @Nullable
    public NodeDistance getNodeDistance() {
      return nodeDistance;
    }

    /** Returns the datacenter this node belongs to, or {@code null} if not available. */
    @Nullable
    public String getDatacenter() {
      return datacenter;
    }

    /**
     * Returns the number of in-flight requests on the specific connection at timeout time, or
     * {@link DriverTimeoutException#UNAVAILABLE} if not available.
     */
    public int getChannelInFlight() {
      return channelInFlight;
    }

    /**
     * Returns the number of active connections in the pool at timeout time, or {@link
     * DriverTimeoutException#UNAVAILABLE} if the pool was no longer available.
     */
    public int getPoolSize() {
      return poolSize;
    }

    /**
     * Returns the total number of in-flight requests across all connections to this host at timeout
     * time, or {@link DriverTimeoutException#UNAVAILABLE} if the pool was no longer available.
     */
    public int getPoolInFlight() {
      return poolInFlight;
    }

    /**
     * Returns the number of remaining stream IDs available in the pool at timeout time, or {@link
     * DriverTimeoutException#UNAVAILABLE} if the pool was no longer available. A low value
     * indicates pool contention.
     */
    public int getPoolAvailableIds() {
      return poolAvailableIds;
    }

    /**
     * Returns the number of orphaned stream IDs in the pool at timeout time, or {@link
     * DriverTimeoutException#UNAVAILABLE} if the pool was no longer available. A high value
     * indicates stale stream ID accumulation from previous timeouts.
     */
    public int getPoolOrphanedIds() {
      return poolOrphanedIds;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(endPoint);
      sb.append(" [");
      if (nodeState != null) {
        sb.append("state: ").append(nodeState).append(", ");
      }
      if (nodeDistance != null) {
        sb.append("distance: ").append(nodeDistance).append(", ");
      }
      if (datacenter != null) {
        sb.append("dc: ").append(datacenter).append(", ");
      }
      sb.append("channel in-flight: ").append(channelInFlight).append(", ");
      if (poolInFlight == UNAVAILABLE) {
        sb.append("pool: n/a");
      } else {
        sb.append("pool size: ")
            .append(poolSize)
            .append(", pool in-flight: ")
            .append(poolInFlight)
            .append(", pool available ids: ")
            .append(poolAvailableIds)
            .append(", pool orphaned ids: ")
            .append(poolOrphanedIds);
      }
      sb.append("]");
      return sb.toString();
    }
  }

  @Nullable private final NodeDiagnostics nodeDiagnostics;

  /**
   * Creates an exception with a plain message and no node diagnostics. Used for cases where the
   * diagnostic data is unavailable (e.g. no nodes were in-flight at timeout time).
   *
   * @param message the exception message.
   */
  public DriverTimeoutException(@NonNull String message) {
    this(message, (NodeDiagnostics) null);
  }

  /**
   * Creates an exception with per-node diagnostic context captured at timeout time. The message is
   * generated automatically from {@code baseMessage} and the diagnostic data.
   *
   * @param baseMessage the base timeout message (e.g. {@code "Query timed out after PT0.5S"}).
   * @param nodeDiagnostics per-node diagnostic snapshot; may be {@code null} if unavailable, in
   *     which case no node information is appended to the message.
   */
  public DriverTimeoutException(
      @NonNull String baseMessage, @Nullable NodeDiagnostics nodeDiagnostics) {
    this(buildMessage(baseMessage, nodeDiagnostics), nodeDiagnostics, null);
  }

  private DriverTimeoutException(
      String message, @Nullable NodeDiagnostics nodeDiagnostics, ExecutionInfo executionInfo) {
    super(message, executionInfo, null, true);
    this.nodeDiagnostics = nodeDiagnostics;
  }

  /**
   * Returns the per-node diagnostic snapshot captured at timeout time, or {@code null} if not
   * available.
   */
  @Nullable
  public NodeDiagnostics getNodeDiagnostics() {
    return nodeDiagnostics;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new DriverTimeoutException(getMessage(), nodeDiagnostics, getExecutionInfo());
  }

  private static String buildMessage(
      @NonNull String baseMessage, @Nullable NodeDiagnostics nodeDiagnostics) {
    if (nodeDiagnostics == null) {
      return baseMessage;
    }
    return baseMessage + " — node in flight: " + nodeDiagnostics;
  }
}
