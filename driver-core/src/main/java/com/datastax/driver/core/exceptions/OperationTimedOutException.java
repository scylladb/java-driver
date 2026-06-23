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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.SocketOptions;

/**
 * Thrown on a client-side timeout, i.e. when the client didn't hear back from the server within
 * {@link SocketOptions#getReadTimeoutMillis()}.
 *
 * <p>When the exception is thrown from the request execution path it can carry additional
 * diagnostic fields: the configured timeout, the measured elapsed timeout, retry and speculative
 * execution counters, connection shard and load information, and host-pool contention metrics.
 * These fields are set to {@link #UNAVAILABLE} when the corresponding information was not available
 * at the time the exception was created.
 */
public class OperationTimedOutException extends ConnectionException {

  private static final long serialVersionUID = 0;
  private static final String DEFAULT_MESSAGE = "Timed out waiting for server response";

  /**
   * Sentinel value returned by the diagnostic getters when the corresponding information was not
   * available at the time the exception was created.
   */
  public static final int UNAVAILABLE = -1;

  private final long configuredTimeoutMs;
  private final long elapsedTimeoutMs;
  private final int retryCount;
  private final int speculativeExecutionIndex;
  private final int connectionInFlight;
  private final int connectionShardId;
  private final int hostShardsCount;
  private final int poolPendingBorrows;
  private final int poolPendingBorrowsForShard;
  private final int poolTotalInFlight;
  private final int poolOpenConnectionsForShard;
  private final int poolMaxConnectionsPerShard;

  public OperationTimedOutException(EndPoint endPoint) {
    super(endPoint, "Operation timed out");
    this.configuredTimeoutMs = UNAVAILABLE;
    this.elapsedTimeoutMs = UNAVAILABLE;
    this.retryCount = UNAVAILABLE;
    this.speculativeExecutionIndex = UNAVAILABLE;
    this.connectionInFlight = UNAVAILABLE;
    this.connectionShardId = UNAVAILABLE;
    this.hostShardsCount = UNAVAILABLE;
    this.poolPendingBorrows = UNAVAILABLE;
    this.poolPendingBorrowsForShard = UNAVAILABLE;
    this.poolTotalInFlight = UNAVAILABLE;
    this.poolOpenConnectionsForShard = UNAVAILABLE;
    this.poolMaxConnectionsPerShard = UNAVAILABLE;
  }

  public OperationTimedOutException(EndPoint endPoint, String msg) {
    super(endPoint, msg);
    this.configuredTimeoutMs = UNAVAILABLE;
    this.elapsedTimeoutMs = UNAVAILABLE;
    this.retryCount = UNAVAILABLE;
    this.speculativeExecutionIndex = UNAVAILABLE;
    this.connectionInFlight = UNAVAILABLE;
    this.connectionShardId = UNAVAILABLE;
    this.hostShardsCount = UNAVAILABLE;
    this.poolPendingBorrows = UNAVAILABLE;
    this.poolPendingBorrowsForShard = UNAVAILABLE;
    this.poolTotalInFlight = UNAVAILABLE;
    this.poolOpenConnectionsForShard = UNAVAILABLE;
    this.poolMaxConnectionsPerShard = UNAVAILABLE;
  }

  public OperationTimedOutException(EndPoint endPoint, String msg, Throwable cause) {
    super(endPoint, msg, cause);
    this.configuredTimeoutMs = UNAVAILABLE;
    this.elapsedTimeoutMs = UNAVAILABLE;
    this.retryCount = UNAVAILABLE;
    this.speculativeExecutionIndex = UNAVAILABLE;
    this.connectionInFlight = UNAVAILABLE;
    this.connectionShardId = UNAVAILABLE;
    this.hostShardsCount = UNAVAILABLE;
    this.poolPendingBorrows = UNAVAILABLE;
    this.poolPendingBorrowsForShard = UNAVAILABLE;
    this.poolTotalInFlight = UNAVAILABLE;
    this.poolOpenConnectionsForShard = UNAVAILABLE;
    this.poolMaxConnectionsPerShard = UNAVAILABLE;
  }

  /**
   * Creates an exception with the diagnostic context that was exposed in 3.11.5.14.
   *
   * <p>This overload is retained for source and binary compatibility with callers that were
   * compiled against that release.
   */
  public OperationTimedOutException(
      EndPoint endPoint,
      long configuredTimeoutMs,
      int connectionInFlight,
      int poolPendingBorrows,
      int poolTotalInFlight) {
    this(
        endPoint,
        configuredTimeoutMs,
        UNAVAILABLE,
        UNAVAILABLE,
        UNAVAILABLE,
        connectionInFlight,
        UNAVAILABLE,
        UNAVAILABLE,
        poolPendingBorrows,
        UNAVAILABLE,
        poolTotalInFlight,
        UNAVAILABLE,
        UNAVAILABLE);
  }

  /**
   * Creates an exception with full diagnostic context captured at timeout time.
   *
   * @param endPoint the host that was being queried.
   * @param configuredTimeoutMs the driver-side read timeout that was in effect for this request, in
   *     milliseconds. Either the per-statement override ({@link
   *     com.datastax.driver.core.Statement#setReadTimeoutMillis}) or the driver-wide value from
   *     {@link SocketOptions#getReadTimeoutMillis()}.
   * @param elapsedTimeoutMs the time observed by the driver between the request being written and
   *     the timeout firing, in milliseconds.
   * @param retryCount the request-level retry count associated with the timed out execution.
   * @param speculativeExecutionIndex the index of the speculative execution that timed out, where 0
   *     is the original execution.
   * @param connectionInFlight the number of in-flight requests on the connection at the time of the
   *     timeout.
   * @param connectionShardId the shard the connection was pinned to, if known.
   * @param hostShardsCount the number of shards reported by the host, if known.
   * @param poolPendingBorrows the number of requests waiting to acquire a connection from the pool
   *     at the time of the timeout. A non-zero value indicates pool contention.
   * @param poolPendingBorrowsForShard the number of requests waiting on the same shard queue, if
   *     known.
   * @param poolTotalInFlight the total number of in-flight requests across all connections to this
   *     host at the time of the timeout.
   * @param poolOpenConnectionsForShard the number of open connections on the same shard, if known.
   * @param poolMaxConnectionsPerShard the configured maximum number of connections for a shard, if
   *     known.
   */
  public OperationTimedOutException(
      EndPoint endPoint,
      long configuredTimeoutMs,
      long elapsedTimeoutMs,
      int retryCount,
      int speculativeExecutionIndex,
      int connectionInFlight,
      int connectionShardId,
      int hostShardsCount,
      int poolPendingBorrows,
      int poolPendingBorrowsForShard,
      int poolTotalInFlight,
      int poolOpenConnectionsForShard,
      int poolMaxConnectionsPerShard) {
    this(
        endPoint,
        DEFAULT_MESSAGE,
        configuredTimeoutMs,
        elapsedTimeoutMs,
        retryCount,
        speculativeExecutionIndex,
        connectionInFlight,
        connectionShardId,
        hostShardsCount,
        poolPendingBorrows,
        poolPendingBorrowsForShard,
        poolTotalInFlight,
        poolOpenConnectionsForShard,
        poolMaxConnectionsPerShard);
  }

  /**
   * Same as {@link #OperationTimedOutException(EndPoint, long, long, int, int, int, int, int, int,
   * int, int, int, int)}, but with a custom message.
   */
  public OperationTimedOutException(
      EndPoint endPoint,
      String msg,
      long configuredTimeoutMs,
      long elapsedTimeoutMs,
      int retryCount,
      int speculativeExecutionIndex,
      int connectionInFlight,
      int connectionShardId,
      int hostShardsCount,
      int poolPendingBorrows,
      int poolPendingBorrowsForShard,
      int poolTotalInFlight,
      int poolOpenConnectionsForShard,
      int poolMaxConnectionsPerShard) {
    this(
        endPoint,
        buildMessage(
            msg,
            configuredTimeoutMs,
            elapsedTimeoutMs,
            retryCount,
            speculativeExecutionIndex,
            connectionInFlight,
            connectionShardId,
            hostShardsCount,
            poolPendingBorrows,
            poolPendingBorrowsForShard,
            poolTotalInFlight,
            poolOpenConnectionsForShard,
            poolMaxConnectionsPerShard),
        null,
        configuredTimeoutMs,
        elapsedTimeoutMs,
        retryCount,
        speculativeExecutionIndex,
        connectionInFlight,
        connectionShardId,
        hostShardsCount,
        poolPendingBorrows,
        poolPendingBorrowsForShard,
        poolTotalInFlight,
        poolOpenConnectionsForShard,
        poolMaxConnectionsPerShard);
  }

  /** Private constructor used by {@link #copy()} to preserve all fields and the cause chain. */
  private OperationTimedOutException(
      EndPoint endPoint,
      String msg,
      Throwable cause,
      long configuredTimeoutMs,
      long elapsedTimeoutMs,
      int retryCount,
      int speculativeExecutionIndex,
      int connectionInFlight,
      int connectionShardId,
      int hostShardsCount,
      int poolPendingBorrows,
      int poolPendingBorrowsForShard,
      int poolTotalInFlight,
      int poolOpenConnectionsForShard,
      int poolMaxConnectionsPerShard) {
    super(endPoint, msg, cause);
    this.configuredTimeoutMs = configuredTimeoutMs;
    this.elapsedTimeoutMs = elapsedTimeoutMs;
    this.retryCount = retryCount;
    this.speculativeExecutionIndex = speculativeExecutionIndex;
    this.connectionInFlight = connectionInFlight;
    this.connectionShardId = connectionShardId;
    this.hostShardsCount = hostShardsCount;
    this.poolPendingBorrows = poolPendingBorrows;
    this.poolPendingBorrowsForShard = poolPendingBorrowsForShard;
    this.poolTotalInFlight = poolTotalInFlight;
    this.poolOpenConnectionsForShard = poolOpenConnectionsForShard;
    this.poolMaxConnectionsPerShard = poolMaxConnectionsPerShard;
  }

  /**
   * Returns the driver-side read timeout that was configured for the request, in milliseconds, or
   * {@link #UNAVAILABLE} if not available.
   */
  public long getConfiguredTimeoutMs() {
    return configuredTimeoutMs;
  }

  /**
   * Returns the elapsed time observed by the driver between request dispatch and timeout firing, in
   * milliseconds, or {@link #UNAVAILABLE} if not available.
   */
  public long getElapsedTimeoutMs() {
    return elapsedTimeoutMs;
  }

  /**
   * Returns the request-level retry count associated with the timed out execution, or {@link
   * #UNAVAILABLE} if not available.
   */
  public int getRetryCount() {
    return retryCount;
  }

  /**
   * Returns the speculative execution index associated with the timed out execution, or {@link
   * #UNAVAILABLE} if not available. The original execution is index 0.
   */
  public int getSpeculativeExecutionIndex() {
    return speculativeExecutionIndex;
  }

  /**
   * Returns the number of in-flight requests on the connection at the time of the timeout, or
   * {@link #UNAVAILABLE} if not available.
   */
  public int getConnectionInFlight() {
    return connectionInFlight;
  }

  /**
   * Returns the shard of the connection that timed out, or {@link #UNAVAILABLE} if not available.
   */
  public int getConnectionShardId() {
    return connectionShardId;
  }

  /**
   * Returns the number of shards reported by the host, or {@link #UNAVAILABLE} if not available.
   */
  public int getHostShardsCount() {
    return hostShardsCount;
  }

  /**
   * Returns the number of requests waiting to acquire a connection from the pool at the time of the
   * timeout, or {@link #UNAVAILABLE} if not available. A non-zero value indicates that requests
   * were queued inside the driver before reaching the server.
   */
  public int getPoolPendingBorrows() {
    return poolPendingBorrows;
  }

  /**
   * Returns the number of requests waiting on the shard-specific pool queue, or {@link
   * #UNAVAILABLE} if not available.
   */
  public int getPoolPendingBorrowsForShard() {
    return poolPendingBorrowsForShard;
  }

  /**
   * Returns the total number of in-flight requests across all connections to the host at the time
   * of the timeout, or {@link #UNAVAILABLE} if not available.
   */
  public int getPoolTotalInFlight() {
    return poolTotalInFlight;
  }

  /**
   * Returns the number of open connections on the shard that timed out, or {@link #UNAVAILABLE} if
   * not available.
   */
  public int getPoolOpenConnectionsForShard() {
    return poolOpenConnectionsForShard;
  }

  /**
   * Returns the configured maximum number of connections for a shard, or {@link #UNAVAILABLE} if
   * not available.
   */
  public int getPoolMaxConnectionsPerShard() {
    return poolMaxConnectionsPerShard;
  }

  @Override
  public OperationTimedOutException copy() {
    return new OperationTimedOutException(
        getEndPoint(),
        getRawMessage(),
        this,
        configuredTimeoutMs,
        elapsedTimeoutMs,
        retryCount,
        speculativeExecutionIndex,
        connectionInFlight,
        connectionShardId,
        hostShardsCount,
        poolPendingBorrows,
        poolPendingBorrowsForShard,
        poolTotalInFlight,
        poolOpenConnectionsForShard,
        poolMaxConnectionsPerShard);
  }

  private static String buildMessage(
      String msg,
      long configuredTimeoutMs,
      long elapsedTimeoutMs,
      int retryCount,
      int speculativeExecutionIndex,
      int connectionInFlight,
      int connectionShardId,
      int hostShardsCount,
      int poolPendingBorrows,
      int poolPendingBorrowsForShard,
      int poolTotalInFlight,
      int poolOpenConnectionsForShard,
      int poolMaxConnectionsPerShard) {
    StringBuilder message = new StringBuilder(msg == null ? DEFAULT_MESSAGE : msg);
    boolean hasDiagnostics = false;
    hasDiagnostics =
        appendDiagnostic(message, hasDiagnostics, "configured timeout", configuredTimeoutMs, "ms");
    hasDiagnostics =
        appendDiagnostic(message, hasDiagnostics, "elapsed timeout", elapsedTimeoutMs, "ms");
    hasDiagnostics = appendDiagnostic(message, hasDiagnostics, "retry count", retryCount, null);
    hasDiagnostics =
        appendDiagnostic(
            message,
            hasDiagnostics,
            "speculative execution index",
            speculativeExecutionIndex,
            null);
    hasDiagnostics =
        appendDiagnostic(message, hasDiagnostics, "connection in-flight", connectionInFlight, null);
    hasDiagnostics =
        appendShardDiagnostic(message, hasDiagnostics, connectionShardId, hostShardsCount);
    if (!isAvailable(connectionShardId)) {
      hasDiagnostics =
          appendDiagnostic(message, hasDiagnostics, "host shards count", hostShardsCount, null);
    }
    hasDiagnostics =
        appendDiagnostic(message, hasDiagnostics, "pool pending borrows", poolPendingBorrows, null);
    hasDiagnostics =
        appendDiagnostic(
            message,
            hasDiagnostics,
            "pool pending borrows for shard",
            poolPendingBorrowsForShard,
            null);
    hasDiagnostics =
        appendDiagnostic(message, hasDiagnostics, "pool total in-flight", poolTotalInFlight, null);
    hasDiagnostics =
        appendDiagnostic(
            message,
            hasDiagnostics,
            "pool open connections for shard",
            poolOpenConnectionsForShard,
            null);
    hasDiagnostics =
        appendDiagnostic(
            message,
            hasDiagnostics,
            "pool max connections per shard",
            poolMaxConnectionsPerShard,
            null);
    if (hasDiagnostics) {
      message.append(']');
    }
    return message.toString();
  }

  private static boolean appendDiagnostic(
      StringBuilder message, boolean hasDiagnostics, String label, long value, String suffix) {
    if (!isAvailable(value)) {
      return hasDiagnostics;
    }
    appendSeparator(message, hasDiagnostics);
    message.append(label).append(": ").append(value);
    if (suffix != null) {
      message.append(suffix);
    }
    return true;
  }

  private static boolean appendShardDiagnostic(
      StringBuilder message, boolean hasDiagnostics, int connectionShardId, int hostShardsCount) {
    if (!isAvailable(connectionShardId)) {
      return hasDiagnostics;
    }
    appendSeparator(message, hasDiagnostics);
    message.append("connection shard: ").append(connectionShardId);
    if (isAvailable(hostShardsCount)) {
      message.append('/').append(hostShardsCount);
    }
    return true;
  }

  private static void appendSeparator(StringBuilder message, boolean hasDiagnostics) {
    if (hasDiagnostics) {
      message.append(", ");
    } else {
      message.append(" [");
    }
  }

  private static boolean isAvailable(long value) {
    return value != UNAVAILABLE;
  }
}
