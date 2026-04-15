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
 * <p>When the exception is thrown from the request execution path it carries additional diagnostic
 * fields: the driver-side timeout that was configured for the request, the number of in-flight
 * requests on the connection at the moment of the timeout, and the pool-level pending-borrow queue
 * depth and total in-flight count. These fields are set to {@link #UNAVAILABLE} when the exception
 * was not created through that path (e.g. when re-thrown or copied via legacy constructors).
 */
public class OperationTimedOutException extends ConnectionException {

  private static final long serialVersionUID = 0;

  /**
   * Sentinel value returned by {@link #getConfiguredTimeoutMs()}, {@link #getConnectionInFlight()},
   * {@link #getPoolPendingBorrows()}, and {@link #getPoolTotalInFlight()} when the corresponding
   * information was not available at the time the exception was created.
   */
  public static final int UNAVAILABLE = -1;

  private final long configuredTimeoutMs;
  private final int connectionInFlight;
  private final int poolPendingBorrows;
  private final int poolTotalInFlight;

  public OperationTimedOutException(EndPoint endPoint) {
    super(endPoint, "Operation timed out");
    this.configuredTimeoutMs = UNAVAILABLE;
    this.connectionInFlight = UNAVAILABLE;
    this.poolPendingBorrows = UNAVAILABLE;
    this.poolTotalInFlight = UNAVAILABLE;
  }

  public OperationTimedOutException(EndPoint endPoint, String msg) {
    super(endPoint, msg);
    this.configuredTimeoutMs = UNAVAILABLE;
    this.connectionInFlight = UNAVAILABLE;
    this.poolPendingBorrows = UNAVAILABLE;
    this.poolTotalInFlight = UNAVAILABLE;
  }

  public OperationTimedOutException(EndPoint endPoint, String msg, Throwable cause) {
    super(endPoint, msg, cause);
    this.configuredTimeoutMs = UNAVAILABLE;
    this.connectionInFlight = UNAVAILABLE;
    this.poolPendingBorrows = UNAVAILABLE;
    this.poolTotalInFlight = UNAVAILABLE;
  }

  /**
   * Creates an exception with full diagnostic context captured at timeout time.
   *
   * @param endPoint the host that was being queried.
   * @param configuredTimeoutMs the driver-side read timeout that was in effect for this request, in
   *     milliseconds. Either the per-statement override ({@link
   *     com.datastax.driver.core.Statement#setReadTimeoutMillis}) or the driver-wide value from
   *     {@link SocketOptions#getReadTimeoutMillis()}.
   * @param connectionInFlight the number of in-flight requests on the connection at the time of the
   *     timeout.
   * @param poolPendingBorrows the number of requests waiting to acquire a connection from the pool
   *     at the time of the timeout. A non-zero value indicates pool contention.
   * @param poolTotalInFlight the total number of in-flight requests across all connections to this
   *     host at the time of the timeout.
   */
  public OperationTimedOutException(
      EndPoint endPoint,
      long configuredTimeoutMs,
      int connectionInFlight,
      int poolPendingBorrows,
      int poolTotalInFlight) {
    super(
        endPoint,
        String.format(
            "Timed out waiting for server response"
                + " [configured timeout: %dms,"
                + " connection in-flight: %d,"
                + " pool pending borrows: %d,"
                + " pool total in-flight: %d]",
            configuredTimeoutMs, connectionInFlight, poolPendingBorrows, poolTotalInFlight));
    this.configuredTimeoutMs = configuredTimeoutMs;
    this.connectionInFlight = connectionInFlight;
    this.poolPendingBorrows = poolPendingBorrows;
    this.poolTotalInFlight = poolTotalInFlight;
  }

  /** Private constructor used by {@link #copy()} to preserve all fields and the cause chain. */
  private OperationTimedOutException(
      EndPoint endPoint,
      String msg,
      Throwable cause,
      long configuredTimeoutMs,
      int connectionInFlight,
      int poolPendingBorrows,
      int poolTotalInFlight) {
    super(endPoint, msg, cause);
    this.configuredTimeoutMs = configuredTimeoutMs;
    this.connectionInFlight = connectionInFlight;
    this.poolPendingBorrows = poolPendingBorrows;
    this.poolTotalInFlight = poolTotalInFlight;
  }

  /**
   * Returns the driver-side read timeout that was configured for the request, in milliseconds, or
   * {@link #UNAVAILABLE} if not available.
   */
  public long getConfiguredTimeoutMs() {
    return configuredTimeoutMs;
  }

  /**
   * Returns the number of in-flight requests on the connection at the time of the timeout, or
   * {@link #UNAVAILABLE} if not available.
   */
  public int getConnectionInFlight() {
    return connectionInFlight;
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
   * Returns the total number of in-flight requests across all connections to the host at the time
   * of the timeout, or {@link #UNAVAILABLE} if not available.
   */
  public int getPoolTotalInFlight() {
    return poolTotalInFlight;
  }

  @Override
  public OperationTimedOutException copy() {
    return new OperationTimedOutException(
        getEndPoint(),
        getRawMessage(),
        this,
        configuredTimeoutMs,
        connectionInFlight,
        poolPendingBorrows,
        poolTotalInFlight);
  }
}
