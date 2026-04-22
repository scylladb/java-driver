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

import static com.datastax.driver.core.Assertions.assertThat;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.EndPoints;
import org.testng.annotations.Test;

public class OperationTimedOutExceptionTest {

  private final EndPoint endPoint = EndPoints.forAddress("127.0.0.1", 9042);

  @Test(groups = "unit")
  public void should_preserve_legacy_five_argument_timeout_constructor() {
    OperationTimedOutException exception = new OperationTimedOutException(endPoint, 10, 5, 7, 11);

    assertThat(exception.getMessage())
        .isEqualTo(
            "[/127.0.0.1:9042] Timed out waiting for server response"
                + " [configured timeout: 10ms, connection in-flight: 5,"
                + " pool pending borrows: 7, pool total in-flight: 11]");
    assertThat(exception.getConfiguredTimeoutMs()).isEqualTo(10);
    assertThat(exception.getElapsedTimeoutMs()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getRetryCount()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getSpeculativeExecutionIndex())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getConnectionInFlight()).isEqualTo(5);
    assertThat(exception.getConnectionShardId()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getHostShardsCount()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolPendingBorrows()).isEqualTo(7);
    assertThat(exception.getPoolPendingBorrowsForShard())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolTotalInFlight()).isEqualTo(11);
    assertThat(exception.getPoolOpenConnectionsForShard())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolMaxConnectionsPerShard())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
  }

  @Test(groups = "unit")
  public void should_store_and_copy_extended_timeout_diagnostics() {
    OperationTimedOutException exception =
        new OperationTimedOutException(endPoint, 10, 17, 2, 3, 5, 1, 12, 7, 4, 11, 2, 6);

    assertThat(exception.getMessage())
        .isEqualTo(
            "[/127.0.0.1:9042] Timed out waiting for server response"
                + " [configured timeout: 10ms, elapsed timeout: 17ms, retry count: 2,"
                + " speculative execution index: 3, connection in-flight: 5,"
                + " connection shard: 1/12, pool pending borrows: 7,"
                + " pool pending borrows for shard: 4, pool total in-flight: 11,"
                + " pool open connections for shard: 2, pool max connections per shard: 6]");
    assertThat(exception.getConfiguredTimeoutMs()).isEqualTo(10);
    assertThat(exception.getElapsedTimeoutMs()).isEqualTo(17);
    assertThat(exception.getRetryCount()).isEqualTo(2);
    assertThat(exception.getSpeculativeExecutionIndex()).isEqualTo(3);
    assertThat(exception.getConnectionInFlight()).isEqualTo(5);
    assertThat(exception.getConnectionShardId()).isEqualTo(1);
    assertThat(exception.getHostShardsCount()).isEqualTo(12);
    assertThat(exception.getPoolPendingBorrows()).isEqualTo(7);
    assertThat(exception.getPoolPendingBorrowsForShard()).isEqualTo(4);
    assertThat(exception.getPoolTotalInFlight()).isEqualTo(11);
    assertThat(exception.getPoolOpenConnectionsForShard()).isEqualTo(2);
    assertThat(exception.getPoolMaxConnectionsPerShard()).isEqualTo(6);

    OperationTimedOutException copy = exception.copy();
    assertThat(copy.getMessage()).isEqualTo(exception.getMessage());
    assertThat(copy.getConfiguredTimeoutMs()).isEqualTo(exception.getConfiguredTimeoutMs());
    assertThat(copy.getElapsedTimeoutMs()).isEqualTo(exception.getElapsedTimeoutMs());
    assertThat(copy.getRetryCount()).isEqualTo(exception.getRetryCount());
    assertThat(copy.getSpeculativeExecutionIndex())
        .isEqualTo(exception.getSpeculativeExecutionIndex());
    assertThat(copy.getConnectionInFlight()).isEqualTo(exception.getConnectionInFlight());
    assertThat(copy.getConnectionShardId()).isEqualTo(exception.getConnectionShardId());
    assertThat(copy.getHostShardsCount()).isEqualTo(exception.getHostShardsCount());
    assertThat(copy.getPoolPendingBorrows()).isEqualTo(exception.getPoolPendingBorrows());
    assertThat(copy.getPoolPendingBorrowsForShard())
        .isEqualTo(exception.getPoolPendingBorrowsForShard());
    assertThat(copy.getPoolTotalInFlight()).isEqualTo(exception.getPoolTotalInFlight());
    assertThat(copy.getPoolOpenConnectionsForShard())
        .isEqualTo(exception.getPoolOpenConnectionsForShard());
    assertThat(copy.getPoolMaxConnectionsPerShard())
        .isEqualTo(exception.getPoolMaxConnectionsPerShard());
    assertThat(copy.getCause()).isSameAs(exception);
  }

  @Test(groups = "unit")
  public void should_leave_extended_timeout_diagnostics_unavailable_for_legacy_constructors() {
    OperationTimedOutException exception = new OperationTimedOutException(endPoint);

    assertThat(exception.getConfiguredTimeoutMs())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getElapsedTimeoutMs()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getRetryCount()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getSpeculativeExecutionIndex())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getConnectionInFlight()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getConnectionShardId()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getHostShardsCount()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolPendingBorrows()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolPendingBorrowsForShard())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolTotalInFlight()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolOpenConnectionsForShard())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
    assertThat(exception.getPoolMaxConnectionsPerShard())
        .isEqualTo(OperationTimedOutException.UNAVAILABLE);
  }

  @Test(groups = "unit")
  public void should_use_default_message_when_custom_message_is_null() {
    OperationTimedOutException exception =
        new OperationTimedOutException(endPoint, null, 10, 17, 2, 3, 5, 1, 12, 7, 4, 11, 2, 6);

    assertThat(exception.getMessage())
        .isEqualTo(
            "[/127.0.0.1:9042] Timed out waiting for server response"
                + " [configured timeout: 10ms, elapsed timeout: 17ms, retry count: 2,"
                + " speculative execution index: 3, connection in-flight: 5,"
                + " connection shard: 1/12, pool pending borrows: 7,"
                + " pool pending borrows for shard: 4, pool total in-flight: 11,"
                + " pool open connections for shard: 2, pool max connections per shard: 6]");
  }
}
