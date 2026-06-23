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
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.OperationTimedOutException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReadTimeoutTest extends ScassandraTestBase.PerClassCluster {

  String query = "SELECT foo FROM bar";

  @BeforeMethod(groups = "short")
  public void setup() {
    primingClient.prime(
        queryBuilder().withQuery(query).withThen(then().withFixedDelay(100L)).build());

    // Set default timeout too low
    cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(10);
  }

  @Test(groups = "short", expectedExceptions = OperationTimedOutException.class)
  public void should_use_default_timeout_if_not_overridden_by_statement() {
    session.execute(query);
  }

  @Test(groups = "short")
  public void should_include_timeout_diagnostics() {
    try {
      session.execute(query);
      fail("expected an OperationTimedOutException");
    } catch (OperationTimedOutException e) {
      assertThat(e.getConfiguredTimeoutMs()).isEqualTo(10);
      assertThat(e.getElapsedTimeoutMs()).isGreaterThanOrEqualTo(10);
      assertThat(e.getRetryCount()).isEqualTo(0);
      assertThat(e.getSpeculativeExecutionIndex()).isEqualTo(0);
      assertThat(e.getConnectionInFlight()).isGreaterThanOrEqualTo(1);
      assertThat(e.getPoolPendingBorrows()).isGreaterThanOrEqualTo(0);
      assertThat(e.getPoolTotalInFlight()).isGreaterThanOrEqualTo(1);
      assertThat(e.getConnectionShardId()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
      assertThat(e.getHostShardsCount()).isEqualTo(OperationTimedOutException.UNAVAILABLE);
      assertThat(e.getPoolPendingBorrowsForShard())
          .isEqualTo(OperationTimedOutException.UNAVAILABLE);
      assertThat(e.getPoolOpenConnectionsForShard())
          .isEqualTo(OperationTimedOutException.UNAVAILABLE);
      assertThat(e.getPoolMaxConnectionsPerShard()).isGreaterThanOrEqualTo(1);
      assertThat(e.getMessage())
          .contains("Timed out waiting for server response")
          .contains("configured timeout: 10ms")
          .contains("elapsed timeout:")
          .contains("retry count: 0")
          .contains("speculative execution index: 0");
    }
  }

  @Test(groups = "short")
  public void should_use_statement_timeout_if_overridden() {
    Statement statement = new SimpleStatement(query).setReadTimeoutMillis(10000);
    session.execute(statement);
  }

  @Test(groups = "short")
  public void should_disable_timeout_if_set_to_zero_at_statement_level() {
    Statement statement = new SimpleStatement(query).setReadTimeoutMillis(0);
    session.execute(statement);
  }
}
