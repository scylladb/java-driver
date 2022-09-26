/*
 * Copyright (C) 2021 ScyllaDB
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

package com.datastax.driver.core.tracing;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import java.net.InetAddress;

/**
 * An abstraction layer over instrumentation library API, corresponding to a logical span in the
 * trace.
 */
public interface TracingInfo {

  /** Final status of the traced execution. */
  enum StatusCode {
    OK,
    ERROR,
  }

  /**
   * Starts a span corresponding to this {@link TracingInfo} object. Must be called exactly once,
   * before any other method, at the beginning of the traced execution.
   *
   * @param name the name given to the span being created.
   */
  void setNameAndStartTime(String name);

  /**
   * Adds provided consistency level to the trace.
   *
   * @param consistency the consistency level to be set.
   */
  void setConsistencyLevel(ConsistencyLevel consistency);

  /**
   * Adds provided statement type to the trace.
   *
   * @param statementType the statement type to be set.
   */
  void setStatementType(String statementType);

  /**
   * Adds provided retry policy to the trace.
   *
   * @param retryPolicy the retry policy to be set.
   */
  void setRetryPolicy(RetryPolicy retryPolicy);

  /**
   * Adds provided load balancing policy to the trace.
   *
   * @param loadBalancingPolicy the load balancing policy to be set.
   */
  void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy);

  /**
   * Adds provided speculative execution policy to the trace.
   *
   * @param speculativeExecutionPolicy the speculative execution policy to be set.
   */
  void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy speculativeExecutionPolicy);

  /**
   * Adds provided batch size to the trace.
   *
   * @param batchSize the batch size to be set.
   */
  void setBatchSize(int batchSize);

  /**
   * Adds provided attempt count to the trace.
   *
   * @param attemptCount the attempt count to be set.
   */
  void setAttemptCount(int attemptCount);

  /**
   * Adds provided shard ID to the trace.
   *
   * @param shardID the shard ID to be set.
   */
  void setShardID(int shardID);

  /**
   * Adds provided peer name to the trace.
   *
   * @param peerName the peer name to be set.
   */
  void setPeerName(String peerName);

  /**
   * Adds provided peer IP to the trace.
   *
   * @param peerIP the peer IP to be set.
   */
  void setPeerIP(InetAddress peerIP);

  /**
   * Adds provided peer port to the trace.
   *
   * @param peerPort the peer port to be set.
   */
  void setPeerPort(int peerPort);

  /**
   * Adds to the trace maximum number of rows fetched in one query.
   *
   * @param fetchSize maximum number of rows fetched in one query.
   */
  void setFetchSize(int fetchSize);

  /**
   * Adds to the trace information whether the query is paged and has more pages pending to be
   * fetched.
   *
   * @param hasMorePages whether the query has more pages to be fetched.
   */
  void setHasMorePages(boolean hasMorePages);

  /**
   * Adds provided number of returned rows to the trace.
   *
   * @param rowsCount the number of returned rows to be set.
   */
  void setRowsCount(int rowsCount);

  /**
   * Adds provided statement text to the trace. If the statement length is greater than the given
   * limit, the statement is truncated to the first {@param limit} characters.
   *
   * @param statement the statement text to be set.
   * @param limit the statement length limit.
   */
  void setStatement(String statement, int limit);

  /**
   * Adds provided keyspace to the trace.
   *
   * @param keyspace the keyspace to be set.
   */
  void setKeyspace(String keyspace);

  /**
   * Adds provided bound values string to the trace.
   *
   * @param boundValues boundValues string to be set.
   */
  void setBoundValues(String boundValues);

  /**
   * Adds provided partition key string to the trace.
   *
   * @param partitionKey the partitionKey to be set.
   */
  void setPartitionKey(String partitionKey);

  /**
   * Adds provided table name to the trace.
   *
   * @param table the table name to be set.
   */
  void setTable(String table);

  /**
   * Adds provided operation type (e.g. SELECT) to the trace.
   *
   * @param operationType the operation type to be set.
   */
  void setOperationType(String operationType);

  /**
   * Adds provided list of contacted replicas to the trace.
   *
   * @param replicas the list of contacted replicas to be set.
   */
  void setReplicas(String replicas);

  /**
   * Records in the trace that the provided exception occured.
   *
   * @param exception the exception to be recorded.
   */
  void recordException(Exception exception);

  /**
   * Sets the final status of the traced execution.
   *
   * @param code the status code to be set.
   */
  void setStatus(StatusCode code);

  /**
   * Sets the final status of the traced execution, with additional description.
   *
   * @param code the status code to be set.
   * @param description the additional description of the status.
   */
  void setStatus(StatusCode code, String description);

  /** Must be always called exactly once at the logical end of traced execution. */
  void tracingFinished();
}
