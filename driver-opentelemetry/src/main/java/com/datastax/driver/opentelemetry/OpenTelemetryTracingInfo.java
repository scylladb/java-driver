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

package com.datastax.driver.opentelemetry;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.core.tracing.PrecisionLevel;
import com.datastax.driver.core.tracing.TracingInfo;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import java.net.InetAddress;

public class OpenTelemetryTracingInfo implements TracingInfo {
  private Span span;
  private final Tracer tracer;
  private final Context context;
  private boolean tracingStarted;
  private final PrecisionLevel precision;

  protected OpenTelemetryTracingInfo(Tracer tracer, Context context, PrecisionLevel precision) {
    this.tracer = tracer;
    this.context = context;
    this.precision = precision;
    this.tracingStarted = false;
  }

  public Tracer getTracer() {
    return tracer;
  }

  public Context getContext() {
    return context.with(span);
  }

  private void assertStarted() {
    assert tracingStarted : "TracingInfo.setStartTime must be called before any other method";
  }

  public PrecisionLevel getPrecision() {
    return precision;
  }

  @Override
  public void setNameAndStartTime(String name) {
    assert !tracingStarted : "TracingInfo.setStartTime may only be called once.";
    tracingStarted = true;
    span = tracer.spanBuilder(name).setParent(context).startSpan();
  }

  @Override
  public void setConsistencyLevel(ConsistencyLevel consistency) {
    assertStarted();
    span.setAttribute("db.scylladb.consistency_level", consistency.toString());
  }

  public void setStatement(String statement) {
    assertStarted();
    if (currentPrecisionLevelIsAtLeast(PrecisionLevel.FULL)) {
      span.setAttribute("db.scylladb.statement", statement);
    }
  }

  public void setHostname(String hostname) {
    assertStarted();
    if (currentPrecisionLevelIsAtLeast(PrecisionLevel.FULL)) {
      span.setAttribute("net.peer.name", hostname);
    }
  }

  @Override
  public void setStatementType(String statementType) {
    assertStarted();
    span.setAttribute("db.scylladb.statement_type", statementType);
  }

  @Override
  public void setRetryPolicy(RetryPolicy retryPolicy) {
    assertStarted();
    span.setAttribute("db.scylladb.retry_policy", retryPolicy.getClass().getSimpleName());
  }

  @Override
  public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
    assertStarted();
    span.setAttribute(
        "db.scylladb.load_balancing_policy", loadBalancingPolicy.getClass().getSimpleName());
  }

  @Override
  public void setSpeculativeExecutionPolicy(SpeculativeExecutionPolicy speculativeExecutionPolicy) {
    assertStarted();
    span.setAttribute(
        "db.scylladb.speculative_execution_policy",
        speculativeExecutionPolicy.getClass().getSimpleName());
  }

  @Override
  public void setBatchSize(int batchSize) {
    assertStarted();
    span.setAttribute("db.scylladb.batch_size", String.valueOf(batchSize));
  }

  @Override
  public void setAttemptCount(int attemptCount) {
    assertStarted();
    span.setAttribute("db.scylladb.attempt_count", String.valueOf(attemptCount));
  }

  @Override
  public void setShardID(int shardID) {
    assertStarted();
    span.setAttribute("db.scylladb.shard_id", String.valueOf(shardID));
  }

  @Override
  public void setPeerName(String peerName) {
    assertStarted();
    span.setAttribute("net.peer.name", peerName);
  }

  @Override
  public void setPeerIP(InetAddress peerIP) {
    assertStarted();
    span.setAttribute("net.peer.ip", peerIP.getHostAddress());
  }

  @Override
  public void setPeerPort(int peerPort) {
    assertStarted();
    span.setAttribute("net.peer.port", String.valueOf(peerPort));
  }

  @Override
  public void setFetchSize(int fetchSize) {
    assertStarted();
    span.setAttribute("db.scylladb.fetch_size", Integer.toString(fetchSize));
  }

  @Override
  public void setHasMorePages(boolean hasMorePages) {
    assertStarted();
    span.setAttribute("db.scylladb.has_more_pages", Boolean.toString(hasMorePages));
  }

  @Override
  public void setRowsCount(int rowsCount) {
    assertStarted();
    span.setAttribute("db.scylladb.rows_count", Integer.toString(rowsCount));
  }

  @Override
  public void setStatement(String statement, int limit) {
    assertStarted();
    if (currentPrecisionLevelIsAtLeast(PrecisionLevel.FULL)) {
      if (statement.length() > limit) statement = statement.substring(0, limit);
      span.setAttribute("db.scylladb.statement", statement);
    }
  }

  @Override
  public void setKeyspace(String keyspace) {
    assertStarted();
    span.setAttribute("db.scylladb.keyspace", keyspace);
  }

  @Override
  public void setBoundValues(String boundValues) {
    assertStarted();
    span.setAttribute("db.scylladb.bound_values", boundValues);
  }

  @Override
  public void setPartitionKey(String partitionKey) {
    assertStarted();
    span.setAttribute("db.scylladb.partition_key", partitionKey);
  }

  @Override
  public void setTable(String table) {
    assertStarted();
    span.setAttribute("db.scylladb.table", table);
  }

  @Override
  public void setOperationType(String operationType) {
    assertStarted();
    span.setAttribute("db.operation", operationType);
  }

  @Override
  public void setReplicas(String replicas) {
    assertStarted();
    span.setAttribute("db.scylladb.replicas", replicas);
  }

  private io.opentelemetry.api.trace.StatusCode mapStatusCode(StatusCode code) {
    switch (code) {
      case OK:
        return io.opentelemetry.api.trace.StatusCode.OK;
      case ERROR:
        return io.opentelemetry.api.trace.StatusCode.ERROR;
    }
    return null;
  }

  @Override
  public void recordException(Exception exception) {
    assertStarted();
    span.recordException(exception);
  }

  @Override
  public void setStatus(StatusCode code, String description) {
    assertStarted();
    span.setStatus(mapStatusCode(code), description);
  }

  @Override
  public void setStatus(StatusCode code) {
    assertStarted();
    span.setStatus(mapStatusCode(code));
  }

  @Override
  public void tracingFinished() {
    assertStarted();
    span.end();
  }

  private boolean currentPrecisionLevelIsAtLeast(PrecisionLevel requiredLevel) {
    return requiredLevel.compareTo(precision) <= 0;
  }
}
