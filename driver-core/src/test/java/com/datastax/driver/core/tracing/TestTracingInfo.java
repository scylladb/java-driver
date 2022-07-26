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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

public class TestTracingInfo implements TracingInfo {

  private final PrecisionLevel precision;
  private TracingInfo parent = null;

  private Boolean spanStarted = false;
  private Boolean spanFinished = false;
  private String spanName;
  private ConsistencyLevel consistencyLevel;
  private String statement;
  private String statementType;
  private Collection<Exception> exceptions;
  private StatusCode statusCode;
  private String description;
  private InetAddress peerIP;
  private RetryPolicy retryPolicy;
  private LoadBalancingPolicy loadBalancingPolicy;
  private Integer batchSize;
  private Integer retryCount;
  private Integer shardID;
  private String peerName;
  private Integer peerPort;
  private Boolean queryPaged;
  private Integer rowsCount;
  private String keyspace;
  private String partitionKey;
  private String table;
  private String operationType;
  private String replicas;

  public TestTracingInfo(PrecisionLevel precision) {
    this.precision = precision;
  }

  public TestTracingInfo(PrecisionLevel precision, TracingInfo parent) {
    this(precision);
    this.parent = parent;
  }

  public PrecisionLevel getPrecision() {
    return precision;
  }

  @Override
  public void setNameAndStartTime(String name) {
    this.spanStarted = true;
    this.spanName = name;
  }

  @Override
  public void setConsistencyLevel(ConsistencyLevel consistency) {
    this.consistencyLevel = consistency;
  }

  @Override
  public void setStatementType(String statementType) {
    this.statementType = statementType;
  }

  @Override
  public void setRetryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  @Override
  public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
    this.loadBalancingPolicy = loadBalancingPolicy;
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  @Override
  public void setShardID(int shardID) {
    this.shardID = shardID;
  }

  @Override
  public void setPeerName(String peerName) {
    this.peerName = peerName;
  }

  @Override
  public void setPeerIP(InetAddress peerIP) {
    this.peerIP = peerIP;
  }

  @Override
  public void setPeerPort(int peerPort) {
    this.peerPort = peerPort;
  }

  @Override
  public void setQueryPaged(Boolean queryPaged) {
    this.queryPaged = queryPaged;
  }

  @Override
  public void setRowsCount(int rowsCount) {
    this.rowsCount = rowsCount;
  }

  @Override
  public void setStatement(String statement, int limit) {
    if (currentPrecisionLevelIsAtLeast(PrecisionLevel.FULL)) {
      if (statement.length() > limit) statement = statement.substring(0, limit);
      this.statement = statement;
    }
  }

  @Override
  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  @Override
  public void setPartitionKey(String partitionKey) {
    this.partitionKey = partitionKey;
  }

  @Override
  public void setTable(String table) {
    this.table = table;
  }

  @Override
  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  @Override
  public void setReplicas(String replicas) {
    this.replicas = replicas;
  }

  @Override
  public void recordException(Exception exception) {
    if (this.exceptions == null) {
      this.exceptions = new ArrayList();
    }
    this.exceptions.add(exception);
  }

  @Override
  public void setStatus(StatusCode code) {
    this.statusCode = code;
  }

  @Override
  public void setStatus(StatusCode code, String description) {
    this.statusCode = code;
    this.description = description;
  }

  @Override
  public void tracingFinished() {
    this.spanFinished = true;
  }

  private boolean currentPrecisionLevelIsAtLeast(PrecisionLevel requiredLevel) {
    return requiredLevel.compareTo(precision) <= 0;
  }

  public boolean isSpanStarted() {
    return spanStarted;
  }

  public boolean isSpanFinished() {
    return spanFinished;
  }

  public String getSpanName() {
    return spanName;
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public String getStatementType() {
    return statementType;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public LoadBalancingPolicy getLoadBalancingPolicy() {
    return loadBalancingPolicy;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public Integer getRetryCount() {
    return retryCount;
  }

  public Integer getShardID() {
    return shardID;
  }

  public String getPeerName() {
    return peerName;
  }

  public InetAddress getPeerIP() {
    return peerIP;
  }

  public Integer getPeerPort() {
    return peerPort;
  }

  public Boolean getQueryPaged() {
    return queryPaged;
  }

  public Integer getRowsCount() {
    return rowsCount;
  }

  public String getStatement() {
    return statement;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public String getPartitionKey() {
    return partitionKey;
  }

  public String getTable() {
    return table;
  }

  public String getOperationType() {
    return operationType;
  }

  public String getReplicas() {
    return replicas;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public String getDescription() {
    return description;
  }

  public TracingInfo getParent() {
    return parent;
  }
}
