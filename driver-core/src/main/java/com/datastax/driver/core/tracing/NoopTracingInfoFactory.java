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

public class NoopTracingInfoFactory implements TracingInfoFactory {

  private static class NoopTracingInfo implements TracingInfo {
    @Override
    public void setNameAndStartTime(String name) {}

    @Override
    public void setConsistencyLevel(ConsistencyLevel consistency) {}

    @Override
    public void setStatementType(String statementType) {}

    @Override
    public void setRetryPolicy(RetryPolicy retryPolicy) {}

    @Override
    public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {};

    @Override
    public void setBatchSize(int batchSize) {}

    @Override
    public void setRetryCount(int retryCount) {}

    @Override
    public void setShardID(int shardID) {}

    @Override
    public void setPeerName(String peerName) {}

    @Override
    public void setPeerIP(InetAddress peerIP) {}

    @Override
    public void setPeerPort(int peerPort) {}

    @Override
    public void setQueryPaged(Boolean queryPaged) {}

    @Override
    public void setRowsCount(int rowsCount) {}

    @Override
    public void setStatement(String statement, int limit) {}

    @Override
    public void setKeyspace(String keyspace) {}

    @Override
    public void setPartitionKey(String partitionKey) {}

    @Override
    public void setTable(String table) {}

    @Override
    public void setOperationType(String operationType) {}

    @Override
    public void setReplicas(String replicas) {}

    @Override
    public void recordException(Exception exception) {}

    @Override
    public void setStatus(StatusCode code, String description) {}

    @Override
    public void setStatus(StatusCode code) {}

    @Override
    public void tracingFinished() {}
  }

  public static final NoopTracingInfo INSTANCE = new NoopTracingInfo();

  @Override
  public TracingInfo buildTracingInfo() {
    return INSTANCE;
  }

  @Override
  public TracingInfo buildTracingInfo(TracingInfo parent) {
    return new NoopTracingInfo();
  }
}
