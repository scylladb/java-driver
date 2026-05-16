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
package com.datastax.driver.core.policies;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CCMConfig;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Integration tests verifying that statements with SERIAL/LOCAL_SERIAL consistency level are routed
 * through the LWT load-balancing path (PRESERVE_REPLICA_ORDER).
 */
@CCMConfig(numberOfNodes = 3)
public class LWTLoadBalancingIT extends CCMTestsSupport {

  @Override
  public Cluster.Builder createClusterBuilder() {
    return Cluster.builder()
        .withLoadBalancingPolicy(
            new TokenAwarePolicy(new RoundRobinPolicy(), TokenAwarePolicy.ReplicaOrdering.RANDOM));
  }

  @Override
  public void onTestContextInitialized() {
    execute("CREATE TABLE IF NOT EXISTS test_lwt (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
    for (int i = 0; i < 10; i++) {
      execute(String.format("INSERT INTO test_lwt (pk, ck, v) VALUES (%d, %d, %d)", i, 0, i));
    }
  }

  @Test(groups = "short")
  public void should_route_local_serial_select_through_lwt_path() {
    Session session = session();

    SimpleStatement simpleSelect =
        new SimpleStatement("SELECT * FROM test_lwt WHERE pk = ? AND ck = ?", 1, 0);
    simpleSelect.setConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);

    PreparedStatement preparedSelect = session.prepare(simpleSelect);
    BoundStatement boundSelect = preparedSelect.bind(1, 0);

    // Verify statement properties
    assertThat(simpleSelect.isLWT()).isFalse();
    assertThat(simpleSelect.getConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);

    // Execute multiple times and collect coordinators — with PRESERVE_REPLICA_ORDER routing,
    // the same partition key should always be routed to the same first replica.
    Set<InetSocketAddress> coordinators = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      ResultSet rs = session.execute(boundSelect);
      Host coordinator = rs.getExecutionInfo().getQueriedHost();
      assertThat(coordinator).isNotNull();
      coordinators.add(coordinator.getEndPoint().resolve());
    }

    // With PRESERVE_REPLICA_ORDER, the first replica is deterministic for a given partition key,
    // so all 30 executions should hit the same coordinator.
    assertThat(coordinators).hasSize(1);
  }

  @Test(groups = "short")
  public void should_route_serial_select_through_lwt_path() {
    Session session = session();

    SimpleStatement simpleSelect =
        new SimpleStatement("SELECT * FROM test_lwt WHERE pk = ? AND ck = ?", 2, 0);
    simpleSelect.setConsistencyLevel(ConsistencyLevel.SERIAL);

    PreparedStatement preparedSelect = session.prepare(simpleSelect);
    BoundStatement boundSelect = preparedSelect.bind(2, 0);

    // Execute multiple times and collect coordinators
    Set<InetSocketAddress> coordinators = new HashSet<>();
    for (int i = 0; i < 30; i++) {
      ResultSet rs = session.execute(boundSelect);
      Host coordinator = rs.getExecutionInfo().getQueriedHost();
      assertThat(coordinator).isNotNull();
      coordinators.add(coordinator.getEndPoint().resolve());
    }

    // With PRESERVE_REPLICA_ORDER, the first replica is deterministic for a given partition key.
    assertThat(coordinators).hasSize(1);
  }
}
