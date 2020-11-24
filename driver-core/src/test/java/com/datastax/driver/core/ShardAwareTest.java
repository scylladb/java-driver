/*
 * Copyright ScyllaDB, Inc.
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

import com.datastax.driver.core.CreateCCM.TestMode;
import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@CreateCCM(TestMode.PER_METHOD)
@CCMConfig(numberOfNodes = 3, dirtiesContext = true, createCluster = false)
public class ShardAwareTest extends CCMTestsSupport {

  static final Logger logger = LoggerFactory.getLogger(ShardAwareTest.class);

  static void createKeyspaceAndColumnFamily(Session session) {
    session.execute("DROP KEYSPACE IF EXISTS preparedtests");
    session.execute(
        "CREATE KEYSPACE preparedtests WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}");
    session.execute("USE preparedtests");
    session.execute("CREATE TABLE cf0 (a text, b text, c text, PRIMARY KEY (a, b))");
  }

  void createData(Session session) {
    session.execute("USE preparedtests");
    PreparedStatement prepared = session.prepare("INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)");

    BoundStatement bs1 = prepared.bind();
    bs1.setString("a", "a");
    bs1.setString("b", "b");
    bs1.setString("c", "c");
    session.execute(bs1);

    BoundStatement bs2 = prepared.bind();
    bs2.setString("a", "e");
    bs2.setString("b", "f");
    bs2.setString("c", "g");
    session.execute(bs2);

    BoundStatement bs3 = prepared.bind();
    bs3.setString("a", "100000");
    bs3.setString("b", "f");
    bs3.setString("c", "g");
    session.execute(bs3);
  }

  class isLocalQuery<E> implements Predicate<Event> {

    @Override
    public boolean test(Event x) {
      return x.getDescription().contains("querying locally");
    }
  }

  void verifySameShardInTracing(ResultSet results, String shardName) {
    ExecutionInfo executionInfo = results.getExecutionInfo();

    QueryTrace trace = executionInfo.getQueryTrace();

    logger.info(
        "'{}' to {} took {}μs",
        trace.getRequestType(),
        trace.getCoordinator(),
        trace.getDurationMicros());
    for (Event event : trace.getEvents()) {
      logger.info(
          "  {} - {} - [{}] - {}",
          event.getSourceElapsedMicros(),
          event.getSource(),
          event.getThreadName(),
          event.getDescription());
      assertThat(event.getThreadName()).isEqualTo(shardName);
    }
    assertThat(trace.getEvents().stream().anyMatch(new isLocalQuery<Event>()));
  }

  void queryData(Session session, boolean verifyInTracing) {
    PreparedStatement prepared = session.prepare("SELECT * FROM cf0 WHERE a=? AND b=?");

    BoundStatement bs1 = prepared.bind();
    bs1.setString("a", "a");
    bs1.setString("b", "b");
    bs1.enableTracing();
    ResultSet results1 = session.execute(bs1);
    Row row1 = results1.one();
    assertThat(row1).isNotNull();
    assertThat(row1.getString("a")).isEqualTo("a");
    assertThat(row1.getString("b")).isEqualTo("b");
    assertThat(row1.getString("c")).isEqualTo("c");

    if (verifyInTracing) {
      this.verifySameShardInTracing(results1, "shard 1");
    }

    BoundStatement bs2 = prepared.bind();
    bs2.setString("a", "100000");
    bs2.setString("b", "f");
    bs2.enableTracing();

    ResultSet results2 = session.execute(bs2);
    Row row2 = results2.one();
    assertThat(row2).isNotNull();
    assertThat(row2.getString("a")).isEqualTo("100000");
    assertThat(row2.getString("b")).isEqualTo("f");
    assertThat(row2.getString("c")).isEqualTo("g");

    if (verifyInTracing) {
      this.verifySameShardInTracing(results2, "shard 0");
    }
    BoundStatement bs3 = prepared.bind();
    bs3.setString("a", "e");
    bs3.setString("b", "f");
    bs3.enableTracing();

    ResultSet results3 = session.execute(bs3);

    if (verifyInTracing) {
      this.verifySameShardInTracing(results3, "shard 1");
    }
  }

  @Test(groups = {"short", "scylla"})
  @CCMConfig(config = {"ring_delay_ms:10000"})
  public void verify_same_shard_in_tracing() throws InterruptedException {

    LoadBalancingPolicy loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy());

    ReconnectionPolicy reconnectionPolicy = new ConstantReconnectionPolicy(1 * 1000);

    // We pass only the first host as contact point, so we know the control connection will be on
    // this host
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withReconnectionPolicy(reconnectionPolicy)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build());
    cluster.init();
    Session session = cluster.connect();

    createKeyspaceAndColumnFamily(session);
    createData(session);
    this.queryData(session, true);
  }

  @Test(groups = {"short", "scylla"})
  @CCMConfig(
      config = {
        "ring_delay_ms:10000",
        "native_transport_port:0",
        "native_shard_aware_transport_port:19042"
      })
  public void verify_same_shard_in_tracing_using_only_shard_aware_port()
      throws InterruptedException {

    LoadBalancingPolicy loadBalancingPolicy = new TokenAwarePolicy(new RoundRobinPolicy());

    ReconnectionPolicy reconnectionPolicy = new ConstantReconnectionPolicy(1 * 1000);

    // We pass only the first host as contact point, so we know the control connection will be on
    // this host
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(19042)
                .withReconnectionPolicy(reconnectionPolicy)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build());
    cluster.init();
    Session session = cluster.connect();

    createKeyspaceAndColumnFamily(session);
    createData(session);
    this.queryData(session, true);
  }
}
