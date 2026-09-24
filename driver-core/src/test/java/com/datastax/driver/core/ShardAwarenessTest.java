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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.datastax.driver.core.QueryTrace.Event;
import com.datastax.driver.core.utils.ScyllaOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@ScyllaOnly
@CCMConfig(
    numberOfNodes = 3,
    jvmArgs = {
      /* 2-shard Scylla: */
      "--smp",
      "2"
    })
public class ShardAwarenessTest extends CCMTestsSupport {
  private static final Logger logger = LoggerFactory.getLogger(ShardAwarenessTest.class);

  /**
   * How many times a traced read is re-issued when its query trace comes back without the local
   * read event. Guards against incomplete traces, not against a wrong shard.
   */
  private static final int MAX_TRACE_ATTEMPTS = 3;

  private final boolean useAdvancedShardAwareness;

  @Factory(dataProvider = "dataProvider")
  public ShardAwarenessTest(boolean useAdvancedShardAwareness) {
    this.useAdvancedShardAwareness = useAdvancedShardAwareness;
  }

  @DataProvider
  public static Object[][] dataProvider() {
    // Run the tests with advanced shard awareness
    // and without it.
    return new Object[][] {{false}, {true}};
  }

  @Override
  public Cluster.Builder createClusterBuilder() {
    Cluster.Builder builder = super.createClusterBuilder();
    if (!useAdvancedShardAwareness) {
      builder = builder.withoutAdvancedShardAwareness();
    }
    return builder;
  }

  private void verifyCorrectShardSingleRow(String pk, String ck, String v, String shard) {
    PreparedStatement prepared =
        session().prepare("SELECT pk, ck, v FROM shardawaretest.t WHERE pk=? AND ck=?");

    for (int attempt = 1; attempt <= MAX_TRACE_ATTEMPTS; attempt++) {
      // Bind a fresh statement on every attempt. Reusing one BoundStatement across attempts
      // would pin the coordinator through the paging optimization, which would silently void
      // the shard assertion below.
      ResultSet result = session().execute(prepared.bind(pk, ck).enableTracing());

      Row row = result.one();
      assertTrue(result.isExhausted());
      assertThat(row).isNotNull();
      assertThat(row.getString("pk")).isEqualTo(pk);
      assertThat(row.getString("ck")).isEqualTo(ck);
      assertThat(row.getString("v")).isEqualTo(v);

      ExecutionInfo executionInfo = result.getExecutionInfo();

      QueryTrace trace = executionInfo.getQueryTrace();
      boolean anyLocal = false;
      for (Event event : trace.getEvents()) {
        logger.info(
            "  {} - {} - [{}] - {}",
            event.getSourceElapsedMicros(),
            event.getSource(),
            event.getThreadName(),
            event.getDescription());
        // Only data-local events carry the owning shard. Scylla 2025.1 adds coordinator-side
        // events that legitimately run on shard 0 regardless of the data shard. Also, since
        // Scylla 2025.1 the thread name carries a service-level suffix ("shard N/sl:<level>"),
        // so strip it before comparing.
        if (event.getDescription().contains("querying locally")) {
          anyLocal = true;
          String normalized = event.getThreadName().replaceFirst("/sl:[^/]*$", "");
          assertThat(normalized).startsWith(shard);
        }
      }
      if (anyLocal) {
        return;
      }
      // QueryTrace waits only until the coordinator's session row reports a duration; the
      // events fetched alongside it can still be incomplete, and QueryTrace.doFetchTrace
      // documents that the trace "may not contain the log of replicas". An event list without
      // the local read is therefore not a functional failure, so retry with a fresh trace.
      // Re-executing the query is the only way to get one: once a trace has been fetched its
      // events are cached and frozen, so re-reading the same trace can never return more.
      logger.warn(
          "Attempt {}/{} for pk={}: trace {} carried no 'querying locally' event ({} events);"
              + " retrying with a fresh trace",
          attempt,
          MAX_TRACE_ATTEMPTS,
          pk,
          trace.getTraceId(),
          trace.getEvents().size());
    }
    fail(
        "No 'querying locally' trace event was observed for the query after "
            + MAX_TRACE_ATTEMPTS
            + " attempts");
  }

  @Test(groups = "short")
  public void correctShardInTracingTest() {
    session().execute("DROP KEYSPACE IF EXISTS shardawaretest");
    session()
        .execute(
            "CREATE KEYSPACE shardawaretest WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': '3'} AND TABLETS = {'enabled': false}");
    session()
        .execute("CREATE TABLE shardawaretest.t (pk text, ck text, v text, PRIMARY KEY (pk, ck))");

    PreparedStatement populateStatement =
        session().prepare("INSERT INTO shardawaretest.t (pk, ck, v) VALUES (?, ?, ?)");
    session().execute(populateStatement.bind("a", "b", "c"));
    session().execute(populateStatement.bind("e", "f", "g"));
    session().execute(populateStatement.bind("100002", "f", "g"));

    verifyCorrectShardSingleRow("a", "b", "c", "shard 0");
    verifyCorrectShardSingleRow("e", "f", "g", "shard 0");
    verifyCorrectShardSingleRow("100002", "f", "g", "shard 1");
  }
}
