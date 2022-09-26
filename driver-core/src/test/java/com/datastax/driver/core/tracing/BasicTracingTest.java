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

import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.PagingOptimizingLoadBalancingPolicy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;

public class BasicTracingTest extends CCMTestsSupport {
  private static TestTracingInfoFactory testTracingInfoFactory;
  private Session session;

  @Override
  public void onTestContextInitialized() {
    initializeTestTracing();
    session.execute("USE " + keyspace);
    session.execute("DROP TABLE IF EXISTS t");
    session.execute("CREATE TABLE t (k int PRIMARY KEY, v int)");
    session.execute("CREATE TABLE blobs (k int PRIMARY KEY, v blob)");
    session.execute("INSERT INTO t(k, v) VALUES (2, 3)");
    session.execute("INSERT INTO t(k, v) VALUES (1, 7)");
    session.execute("INSERT INTO t(k, v) VALUES (5, 7)");
    session.execute("INSERT INTO t(k, v) VALUES (6, 7)");
    session.execute("INSERT INTO t(k, v) VALUES (7, 7)");
    session.execute("INSERT INTO t(k, v) VALUES (8, 7)");
    session.execute("INSERT INTO t(k, v) VALUES (9, 7)");
    session.execute("INSERT INTO t(k, v) VALUES (10, 7)");

    Collection<TracingInfo> spans = testTracingInfoFactory.getSpans();
    spans.clear();
  }

  @Test(groups = "short")
  public void simpleTracingTest() {
    session.execute("INSERT INTO t(k, v) VALUES (4, 5)");

    Collection<TracingInfo> spans = testTracingInfoFactory.getSpans();
    assertNotEquals(spans.size(), 0);

    TracingInfo rootSpan = getRoot(spans);
    assertTrue(rootSpan instanceof TestTracingInfo);
    TestTracingInfo root = (TestTracingInfo) rootSpan;

    assertTrue(root.isSpanStarted());
    assertTrue(root.isSpanFinished());
    assertEquals(root.getStatusCode(), TracingInfo.StatusCode.OK);

    spans.clear();
  }

  @Test(groups = "short")
  public void tagsInsertTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO blobs(k, v) VALUES (?, ?)");

    Collection<TracingInfo> prepareSpans = testTracingInfoFactory.getSpans();
    assertNotEquals(prepareSpans.size(), 0);
    assertTrue(getRoot(prepareSpans) instanceof TestTracingInfo);
    prepareSpans.clear();

    BoundStatement bound = prepared.bind(1, ByteBuffer.wrap("\n\0\n".getBytes()));
    session.execute(bound);

    Collection<TracingInfo> spans = testTracingInfoFactory.getSpans();
    assertNotEquals(spans.size(), 0);

    TracingInfo rootSpan = getRoot(spans);
    assertTrue(rootSpan instanceof TestTracingInfo);
    TestTracingInfo root = (TestTracingInfo) rootSpan;

    assertTrue(root.isSpanStarted());
    assertTrue(root.isSpanFinished());
    assertEquals(root.getStatusCode(), TracingInfo.StatusCode.OK);

    // these tags should be set for request span
    assertEquals(root.getStatementType(), "prepared");
    assertNull(root.getBatchSize());
    assertEquals(root.getConsistencyLevel(), ConsistencyLevel.ONE);
    assertNull(root.getRowsCount()); // no rows are returned in INSERT
    assertTrue(root.getLoadBalancingPolicy() instanceof PagingOptimizingLoadBalancingPolicy);
    assertTrue(root.getSpeculativeExecutionPolicy() instanceof NoSpeculativeExecutionPolicy);
    assertTrue(root.getRetryPolicy() instanceof DefaultRetryPolicy);
    assertNull(root.getFetchSize()); // fetch size was not explicitly set for this statement
    assertNull(root.getHasMorePages()); // no paging are returned in INSERT
    assertNull(root.getStatement()); // because of precision level NORMAL
    // these are tags specific to bound statement
    assertEquals(root.getKeyspace(), keyspace);
    assertEquals(root.getBoundValues(), "k=1, v=0x0A000A"); // "\n\0\n"
    assertEquals(root.getPartitionKey(), "k=1");
    assertEquals(root.getTable(), "blobs");

    // these tags should not be set for request span
    assertNull(root.getPeerName());
    assertNull(root.getPeerIP());
    assertNull(root.getPeerPort());
    assertNull(root.getAttemptCount());

    ArrayList<TracingInfo> speculativeExecutions = getChildren(spans, root);
    assertTrue(speculativeExecutions.size() > 0);

    for (TracingInfo speculativeExecutionSpan : speculativeExecutions) {
      assertTrue(speculativeExecutionSpan instanceof TestTracingInfo);
      TestTracingInfo tracingInfo = (TestTracingInfo) speculativeExecutionSpan;

      // these tags should not be set for speculative execution span
      assertNull(tracingInfo.getStatementType());
      assertNull(tracingInfo.getBatchSize());
      assertNull(tracingInfo.getConsistencyLevel());
      assertNull(tracingInfo.getRowsCount());
      assertNull(tracingInfo.getLoadBalancingPolicy());
      assertNull(tracingInfo.getRetryPolicy());
      assertNull(tracingInfo.getFetchSize());
      assertNull(tracingInfo.getHasMorePages());
      assertNull(tracingInfo.getStatement());
      assertNull(tracingInfo.getPeerName());
      assertNull(tracingInfo.getPeerIP());
      assertNull(tracingInfo.getPeerPort());
      // these are tags specific to bound statement
      assertNull(tracingInfo.getKeyspace());
      assertNull(tracingInfo.getPartitionKey());
      assertNull(tracingInfo.getTable());

      // this tag should be set for speculative execution span
      assertTrue(tracingInfo.getAttemptCount() >= 1);
    }

    ArrayList<TracingInfo> attempts = new ArrayList<TracingInfo>();
    for (TracingInfo tracingInfo : speculativeExecutions) {
      attempts.addAll(getChildren(spans, tracingInfo));
    }
    assertTrue(attempts.size() > 0);

    for (TracingInfo attemptSpan : attempts) {
      assertTrue(attemptSpan instanceof TestTracingInfo);
      TestTracingInfo tracingInfo = (TestTracingInfo) attemptSpan;

      // these tags should not be set for attempt span
      assertNull(tracingInfo.getStatementType());
      assertNull(tracingInfo.getBatchSize());
      assertNull(tracingInfo.getConsistencyLevel());
      assertNull(tracingInfo.getRowsCount());
      assertNull(tracingInfo.getLoadBalancingPolicy());
      assertNull(tracingInfo.getRetryPolicy());
      assertNull(tracingInfo.getFetchSize());
      assertNull(tracingInfo.getHasMorePages());
      assertNull(tracingInfo.getStatement());
      assertNull(tracingInfo.getAttemptCount());
      // these are tags specific to bound statement
      assertNull(tracingInfo.getKeyspace());
      assertNull(tracingInfo.getPartitionKey());
      assertNull(tracingInfo.getTable());

      // these tags should be set for attempt span
      assertNotNull(tracingInfo.getPeerName());
      assertNotNull(tracingInfo.getPeerIP());
      assertNotNull(tracingInfo.getPeerPort());
      assertTrue(tracingInfo.getPeerPort() >= 0 && tracingInfo.getPeerPort() <= 65535);
    }

    spans.clear();
  }

  @Test(groups = "short")
  public void tagsSelectTest() {
    SimpleStatement s = new SimpleStatement("SELECT k FROM t WHERE v = 7 ALLOW FILTERING");
    s.setFetchSize(2);
    s.setIdempotent(true);
    s.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
    s.setConsistencyLevel(ConsistencyLevel.QUORUM);

    final Collection<TracingInfo> spans = testTracingInfoFactory.getSpans();
    class SpanChecks {
      int totalRows = 0;

      void checkTotalCount() {
        assertEquals(totalRows, 7);
      }

      void checkAssertions(boolean hasMorePages) {
        assertEquals(spans.size(), 3);

        TracingInfo rootSpan = getRoot(spans);
        assertTrue(rootSpan instanceof TestTracingInfo);
        TestTracingInfo root = (TestTracingInfo) rootSpan;

        assertTrue(root.isSpanStarted());
        assertTrue(root.isSpanFinished());
        assertEquals(root.getStatusCode(), TracingInfo.StatusCode.OK);

        // these tags should be set for request span
        assertEquals(root.getStatementType(), "regular");
        assertNull(root.getBatchSize());
        assertEquals(root.getConsistencyLevel(), ConsistencyLevel.QUORUM);
        assertNotNull(root.getRowsCount());
        totalRows += root.getRowsCount();
        assertTrue(root.getLoadBalancingPolicy() instanceof PagingOptimizingLoadBalancingPolicy);
        assertTrue(root.getSpeculativeExecutionPolicy() instanceof NoSpeculativeExecutionPolicy);
        assertTrue(root.getRetryPolicy() == FallthroughRetryPolicy.INSTANCE);
        assertEquals(root.getFetchSize(), new Integer(2));
        assertEquals(root.getHasMorePages(), new Boolean(hasMorePages));
        assertNull(root.getStatement()); // because of precision level NORMAL

        // these are tags specific to bound statement
        assertNull(root.getKeyspace());
        assertNull(root.getPartitionKey());
        assertNull(root.getTable());

        // these tags should not be set for request span
        assertNull(root.getPeerName());
        assertNull(root.getPeerIP());
        assertNull(root.getPeerPort());
        assertNull(root.getAttemptCount());

        ArrayList<TracingInfo> speculativeExecutions = getChildren(spans, root);
        assertTrue(speculativeExecutions.size() > 0);

        for (TracingInfo speculativeExecutionSpan : speculativeExecutions) {
          assertTrue(speculativeExecutionSpan instanceof TestTracingInfo);
          TestTracingInfo tracingInfo = (TestTracingInfo) speculativeExecutionSpan;

          // these tags should not be set for speculative execution span
          assertNull(tracingInfo.getStatementType());
          assertNull(tracingInfo.getBatchSize());
          assertNull(tracingInfo.getConsistencyLevel());
          assertNull(tracingInfo.getRowsCount());
          assertNull(tracingInfo.getLoadBalancingPolicy());
          assertNull(tracingInfo.getRetryPolicy());
          assertNull(tracingInfo.getFetchSize());
          assertNull(tracingInfo.getHasMorePages());
          assertNull(tracingInfo.getStatement());
          assertNull(tracingInfo.getPeerName());
          assertNull(tracingInfo.getPeerIP());
          assertNull(tracingInfo.getPeerPort());
          // these are tags specific to bound statement
          assertNull(tracingInfo.getKeyspace());
          assertNull(tracingInfo.getPartitionKey());
          assertNull(tracingInfo.getTable());

          // this tag should be set for speculative execution span
          assertTrue(tracingInfo.getAttemptCount() >= 1);
        }

        ArrayList<TracingInfo> attempts = new ArrayList<TracingInfo>();
        for (TracingInfo tracingInfo : speculativeExecutions) {
          attempts.addAll(getChildren(spans, tracingInfo));
        }
        assertTrue(attempts.size() > 0);

        for (TracingInfo attemptSpan : attempts) {
          assertTrue(attemptSpan instanceof TestTracingInfo);
          TestTracingInfo tracingInfo = (TestTracingInfo) attemptSpan;

          // these tags should not be set for attempt span
          assertNull(tracingInfo.getStatementType());
          assertNull(tracingInfo.getBatchSize());
          assertNull(tracingInfo.getConsistencyLevel());
          assertNull(tracingInfo.getRowsCount());
          assertNull(tracingInfo.getLoadBalancingPolicy());
          assertNull(tracingInfo.getRetryPolicy());
          assertNull(tracingInfo.getFetchSize());
          assertNull(tracingInfo.getHasMorePages());
          assertNull(tracingInfo.getStatement());
          assertNull(tracingInfo.getAttemptCount());
          // these are tags specific to bound statement
          assertNull(tracingInfo.getKeyspace());
          assertNull(tracingInfo.getPartitionKey());
          assertNull(tracingInfo.getTable());

          // these tags should be set for attempt span
          assertNotNull(tracingInfo.getPeerName());
          assertNotNull(tracingInfo.getPeerIP());
          assertNotNull(tracingInfo.getPeerPort());
          assertTrue(tracingInfo.getPeerPort() >= 0 && tracingInfo.getPeerPort() <= 65535);
        }

        spans.clear();
      }
    }

    SpanChecks spanChecks = new SpanChecks();

    try {
      ResultSet rs = session.execute(s);

      while (!rs.isFullyFetched()) {
        spanChecks.checkAssertions(true);
        rs.fetchMoreResults().get();
      }
      spanChecks.checkAssertions(false);

    } catch (InterruptedException e) {
      assert false : "InterruptedException";
    } catch (ExecutionException e) {
      assert false : "ExecutionException";
    }
    spanChecks.checkTotalCount();
  }

  private void initializeTestTracing() {
    testTracingInfoFactory = new TestTracingInfoFactory(VerbosityLevel.NORMAL);
    cluster().setTracingInfoFactory(testTracingInfoFactory);
    session = cluster().connect();
  }

  private TracingInfo getRoot(Collection<TracingInfo> spans) {
    TracingInfo root = null;
    for (TracingInfo tracingInfo : spans) {
      if (tracingInfo instanceof TestTracingInfo
          && ((TestTracingInfo) tracingInfo).getParent() == null) {
        assertNull(root); // There should be only one root.
        root = tracingInfo;
      }
    }

    return root;
  }

  private ArrayList<TracingInfo> getChildren(Collection<TracingInfo> spans, TracingInfo parent) {
    ArrayList<TracingInfo> children = new ArrayList<TracingInfo>();
    for (TracingInfo tracingInfo : spans) {
      if (tracingInfo instanceof TestTracingInfo
          && ((TestTracingInfo) tracingInfo).getParent() == parent) {
        children.add(tracingInfo);
      }
    }
    return children;
  }
}
