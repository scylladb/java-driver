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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.PagingOptimizingLoadBalancingPolicy;
import java.util.ArrayList;
import java.util.Collection;
import org.testng.annotations.Test;

public class BasicTracingTest extends CCMTestsSupport {
  private static TestTracingInfoFactory testTracingInfoFactory;
  private Session session;

  @Override
  public void onTestContextInitialized() {
    initializeTestTracing();
    session.execute("USE " + keyspace);
    session.execute("CREATE TABLE t (k int PRIMARY KEY, v int)");

    Collection<TracingInfo> spans = testTracingInfoFactory.getSpans();
    spans.clear();
  }

  @Test(groups = "short")
  public void simpleTracingTest() {
    session.execute("INSERT INTO t(k, v) VALUES (1, 7)");

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
  public void tagsTest() {
    PreparedStatement prepared = session.prepare("INSERT INTO t(k, v) VALUES (?, ?)");

    Collection<TracingInfo> prepareSpans = testTracingInfoFactory.getSpans();
    assertNotEquals(prepareSpans.size(), 0);
    assertTrue(getRoot(prepareSpans) instanceof TestTracingInfo);
    prepareSpans.clear();

    BoundStatement bound = prepared.bind(1, 7);
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
    assertEquals(root.getBatchSize(), new Integer(1));
    assertEquals(root.getConsistencyLevel(), ConsistencyLevel.ONE);
    assertNull(root.getRowsCount()); // no rows are returned in insert
    assertTrue(root.getLoadBalancingPolicy() instanceof PagingOptimizingLoadBalancingPolicy);
    assertTrue(root.getRetryPolicy() instanceof DefaultRetryPolicy);
    assertFalse(root.getQueryPaged());
    assertNull(root.getStatement()); // because of precision level NORMAL
    // these are tags specific to bound statement
    assertEquals(root.getKeyspace(), keyspace);
    assertEquals(root.getPartitionKey(), "k=1");
    assertEquals(root.getTable(), "t");

    // these tags should not be set for request span
    assertNull(root.getPeerName());
    assertNull(root.getPeerIP());
    assertNull(root.getPeerPort());
    assertNull(root.getRetryCount());

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
      assertNull(tracingInfo.getQueryPaged());
      assertNull(tracingInfo.getStatement());
      assertNull(tracingInfo.getPeerName());
      assertNull(tracingInfo.getPeerIP());
      assertNull(tracingInfo.getPeerPort());
      // these are tags specific to bound statement
      assertNull(tracingInfo.getKeyspace());
      assertNull(tracingInfo.getPartitionKey());
      assertNull(tracingInfo.getTable());

      // this tag should be set for speculative execution span
      assertTrue(tracingInfo.getRetryCount() >= 0);
    }

    ArrayList<TracingInfo> queries = new ArrayList<TracingInfo>();
    for (TracingInfo tracingInfo : speculativeExecutions) {
      queries.addAll(getChildren(spans, tracingInfo));
    }
    assertTrue(queries.size() > 0);

    for (TracingInfo querySpan : queries) {
      assertTrue(querySpan instanceof TestTracingInfo);
      TestTracingInfo tracingInfo = (TestTracingInfo) querySpan;

      // these tags should not be set for query span
      assertNull(tracingInfo.getStatementType());
      assertNull(tracingInfo.getBatchSize());
      assertNull(tracingInfo.getConsistencyLevel());
      assertNull(tracingInfo.getRowsCount());
      assertNull(tracingInfo.getLoadBalancingPolicy());
      assertNull(tracingInfo.getRetryPolicy());
      assertNull(tracingInfo.getQueryPaged());
      assertNull(tracingInfo.getStatement());
      assertNull(tracingInfo.getRetryCount());
      // these are tags specific to bound statement
      assertNull(tracingInfo.getKeyspace());
      assertNull(tracingInfo.getPartitionKey());
      assertNull(tracingInfo.getTable());

      // these tags should be set for query span
      assertNotNull(tracingInfo.getPeerName());
      assertNotNull(tracingInfo.getPeerIP());
      assertNotNull(tracingInfo.getPeerPort());
      assertTrue(tracingInfo.getPeerPort() >= 0 && tracingInfo.getPeerPort() <= 65535);
    }

    spans.clear();
  }

  private void initializeTestTracing() {
    testTracingInfoFactory = new TestTracingInfoFactory(PrecisionLevel.NORMAL);
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
