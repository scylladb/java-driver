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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.core.tracing.TracingInfoFactory;
import com.datastax.driver.core.tracing.VerbosityLevel;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/** Tests for OpenTelemetry integration. */
public class OpenTelemetryTest extends CCMTestsSupport {
  /** Collects and saves spans. */
  private static final class BookkeepingSpanProcessor implements SpanProcessor {
    final Lock lock = new ReentrantLock();
    final Condition allEnded = lock.newCondition();

    final Collection<ReadableSpan> startedSpans = new ArrayList<>();
    final Collection<ReadableSpan> spans = new ArrayList<>();

    int activeSpans = 0;

    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {
      lock.lock();

      startedSpans.add(span);
      ++activeSpans;

      lock.unlock();
    }

    @Override
    public boolean isStartRequired() {
      return true;
    }

    @Override
    public void onEnd(ReadableSpan span) {
      lock.lock();

      spans.add(span);
      --activeSpans;

      if (activeSpans == 0) allEnded.signal();

      lock.unlock();
    }

    @Override
    public boolean isEndRequired() {
      return true;
    }

    public Collection<ReadableSpan> getSpans() {
      lock.lock();

      try {
        while (activeSpans > 0) allEnded.await();

        for (ReadableSpan span : startedSpans) {
          assertTrue(span.hasEnded());
        }
      } catch (InterruptedException e) {
        assert false;
      } finally {
        lock.unlock();
      }

      return spans;
    }
  }

  private Session session;

  /**
   * Prepare OpenTelemetry configuration and run test with it.
   *
   * @param precisionLevel precision level of tracing for the tests.
   * @param test test to run.
   * @return collected spans.
   */
  private Collection<ReadableSpan> collectSpans(
      VerbosityLevel precisionLevel, BiConsumer<Tracer, TracingInfoFactory> test) {
    final Resource serviceNameResource =
        Resource.create(
            Attributes.of(ResourceAttributes.SERVICE_NAME, "Scylla Java driver - test"));

    final BookkeepingSpanProcessor collector = new BookkeepingSpanProcessor();

    final SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(collector)
            .setResource(Resource.getDefault().merge(serviceNameResource))
            .build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();

    final Tracer tracer = openTelemetry.getTracerProvider().get("this");
    final OpenTelemetryTracingInfoFactory tracingInfoFactory =
        new OpenTelemetryTracingInfoFactory(tracer, precisionLevel);
    cluster().setTracingInfoFactory(tracingInfoFactory);

    session = cluster().connect();
    session.execute("USE " + keyspace);
    session.execute("DROP TABLE IF EXISTS t");
    session.execute("CREATE TABLE t (k int PRIMARY KEY, v int)");

    BatchStatement bs = new BatchStatement();
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (12, 3)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (1, 7)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (5, 7)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (6, 7)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (7, 7)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (8, 7)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (9, 7)"));
    bs.add(new SimpleStatement("INSERT INTO t(k, v) VALUES (10, 7)"));
    bs.setConsistencyLevel(ConsistencyLevel.ALL);
    session.execute(bs);

    collector.getSpans().clear();

    test.accept(tracer, tracingInfoFactory);
    session.close();

    tracerProvider.close();
    cluster().setTracingInfoFactory(new NoopTracingInfoFactory());

    return collector.getSpans();
  }

  private Collection<ReadableSpan> getChildrenOfSpans(
      Collection<ReadableSpan> allSpans, Collection<ReadableSpan> parentSpans) {
    return allSpans.stream()
        .filter(
            span ->
                parentSpans.stream()
                    .filter(
                        parentSpan ->
                            parentSpan.getSpanContext().equals(span.getParentSpanContext()))
                    .findAny()
                    .isPresent())
        .collect(Collectors.toList());
  }

  /** Basic test for creating spans with INSERT statement. */
  @Test(groups = "short")
  public void simpleTracingInsertTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            VerbosityLevel.NORMAL,
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              session.execute("INSERT INTO t(k, v) VALUES (4, 2)");
              session.execute("INSERT INTO t(k, v) VALUES (2, 1)");

              scope.close();
              userSpan.end();
            });

    // Retrieve the span created directly by tracer.
    final List<ReadableSpan> userSpans =
        spans.stream()
            .filter(span -> !span.getParentSpanContext().isValid()) // The root span has no parent.
            .collect(Collectors.toList());
    assertEquals(userSpans.size(), 1);
    final ReadableSpan userSpan = userSpans.get(0);

    for (ReadableSpan span : spans) {
      assertTrue(span.getSpanContext().isValid());
      assertTrue( // Each span is either the root span or has a valid parent span.
          span.getSpanContext().equals(userSpan.getSpanContext())
              || span.getParentSpanContext().isValid());
    }

    // Retrieve spans representing requests.
    final Collection<ReadableSpan> requestSpans =
        spans.stream()
            .filter(span -> span.getParentSpanContext().equals(userSpan.getSpanContext()))
            .collect(Collectors.toList());
    assertEquals(requestSpans.size(), 2);

    final Collection<ReadableSpan> speculativeExecutionsSpans =
        getChildrenOfSpans(spans, requestSpans);
    assertEquals(speculativeExecutionsSpans.size(), 2);

    final Collection<ReadableSpan> attemptSpans =
        getChildrenOfSpans(spans, speculativeExecutionsSpans);
    assertEquals(attemptSpans.size(), 2);

    requestSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "request");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              // tags generic for any (reasonable) statement
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.consistency_level")), "ONE");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.statement_type")), "regular");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.load_balancing_policy")),
                  "PagingOptimizingLoadBalancingPolicy");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.speculative_execution_policy")),
                  "NoSpeculativeExecutionPolicy");

              assertNull(
                  tags.get(
                      AttributeKey.stringKey("db.scylladb.fetch_size"))); // was not set explicitly
              // no such information in RegularStatement:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.batch_size")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.keyspace")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.table")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.partition_key")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.db.operation")));
              // no such information with VerbosityLevel.NORMAL:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.statement")));
              // no such information with operation INSERT:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.rows_count")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.has_more_pages")));
              // no such tags in "request" span:
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    speculativeExecutionsSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "speculative_execution");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.attempt_count")), "1");
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    attemptSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "attempt");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNotNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });
  }

  /** Basic test for creating spans with UPDATE statement. */
  @Test(groups = "short")
  public void simpleTracingUpdateTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            VerbosityLevel.NORMAL,
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              final BatchStatement batch = new BatchStatement();
              batch.addAll(
                  new ArrayList<String>() {
                    {
                      add("UPDATE t SET v=0 WHERE k=1");
                      add("UPDATE t SET v=0 WHERE k=2");
                      add("UPDATE t SET v=0 WHERE k=3");
                      add("UPDATE t SET v=0 WHERE k=4");
                    }
                  }.stream().map(SimpleStatement::new).collect(Collectors.toList()));

              session.execute(batch);

              scope.close();
              userSpan.end();
            });

    // Retrieve the span created directly by tracer.
    final List<ReadableSpan> userSpans =
        spans.stream()
            .filter(span -> !span.getParentSpanContext().isValid()) // The root span has no parent.
            .collect(Collectors.toList());
    assertEquals(userSpans.size(), 1);
    final ReadableSpan userSpan = userSpans.get(0);

    for (ReadableSpan span : spans) {
      assertTrue(span.getSpanContext().isValid());
      assertTrue( // Each span is either the root span or has a valid parent span.
          span.getSpanContext().equals(userSpan.getSpanContext())
              || span.getParentSpanContext().isValid());
    }

    // Retrieve spans representing requests.
    final Collection<ReadableSpan> requestSpans =
        spans.stream()
            .filter(span -> span.getParentSpanContext().equals(userSpan.getSpanContext()))
            .collect(Collectors.toList());
    assertEquals(requestSpans.size(), 1);

    final Collection<ReadableSpan> speculativeExecutionsSpans =
        getChildrenOfSpans(spans, requestSpans);
    assertEquals(speculativeExecutionsSpans.size(), 1);

    final Collection<ReadableSpan> attemptSpans =
        getChildrenOfSpans(spans, speculativeExecutionsSpans);
    assertEquals(attemptSpans.size(), 1);

    requestSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "request");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              // tags generic for any (reasonable) statement
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.consistency_level")), "ONE");
              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.statement_type")), "batch");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.load_balancing_policy")),
                  "PagingOptimizingLoadBalancingPolicy");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.speculative_execution_policy")),
                  "NoSpeculativeExecutionPolicy");

              // tags specific to BatchStatement
              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.batch_size")), "4");

              assertNull(
                  tags.get(
                      AttributeKey.stringKey("db.scylladb.fetch_size"))); // was not set explicitly
              // no such information in BatchStatement:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.keyspace")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.table")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.partition_key")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.db.operation")));
              // no such information with VerbosityLevel.NORMAL:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.statement")));
              // no such information with operation UPDATE:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.rows_count")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.has_more_pages")));
              // no such tags in "request" span:
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    speculativeExecutionsSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "speculative_execution");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.attempt_count")), "1");
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    attemptSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "attempt");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNotNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });
  }

  /** Basic test for creating spans with DELETE statement. */
  @Test(groups = "short")
  public void simpleTracingDeleteTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            VerbosityLevel.NORMAL,
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              final PreparedStatement ps = session.prepare("DELETE FROM t WHERE k=?");

              session.execute(ps.bind(7));
              session.execute(ps.bind(8));

              scope.close();
              userSpan.end();
            });

    // Retrieve the span created directly by tracer.
    final List<ReadableSpan> userSpans =
        spans.stream()
            .filter(span -> !span.getParentSpanContext().isValid()) // The root span has no parent.
            .collect(Collectors.toList());
    assertEquals(userSpans.size(), 1);
    final ReadableSpan userSpan = userSpans.get(0);

    for (ReadableSpan span : spans) {
      assertTrue(span.getSpanContext().isValid());
      assertTrue( // Each span is either the root span or has a valid parent span.
          span.getSpanContext().equals(userSpan.getSpanContext())
              || span.getParentSpanContext().isValid());
    }

    // Retrieve spans representing requests.
    final Collection<ReadableSpan> requestSpans =
        spans.stream()
            .filter(
                span ->
                    span.getParentSpanContext().equals(userSpan.getSpanContext())
                        && span.toSpanData()
                                .getAttributes()
                                .get(AttributeKey.stringKey("db.scylladb.statement_type"))
                            != null) // to exclude preparation spans
            .collect(Collectors.toList());
    assertEquals(requestSpans.size(), 2);

    final Collection<ReadableSpan> speculativeExecutionsSpans =
        getChildrenOfSpans(spans, requestSpans);
    assertEquals(speculativeExecutionsSpans.size(), 2);

    final Collection<ReadableSpan> attemptSpans =
        getChildrenOfSpans(spans, speculativeExecutionsSpans);
    assertEquals(attemptSpans.size(), 2);

    requestSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "request");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              // tags generic for any (reasonable) statement
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.consistency_level")), "ONE");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.statement_type")), "prepared");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.load_balancing_policy")),
                  "PagingOptimizingLoadBalancingPolicy");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.speculative_execution_policy")),
                  "NoSpeculativeExecutionPolicy");

              // tags specific to PreparedStatement
              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.keyspace")), keyspace);
              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.table")), "t");
              String partitionKey = tags.get(AttributeKey.stringKey("db.scylladb.partition_key"));
              assertTrue(partitionKey.equals("k=7") || partitionKey.equals("k=8"));
              String boundValues = tags.get(AttributeKey.stringKey("db.scylladb.bound_values"));
              assertTrue(boundValues.equals("k=7") || boundValues.equals("k=8"));
              assertNull(
                  tags.get(
                      AttributeKey.stringKey("db.scylladb.db.operation"))); // not supported so far

              assertNull(
                  tags.get(
                      AttributeKey.stringKey("db.scylladb.fetch_size"))); // was not set explicitly
              // no such information in BatchStatement:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.batch_size")));
              // no such information with VerbosityLevel.NORMAL:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.statement")));
              // no such information with operation DELETE:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.rows_count")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.has_more_pages")));
              // no such tags in "request" span:
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    speculativeExecutionsSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "speculative_execution");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.attempt_count")), "1");
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    attemptSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "attempt");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNotNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });
  }

  /** Basic test for creating spans with TRUNCATE statement. */
  @Test(groups = "short")
  public void simpleTracingTruncateTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            VerbosityLevel.FULL,
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              session.execute("TRUNCATE t");

              scope.close();
              userSpan.end();
            });

    // Retrieve the span created directly by tracer.
    final List<ReadableSpan> userSpans =
        spans.stream()
            .filter(span -> !span.getParentSpanContext().isValid()) // The root span has no parent.
            .collect(Collectors.toList());
    assertEquals(userSpans.size(), 1);
    final ReadableSpan userSpan = userSpans.get(0);

    for (ReadableSpan span : spans) {
      assertTrue(span.getSpanContext().isValid());
      assertTrue( // Each span is either the root span or has a valid parent span.
          span.getSpanContext().equals(userSpan.getSpanContext())
              || span.getParentSpanContext().isValid());
    }

    // Retrieve spans representing requests.
    final Collection<ReadableSpan> requestSpans =
        spans.stream()
            .filter(span -> span.getParentSpanContext().equals(userSpan.getSpanContext()))
            .collect(Collectors.toList());
    assertEquals(requestSpans.size(), 1);

    final Collection<ReadableSpan> speculativeExecutionsSpans =
        getChildrenOfSpans(spans, requestSpans);
    assertEquals(speculativeExecutionsSpans.size(), 1);

    final Collection<ReadableSpan> attemptSpans =
        getChildrenOfSpans(spans, speculativeExecutionsSpans);
    assertEquals(attemptSpans.size(), 1);

    requestSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "request");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              // tags generic for any (reasonable) statement
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.consistency_level")), "ONE");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.statement_type")), "regular");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.load_balancing_policy")),
                  "PagingOptimizingLoadBalancingPolicy");
              assertEquals(
                  tags.get(AttributeKey.stringKey("db.scylladb.speculative_execution_policy")),
                  "NoSpeculativeExecutionPolicy");

              assertNull(
                  tags.get(
                      AttributeKey.stringKey("db.scylladb.fetch_size"))); // was not set explicitly
              // no such information in RegularStatement:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.batch_size")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.keyspace")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.table")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.partition_key")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.db.operation")));
              // present with VerbosityLevel.FULL:
              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.statement")), "TRUNCATE t");
              // no such information with operation TRUNCATE:
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.rows_count")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.has_more_pages")));
              // no such tags in "request" span:
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    speculativeExecutionsSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "speculative_execution");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.attempt_count")), "1");
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    attemptSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "attempt");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNotNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });
  }

  /** Basic test for creating spans with SELECT statement. */
  @Test(groups = "short")
  public void simpleTracingSelectTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            VerbosityLevel.NORMAL,
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              SimpleStatement s =
                  new SimpleStatement("SELECT k FROM t WHERE v = 7 ALLOW FILTERING");
              s.setFetchSize(2);
              s.setIdempotent(true);
              s.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
              s.setConsistencyLevel(ConsistencyLevel.ALL);

              assertEquals(session.execute(s).all().size(), 7);

              scope.close();
              userSpan.end();
            });

    // Retrieve the span created directly by tracer.
    final List<ReadableSpan> userSpans =
        spans.stream()
            .filter(span -> !span.getParentSpanContext().isValid()) // The root span has no parent.
            .collect(Collectors.toList());
    assertEquals(userSpans.size(), 1);
    final ReadableSpan userSpan = userSpans.get(0);

    for (ReadableSpan span : spans) {
      assertTrue(span.getSpanContext().isValid());
      assertTrue( // Each span is either the root span or has a valid parent span.
          span.getSpanContext().equals(userSpan.getSpanContext())
              || span.getParentSpanContext().isValid());
    }

    // Retrieve spans representing requests.
    final Collection<ReadableSpan> requestSpans =
        spans.stream()
            .filter(span -> span.getParentSpanContext().equals(userSpan.getSpanContext()))
            .collect(Collectors.toList());
    assert requestSpans.size() >= 4;

    final Collection<ReadableSpan> speculativeExecutionsSpans =
        getChildrenOfSpans(spans, requestSpans);
    assert speculativeExecutionsSpans.size() >= 4;

    final Collection<ReadableSpan> attemptSpans =
        getChildrenOfSpans(spans, speculativeExecutionsSpans);
    assert attemptSpans.size() >= 4;

    boolean wasNoMorePages = false;
    int totalRows = 0;

    for (ReadableSpan requestSpan : requestSpans) {
      SpanData spanData = requestSpan.toSpanData();
      assertEquals(spanData.getName(), "request");
      assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
      Attributes tags = spanData.getAttributes();

      // tags generic for any (reasonable) statement
      assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.consistency_level")), "ALL");
      assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.fetch_size")), "2");
      assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.statement_type")), "regular");
      assertEquals(
          tags.get(AttributeKey.stringKey("db.scylladb.load_balancing_policy")),
          "PagingOptimizingLoadBalancingPolicy");
      assertEquals(
          tags.get(AttributeKey.stringKey("db.scylladb.speculative_execution_policy")),
          "NoSpeculativeExecutionPolicy");
      assertEquals(
          tags.get(AttributeKey.stringKey("db.scylladb.retry_policy")), "FallthroughRetryPolicy");

      // tags specific for SELECT statement
      final String hasMorePages = tags.get(AttributeKey.stringKey("db.scylladb.has_more_pages"));
      assertNotNull(hasMorePages);
      if (hasMorePages.equals("false")) wasNoMorePages = true;
      assertTrue(!(wasNoMorePages && hasMorePages.equals("true")));
      final String rowsCount = tags.get(AttributeKey.stringKey("db.scylladb.rows_count"));
      assertNotNull(rowsCount);
      totalRows += Integer.parseInt(rowsCount);

      // no such information in RegularStatement:
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.batch_size")));
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.keyspace")));
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.table")));
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.partition_key")));
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.db.operation")));
      // no such information with VerbosityLevel.NORMAL:
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.statement")));
      // no such tags in "request" span:
      assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
      assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
      assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
      assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
    }

    assertTrue(wasNoMorePages);
    assertEquals(totalRows, 7);

    speculativeExecutionsSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "speculative_execution");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertEquals(tags.get(AttributeKey.stringKey("db.scylladb.attempt_count")), "1");
              assertNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });

    attemptSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "attempt");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
              Attributes tags = spanData.getAttributes();

              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.name")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.ip")));
              assertNotNull(tags.get(AttributeKey.stringKey("net.peer.port")));
              assertNotNull(tags.get(AttributeKey.stringKey("db.scylladb.shard_id")));
            });
  }

  /** Basic test for creating spans with an erroneous statement. */
  @Test(groups = "short")
  public void simpleRequestErrorTracingTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            VerbosityLevel.FULL,
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              try {
                session.execute("INSERT ONTO t(k, v) VALUES (4, 2)");
                //                             ^ syntax error here
                assert false; // exception should be thrown before this line is executed
              } catch (SyntaxError error) {
                // pass
              }

              try {
                session.execute("INSERT INTO t(k, v) VALUES (2, 1, 3, 7)");
                //                                                  ^ too many values
                assert false; // exception should be thrown before this line is executed
              } catch (InvalidQueryException error) {
                // pass
              }

              scope.close();
              userSpan.end();
            });

    // Retrieve span created directly by tracer.
    final List<ReadableSpan> userSpans =
        spans.stream()
            .filter(span -> !span.getParentSpanContext().isValid())
            .collect(Collectors.toList());
    assertEquals(userSpans.size(), 1);
    final ReadableSpan userSpan = userSpans.get(0);

    for (ReadableSpan span : spans) {
      assertTrue(span.getSpanContext().isValid());
      assertTrue(
          span.getSpanContext().equals(userSpan.getSpanContext())
              || span.getParentSpanContext().isValid());
    }

    // Retrieve spans representing requests.
    final Collection<ReadableSpan> requestSpans =
        spans.stream()
            .filter(span -> span.getParentSpanContext().equals(userSpan.getSpanContext()))
            .collect(Collectors.toList());
    assertEquals(requestSpans.size(), 2);

    requestSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "request");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.ERROR);
              final String collectedStatement =
                  spanData.getAttributes().get(AttributeKey.stringKey("db.scylladb.statement"));
              assert collectedStatement.equals("INSERT INTO t(k, v) VALUES (2, 1, 3, 7)")
                      || collectedStatement.equals("INSERT ONTO t(k, v) VALUES (4, 2)")
                  : "Bad statement gathered";
            });
  }
}
