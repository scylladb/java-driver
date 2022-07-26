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
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.core.tracing.TracingInfoFactory;
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
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

/** Tests for OpenTelemetry integration. */
public class OpenTelemetryTest extends CCMTestsSupport {
  /** Collects and saves spans. */
  private static final class SpansCollector implements SpanProcessor {
    final Collection<ReadableSpan> startedSpans =
        Collections.synchronizedList(new ArrayList<ReadableSpan>());
    final Collection<ReadableSpan> spans =
        Collections.synchronizedList(new ArrayList<ReadableSpan>());

    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {
      startedSpans.add(span);
    }

    @Override
    public boolean isStartRequired() {
      return true;
    }

    @Override
    public void onEnd(ReadableSpan span) {
      spans.add(span);
    }

    @Override
    public boolean isEndRequired() {
      return true;
    }

    public Collection<ReadableSpan> getSpans() {
      for (ReadableSpan span : startedSpans) {
        assertTrue(span.hasEnded());
      }

      return spans;
    }
  }

  private Session session;

  /**
   * Prepare OpenTelemetry configuration and run test with it.
   *
   * @param test test to run.
   * @return collected spans.
   */
  private Collection<ReadableSpan> collectSpans(BiConsumer<Tracer, TracingInfoFactory> test) {
    final Resource serviceNameResource =
        Resource.create(
            Attributes.of(ResourceAttributes.SERVICE_NAME, "Scylla Java driver - test"));

    final SpansCollector collector = new SpansCollector();

    final SdkTracerProvider tracerProvider =
        SdkTracerProvider.builder()
            .addSpanProcessor(collector)
            .setResource(Resource.getDefault().merge(serviceNameResource))
            .build();
    final OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();

    final Tracer tracer = openTelemetry.getTracerProvider().get("this");
    final OpenTelemetryTracingInfoFactory tracingInfoFactory =
        new OpenTelemetryTracingInfoFactory(tracer);
    cluster().setTracingInfoFactory(tracingInfoFactory);
    session = cluster().connect();

    session.execute("USE " + keyspace);
    session.execute("CREATE TABLE t (k int PRIMARY KEY, v int)");
    collector.getSpans().clear();

    test.accept(tracer, tracingInfoFactory);

    tracerProvider.close();
    cluster().setTracingInfoFactory(new NoopTracingInfoFactory());

    return collector.getSpans();
  }

  /** Basic test for creating spans. */
  @Test(groups = "short")
  public void simpleTracingTest() {
    final Collection<ReadableSpan> spans =
        collectSpans(
            (tracer, tracingInfoFactory) -> {
              Span userSpan = tracer.spanBuilder("user span").startSpan();
              Scope scope = userSpan.makeCurrent();

              session.execute("INSERT INTO t(k, v) VALUES (4, 2)");
              session.execute("INSERT INTO t(k, v) VALUES (2, 1)");

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
    final Collection<ReadableSpan> rootSpans =
        spans.stream()
            .filter(span -> span.getParentSpanContext().equals(userSpan.getSpanContext()))
            .collect(Collectors.toList());
    assertEquals(rootSpans.size(), 2);

    rootSpans.stream()
        .map(ReadableSpan::toSpanData)
        .forEach(
            spanData -> {
              assertEquals(spanData.getName(), "request");
              assertEquals(spanData.getStatus().getStatusCode(), StatusCode.OK);
            });
  }
}
