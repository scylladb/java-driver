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

import com.datastax.driver.core.tracing.NoopTracingInfoFactory;
import com.datastax.driver.core.tracing.TracingInfo;
import com.datastax.driver.core.tracing.TracingInfoFactory;
import com.datastax.driver.core.tracing.VerbosityLevel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

public class OpenTelemetryTracingInfoFactory implements TracingInfoFactory {
  private final Tracer tracer;
  private final VerbosityLevel precision;

  public OpenTelemetryTracingInfoFactory(final Tracer tracer) {
    this(tracer, VerbosityLevel.NORMAL);
  }

  public OpenTelemetryTracingInfoFactory(final Tracer tracer, final VerbosityLevel precision) {
    this.tracer = tracer;
    this.precision = precision;
  }

  @Override
  public TracingInfo buildTracingInfo() {
    final Context current = Context.current();
    return new OpenTelemetryTracingInfo(tracer, current, precision);
  }

  @Override
  public TracingInfo buildTracingInfo(TracingInfo parent) {
    if (parent instanceof OpenTelemetryTracingInfo) {
      final OpenTelemetryTracingInfo castedParent = (OpenTelemetryTracingInfo) parent;
      return new OpenTelemetryTracingInfo(
          castedParent.getTracer(), castedParent.getContext(), castedParent.getVerbosity());
    }

    return new NoopTracingInfoFactory().buildTracingInfo();
  }

  public TracingInfo buildTracingInfo(Span parent) {
    final Context current = Context.current().with(parent);
    return new OpenTelemetryTracingInfo(tracer, current, precision);
  }
}
