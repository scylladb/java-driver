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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class TestTracingInfoFactory implements TracingInfoFactory {
  private final VerbosityLevel precision;
  private Collection<TracingInfo> spans =
      Collections.synchronizedList(new ArrayList<TracingInfo>());

  public TestTracingInfoFactory() {
    this.precision = VerbosityLevel.NORMAL;
  }

  public TestTracingInfoFactory(final VerbosityLevel precision) {
    this.precision = precision;
  }

  @Override
  public TracingInfo buildTracingInfo() {
    TracingInfo tracingInfo = new TestTracingInfo(precision);
    spans.add(tracingInfo);
    return tracingInfo;
  }

  @Override
  public TracingInfo buildTracingInfo(TracingInfo parent) {
    TracingInfo tracingInfo;

    if (parent instanceof TestTracingInfo) {
      final TestTracingInfo castedParent = (TestTracingInfo) parent;
      tracingInfo = new TestTracingInfo(castedParent.getVerbosity(), parent);
      spans.add(tracingInfo);
      return tracingInfo;
    }

    tracingInfo = new NoopTracingInfoFactory().buildTracingInfo();
    spans.add(tracingInfo);
    return tracingInfo;
  }

  public Collection<TracingInfo> getSpans() {
    return spans;
  }
}
