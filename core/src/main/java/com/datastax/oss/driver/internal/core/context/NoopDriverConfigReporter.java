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
package com.datastax.oss.driver.internal.core.context;

import java.util.Map;
import net.jcip.annotations.ThreadSafe;

/**
 * A {@link DriverConfigReporter} that reports nothing, used when the driver cannot build a report
 * at all.
 *
 * <p>Today that means Jackson is absent from the classpath: the driver declares it as a required
 * dependency but documents that it can be excluded (see {@code manual/core/integration}), and
 * {@link DefaultDriverConfigReporter} serializes the report with it. Substituting this
 * implementation keeps that exclusion working.
 *
 * <p>Deliberately free of any reference to Jackson, including in the signatures it inherits — a
 * single one would make loading <em>this</em> class fail for exactly the deployments it exists to
 * serve. Note that the failure it avoids is a {@link NoClassDefFoundError} raised while linking the
 * default implementation, which is an {@code Error} rather than an exception, so no {@code
 * try}/{@code catch} on the connection path could stand in for choosing the right implementation
 * here.
 */
@ThreadSafe
public class NoopDriverConfigReporter implements DriverConfigReporter {

  @Override
  public void populateControlConnectionOptions(Map<String, String> startupOptions) {
    // nothing to do
  }
}
