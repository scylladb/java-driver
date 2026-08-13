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

import com.google.common.annotations.Beta;

/**
 * Builds the {@code DRIVER_CONFIG} payload that the control connection sends in its CQL {@code
 * STARTUP} options, so ScyllaDB can store it in {@code system.clients.client_options} and operators
 * can inspect a client's effective driver settings while investigating incidents.
 *
 * <p>Only the control connection carries the blob; pooled connections are correlated back to it via
 * the {@code SESSION_ID} startup option, which {@link Connection} sends on every connection
 * independently of this reporter and of {@link Cluster.Builder#withDriverConfigReporting(boolean)}.
 *
 * <p>{@code system.clients.client_options} is per node: only the node holding the control
 * connection stores {@code DRIVER_CONFIG}; other nodes only see {@code SESSION_ID}-bearing pooled-
 * connection rows. Consumers must query and aggregate across all nodes to see the full picture.
 */
@Beta
public interface DriverConfigReporter {

  /**
   * Builds the configuration report, or returns {@code null} if it could not be built (in which
   * case no {@code DRIVER_CONFIG} option is sent).
   *
   * <p>Called once per control connection, from its {@code STARTUP} frame assembly, and not cached:
   * the report describes the objects in force at the handshake that sends it, rather than the
   * configuration the {@link Cluster} was constructed from. Implementations must therefore tolerate
   * being called on a Netty event loop, and repeatedly over a cluster's lifetime.
   *
   * <p><b>Implementations must not throw:</b> a failure to build the report must be swallowed (and
   * logged) rather than propagated, so that a diagnostic aid can never break cluster
   * initialization. For the same reason they must bound the report's size and return {@code null}
   * rather than an oversized one: {@code STARTUP} option values carry an unchecked 16-bit length
   * prefix, so a value over 65535 encoded bytes corrupts the frame instead of merely being useless.
   * The built-in reporter caps the report at 32KiB of UTF-8, the limit the other ScyllaDB drivers
   * apply.
   *
   * <p>An implementation that cannot even be <em>loaded</em> is handled one level up rather than
   * here: {@link DefaultDriverConfigReporter} needs Jackson, and on a classpath without it the
   * failure is a {@link LinkageError} raised while initializing the class, which no method of this
   * interface could catch. {@code Connection.Factory.buildDriverConfigReport} contains that and
   * reports nothing, so the connection is still established.
   *
   * @return the report to send under the {@code DRIVER_CONFIG} startup option, or {@code null} to
   *     send nothing.
   */
  String buildReport();
}
