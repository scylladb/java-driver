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
package com.datastax.oss.driver.internal.core.metrics;

import com.datastax.oss.driver.api.core.metrics.NodeMetric;

public interface NodeMetricUpdater extends MetricUpdater<NodeMetric> {

  /**
   * Takes over the metrics-expiration countdown from the updater this one is replacing, when a node
   * rebuilds its updater after its endpoint changed.
   *
   * <p>Without this the countdown is simply lost. It is armed and cancelled through {@code
   * node.getMetricUpdater()} -- by the metrics factories, on node state events -- so once a node
   * has swapped in a replacement, the cancel that a later UP event triggers reaches the new updater
   * and finds nothing, while the old updater's timer is still pending on an object nothing else
   * refers to. Both halves of that are wrong: the replacement never expires, because a node that is
   * already down will not produce another DOWN event to arm it, and the orphan eventually fires
   * {@link #clearMetrics()} on names it recomputes from whatever endpoint the node holds by then.
   *
   * <p>Implementations that do not expire metrics can ignore this.
   */
  default void adoptExpirationFrom(NodeMetricUpdater previous) {
    // nothing to hand over
  }
}
