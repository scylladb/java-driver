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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DropwizardMetricsFactoryTest {

  @Test
  public void should_throw_if_registry_of_wrong_type() {
    // given
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    DriverConfig config = mock(DriverConfig.class);
    List<String> enabledMetrics =
        Collections.singletonList(DefaultSessionMetric.CQL_REQUESTS.getPath());
    // when
    when(config.getDefaultProfile()).thenReturn(profile);
    when(context.getConfig()).thenReturn(config);
    when(context.getSessionName()).thenReturn("MockSession");
    // registry object is not a registry type
    when(context.getMetricRegistry()).thenReturn(Integer.MAX_VALUE);
    when(profile.getDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER))
        .thenReturn(Duration.ofHours(1));
    when(profile.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED))
        .thenReturn(enabledMetrics);
    // then
    try {
      new DropwizardMetricsFactory(context);
      fail(
          "MetricsFactory should require correct registry object type: "
              + MetricRegistry.class.getName());
    } catch (IllegalArgumentException iae) {
      assertThat(iae.getMessage())
          .isEqualTo(
              "Unexpected Metrics registry object. "
                  + "Expected registry object to be of type '%s', but was '%s'",
              MetricRegistry.class.getName(), Integer.class.getName());
    }
  }

  @Test
  @DataProvider({"DOWN", "FORCED_DOWN", "UP", "UNKNOWN", "null"})
  public void should_ignore_contact_point_state_events(NodeState state) {
    InternalDriverContext context = newContext();
    DropwizardMetricsFactory factory = new DropwizardMetricsFactory(context);
    DefaultNode contactPoint =
        DefaultNode.newContactPoint(
            new DefaultEndPoint(new InetSocketAddress("127.0.0.1", 9042)), context);

    factory.processNodeStateEvent(stateEvent(contactPoint, state));
  }

  @Test
  @DataProvider({"DOWN", "FORCED_DOWN", "null"})
  public void should_start_metrics_expiration_when_node_goes_down_or_is_removed(NodeState state) {
    DropwizardMetricsFactory factory = new DropwizardMetricsFactory(newContext());
    DefaultNode node = mock(DefaultNode.class);
    DropwizardNodeMetricUpdater updater = mock(DropwizardNodeMetricUpdater.class);
    when(node.getMetricUpdater()).thenReturn(updater);

    factory.processNodeStateEvent(stateEvent(node, state));

    verify(updater).startMetricsExpirationTimeout();
    verifyNoMoreInteractions(updater);
  }

  @Test
  @DataProvider({"UP", "UNKNOWN"})
  public void should_cancel_metrics_expiration_when_node_comes_up_or_is_added(NodeState state) {
    DropwizardMetricsFactory factory = new DropwizardMetricsFactory(newContext());
    DefaultNode node = mock(DefaultNode.class);
    DropwizardNodeMetricUpdater updater = mock(DropwizardNodeMetricUpdater.class);
    when(node.getMetricUpdater()).thenReturn(updater);

    factory.processNodeStateEvent(stateEvent(node, state));

    verify(updater).cancelMetricsExpirationTimeout();
    verifyNoMoreInteractions(updater);
  }

  private static InternalDriverContext newContext() {
    InternalDriverContext context = mock(InternalDriverContext.class);
    DriverConfig config = mock(DriverConfig.class);
    DriverExecutionProfile profile = mock(DriverExecutionProfile.class);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(profile);
    when(profile.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED))
        .thenReturn(Collections.emptyList());
    when(profile.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED))
        .thenReturn(Collections.emptyList());
    return context;
  }

  private static NodeStateEvent stateEvent(DefaultNode node, NodeState state) {
    if (state == null) {
      return NodeStateEvent.removed(node);
    } else if (state == NodeState.UNKNOWN) {
      return NodeStateEvent.added(node);
    } else {
      return NodeStateEvent.changed(NodeState.UNKNOWN, state, node);
    }
  }
}
