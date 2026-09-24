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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class DefaultLoadBalancingPolicyConfigTest extends LoadBalancingPolicyTestBase {

  @Before
  @Override
  public void setup() {
    MockitoAnnotations.initMocks(this);
    super.setup();
  }

  @Test
  @DataProvider(value = {"REGULAR", "regular", "PRESERVE_REPLICA_ORDER", "Preserve_Replica_Order"})
  public void should_accept_valid_routing_methods(String routingMethod) {
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn(routingMethod);
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
    assertThat(policy).isNotNull();
  }

  @Test
  @DataProvider(
      value = {"INVALID_METHOD", "", "@#$%^&*()", "  REGULAR  "},
      trimValues = false)
  public void should_default_to_preserve_replica_order_for_invalid_routing_methods(
      String invalidValue) {
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn(invalidValue);
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    assertThat(policy).isNotNull();

    verify(appender).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getValue().getFormattedMessage())
        .contains("Unknown request routing method")
        .contains("defaulting to PRESERVE_REPLICA_ORDER");
  }

  @Test
  @UseDataProvider("configurationCombinations")
  public void should_accept_configuration_combinations(
      String routingMethod, boolean slowAvoidance) {
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));

    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn(routingMethod);
    when(defaultProfile.getBoolean(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true))
        .thenReturn(slowAvoidance);

    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
    assertThat(policy).isNotNull();

    verify(defaultProfile, atLeast(1))
        .getBoolean(DefaultDriverOption.LOAD_BALANCING_POLICY_SLOW_AVOIDANCE, true);
  }

  @DataProvider
  public static Object[][] configurationCombinations() {
    return new Object[][] {
      {"PRESERVE_REPLICA_ORDER", false},
      {"REGULAR", true}
    };
  }
}
