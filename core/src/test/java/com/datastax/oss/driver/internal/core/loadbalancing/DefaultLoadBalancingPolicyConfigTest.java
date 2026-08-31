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

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
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

  /**
   * The configured value is upper-cased before {@code valueOf}, so the fold must pin {@link
   * Locale#ROOT}: in a Turkish JVM the i of {@code preserve_replica_order} becomes a dotted
   * capital, {@code valueOf} then fails, and the policy silently falls back to the value the user
   * had asked for anyway — observable only as this warning. The fallback is why the assertion has
   * to be on the log rather than on the resolved method.
   */
  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_accept_valid_routing_method_in_any_default_locale(Locale locale) {
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(defaultProfile.getString(
            DefaultDriverOption.LOAD_BALANCING_DEFAULT_LWT_REQUEST_ROUTING_METHOD))
        .thenReturn("preserve_replica_order");

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME))
          .isNotNull();
    } finally {
      Locale.setDefault(def);
    }

    verify(appender, atLeast(0)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues())
        .extracting(ILoggingEvent::getFormattedMessage)
        .noneMatch(message -> message.contains("Unknown request routing method"));
  }

  @DataProvider
  public static Object[][] configurationCombinations() {
    return new Object[][] {
      {"PRESERVE_REPLICA_ORDER", false},
      {"REGULAR", true}
    };
  }
}
