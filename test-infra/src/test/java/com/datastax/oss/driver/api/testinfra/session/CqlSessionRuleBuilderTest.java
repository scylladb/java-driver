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
package com.datastax.oss.driver.api.testinfra.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import org.junit.Test;

public class CqlSessionRuleBuilderTest {

  @Test
  public void should_leave_default_loader_selection_to_session_builder() {
    assertThat(CqlSessionRuleBuilder.copyForSession(null)).isNull();
  }

  @Test
  public void should_copy_typesafe_loader_for_session_isolation() {
    DefaultDriverConfigLoader original =
        DefaultDriverConfigLoader.fromString("datastax-java-driver {}");

    DriverConfigLoader copy = CqlSessionRuleBuilder.copyForSession(original);

    assertThat(copy).isInstanceOf(DefaultDriverConfigLoader.class).isNotSameAs(original);
    assertThat(((DefaultDriverConfigLoader) copy).getConfigSupplier())
        .isSameAs(original.getConfigSupplier());
  }

  @Test
  public void should_preserve_custom_loader() {
    DriverConfigLoader custom = DriverConfigLoader.fromMap(OptionsMap.driverDefaults());

    assertThat(CqlSessionRuleBuilder.copyForSession(custom)).isSameAs(custom);
  }
}
