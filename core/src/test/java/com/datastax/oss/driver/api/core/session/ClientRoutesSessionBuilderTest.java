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
package com.datastax.oss.driver.api.core.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRoutesEndpoint;
import java.util.UUID;
import org.junit.Test;

public class ClientRoutesSessionBuilderTest {

  @Test
  public void should_set_client_routes_config_programmatically() {
    UUID connectionId = UUID.randomUUID();
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRoutesEndpoint(connectionId, "host:9042"))
            .build();

    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(config);

    assertThat(builder.clientRoutesConfig).isEqualTo(config);
    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig())
        .isEqualTo(config);
  }

  @Test
  public void should_allow_null_client_routes_config() {
    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(null);

    assertThat(builder.clientRoutesConfig).isNull();
    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig()).isNull();
  }

  /** Test subclass to access protected fields. */
  private static class TestSessionBuilder extends SessionBuilder<TestSessionBuilder, CqlSession> {
    @Override
    protected CqlSession wrap(CqlSession defaultSession) {
      // Return a mock instead of manually implementing all methods
      return mock(CqlSession.class);
    }
  }
}
