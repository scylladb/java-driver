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
import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.UUID;
import org.junit.Test;

public class ClientRoutesSessionBuilderTest {

  // ---------------------------------------------------------------------------
  // SessionBuilder integration tests
  // ---------------------------------------------------------------------------

  @Test
  public void should_set_client_routes_config_programmatically() {
    String connectionId = UUID.randomUUID().toString();
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId, "host"))
            .build();

    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(config);

    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig())
        .isEqualTo(config);
  }

  @Test
  public void should_allow_null_client_routes_config() {
    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(null);

    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig()).isNull();
  }

  @Test
  public void should_preserve_config_through_chained_builder_calls() {
    String connectionId1 = UUID.randomUUID().toString();
    String connectionId2 = UUID.randomUUID().toString();
    ClientRoutesConfig config1 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId1, "host1"))
            .build();
    ClientRoutesConfig config2 =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(connectionId2, "host2"))
            .build();

    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(config1);
    builder.withClientRoutesConfig(config2);

    // Last call wins
    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig())
        .isEqualTo(config2);
  }

  @Test
  public void should_override_config_with_null() {
    ClientRoutesConfig config =
        ClientRoutesConfig.builder()
            .addEndpoint(new ClientRouteProxy(UUID.randomUUID().toString(), "host"))
            .build();

    TestSessionBuilder builder = new TestSessionBuilder();
    builder.withClientRoutesConfig(config);
    builder.withClientRoutesConfig(null);

    assertThat(builder.programmaticArgumentsBuilder.build().getClientRoutesConfig()).isNull();
  }

  /** Test subclass to access protected fields. */
  private static class TestSessionBuilder extends SessionBuilder<TestSessionBuilder, CqlSession> {
    @Override
    protected CqlSession wrap(@NonNull CqlSession defaultSession) {
      return mock(CqlSession.class);
    }
  }
}
