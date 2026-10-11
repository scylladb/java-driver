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
package com.datastax.oss.driver.internal.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase.Credentials;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import org.junit.Test;

public class PlainTextAuthProviderTest {

  @Test
  public void should_read_configured_proxy_authorization_id() {
    DriverConfigLoader loader =
        DriverConfigLoader.fromString(
            "datastax-java-driver.advanced.auth-provider {"
                + " username = login, password = secret, authorization-id = proxy }");
    DriverContext context = mock(DriverContext.class);
    when(context.getSessionName()).thenReturn("test");
    when(context.getConfig()).thenReturn(loader.getInitialConfig());

    Credentials credentials =
        new PlainTextAuthProvider(context)
            .getCredentials(
                mock(EndPoint.class), "com.datastax.bdp.cassandra.auth.DseAuthenticator");

    assertThat(credentials.getUsername()).isEqualTo("login".toCharArray());
    assertThat(credentials.getPassword()).isEqualTo("secret".toCharArray());
    assertThat(credentials.getAuthorizationId()).isEqualTo("proxy".toCharArray());
  }
}
