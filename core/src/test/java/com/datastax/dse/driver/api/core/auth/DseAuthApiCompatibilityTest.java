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
package com.datastax.dse.driver.api.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.Map;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class DseAuthApiCompatibilityTest {

  @Test
  public void should_keep_removed_credential_signatures_and_reject_proxy_authentication() {
    assertThatThrownBy(() -> CqlSession.builder().withAuthCredentials("u", "p", "role"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> CqlSession.builder().withCredentials("u", "p", "role"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new ProgrammaticPlainTextAuthProvider("u", "p", "role"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () ->
                new PlainTextAuthProviderBase.Credentials(
                    new char[0], new char[0], "role".toCharArray()))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () -> ProxyAuthentication.executeAs("role", SimpleStatement.newInstance("SELECT 1")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_accept_empty_authorization_ids_for_ordinary_authentication() {
    assertThat(CqlSession.builder().withAuthCredentials("u", "p", "")).isNotNull();
    assertThat(CqlSession.builder().withCredentials("u", "p", "")).isNotNull();
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("u", "p", "");
    provider.setAuthorizationId("");
    assertThat(
            new PlainTextAuthProviderBase.Credentials(new char[0], new char[0], new char[0])
                .getAuthorizationId())
        .isEmpty();
    assertThat(
            new PlainTextAuthProviderBase.Credentials(new char[0], new char[0])
                .getAuthorizationId())
        .isEmpty();
    assertThatThrownBy(() -> provider.setAuthorizationId("role"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_null_authorization_ids() {
    assertThatThrownBy(() -> CqlSession.builder().withAuthCredentials("u", "p", null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> CqlSession.builder().withCredentials("u", "p", null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new ProgrammaticPlainTextAuthProvider("u", "p", null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> new ProgrammaticPlainTextAuthProvider("u", "p").setAuthorizationId(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> new PlainTextAuthProviderBase.Credentials(new char[0], new char[0], null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void should_keep_gssapi_options_and_reject_use() {
    assertThat(DseGssApiAuthProviderBase.DEFAULT_SASL_SERVICE_NAME).isEqualTo("dse");
    assertThatThrownBy(() -> DseGssApiAuthProviderBase.GssApiOptions.builder().build())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () ->
                DseGssApiAuthProviderBase.GssApiOptions.Builder.fetchLoginConfiguration(
                    Map.of("principal", "user")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(TypedDriverOption.AUTH_PROVIDER_SERVICE.getRawOption())
        .isSameAs(DseDriverOption.AUTH_PROVIDER_SERVICE);
  }

  @Test
  public void should_reject_gssapi_authenticator_construction_for_any_server() {
    assertThatThrownBy(
            () ->
                new DseGssApiAuthProviderBase.GssApiAuthenticator(
                    null, null, "org.apache.cassandra.auth.PasswordAuthenticator"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new DseGssApiAuthProviderBase.GssApiAuthenticator(null, null, null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_legacy_authentication_configuration_before_connecting() {
    OptionsMap options = OptionsMap.driverDefaults();
    options.put(TypedDriverOption.AUTH_PROVIDER_SERVICE, "dse");

    assertThatThrownBy(
            () ->
                CqlSession.builder()
                    .withConfigLoader(DriverConfigLoader.fromMap(options))
                    .buildAsync()
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(UnsupportedOperationException.class);
  }
}
