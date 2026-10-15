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
import static org.mockito.Mockito.mock;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import java.util.List;
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
  public void should_reject_all_legacy_options_for_builtin_plaintext_providers() {
    for (String providerClass :
        List.of(
            "PlainTextAuthProvider",
            "com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider")) {
      for (OptionsMap options : legacyOptions()) {
        options.put(TypedDriverOption.AUTH_PROVIDER_CLASS, providerClass);
        assertThatThrownBy(
                () ->
                    new ValidationBuilder()
                        .withConfigLoader(DriverConfigLoader.fromMap(options))
                        .buildAsync()
                        .toCompletableFuture()
                        .join())
            .hasCauseInstanceOf(UnsupportedOperationException.class);
      }
    }
    for (OptionsMap options : legacyOptions()) {
      options.put(TypedDriverOption.AUTH_PROVIDER_CLASS, "custom.Provider");
      assertThatThrownBy(
              () ->
                  new ValidationBuilder()
                      .withAuthCredentials("u", "p")
                      .withConfigLoader(DriverConfigLoader.fromMap(options))
                      .buildAsync()
                      .toCompletableFuture()
                      .join())
          .hasCauseInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  public void should_allow_custom_providers_to_own_legacy_option_names() {
    for (OptionsMap options : legacyOptions()) {
      options.put(TypedDriverOption.AUTH_PROVIDER_CLASS, "custom.Provider");
      assertValidationPasses(new ValidationBuilder(), options);
      options.put(TypedDriverOption.AUTH_PROVIDER_CLASS, "PlainTextAuthProvider");
      assertValidationPasses(
          new ValidationBuilder().withAuthProvider(mock(AuthProvider.class)), options);
      assertValidationPasses(
          new ValidationBuilder()
              .withAuthProvider(new ProgrammaticPlainTextAuthProvider("u", "p") {}),
          options);
    }
  }

  private static List<OptionsMap> legacyOptions() {
    OptionsMap service = OptionsMap.driverDefaults();
    service.put(TypedDriverOption.AUTH_PROVIDER_SERVICE, "dse");
    OptionsMap authorizationId = OptionsMap.driverDefaults();
    authorizationId.put(TypedDriverOption.AUTH_PROVIDER_AUTHORIZATION_ID, "role");
    OptionsMap login = OptionsMap.driverDefaults();
    login.put(TypedDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION, "login");
    OptionsMap sasl = OptionsMap.driverDefaults();
    sasl.put(TypedDriverOption.AUTH_PROVIDER_SASL_PROPERTIES, Map.of("qop", "auth"));
    return List.of(service, authorizationId, login, sasl);
  }

  private static void assertValidationPasses(ValidationBuilder builder, OptionsMap options) {
    assertThatThrownBy(
            () ->
                builder
                    .withConfigLoader(DriverConfigLoader.fromMap(options))
                    .buildAsync()
                    .toCompletableFuture()
                    .join())
        .hasCauseInstanceOf(ValidationPassed.class);
  }

  private static class ValidationPassed extends RuntimeException {}

  private static class ValidationBuilder extends SessionBuilder<ValidationBuilder, CqlSession> {
    @Override
    protected DriverContext buildContext(
        DriverConfigLoader loader, ProgrammaticArguments arguments) {
      // Stop after configuration validation without allocating resources or opening connections.
      throw new ValidationPassed();
    }

    @Override
    protected CqlSession wrap(CqlSession session) {
      return session;
    }
  }
}
