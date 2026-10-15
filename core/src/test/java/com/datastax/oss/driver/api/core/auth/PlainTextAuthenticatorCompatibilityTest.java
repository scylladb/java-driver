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
package com.datastax.oss.driver.api.core.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.datastax.dse.driver.api.core.auth.BaseDseAuthenticator;
import com.datastax.dse.driver.api.core.auth.DsePlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase.Credentials;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase.PlainTextAuthenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class PlainTextAuthenticatorCompatibilityTest {
  private static final String PASSWORD_AUTHENTICATOR =
      "org.apache.cassandra.auth.PasswordAuthenticator";
  private static final String DSE_AUTHENTICATOR =
      "com.datastax.bdp.cassandra.auth.DseAuthenticator";
  private final EndPoint endPoint = mock(EndPoint.class);

  @Test
  public void should_preserve_all_plaintext_constructors_and_base_type() {
    Credentials credentials =
        new Credentials("user".toCharArray(), "pass".toCharArray(), new char[0]);
    BaseDseAuthenticator[] authenticators = {
      new PlainTextAuthenticator(credentials),
      new PlainTextAuthenticator(credentials, endPoint),
      new PlainTextAuthenticator(credentials, endPoint, PASSWORD_AUTHENTICATOR),
      new PlainTextAuthenticator(credentials, endPoint, "custom.Authenticator")
    };
    for (BaseDseAuthenticator authenticator : authenticators) {
      assertThat(authenticator.initialResponseSync()).isEqualTo(plainResponse());
      authenticator.onAuthenticationSuccessSync(null);
      assertThat(authenticator.evaluateChallengeSync(utf8("PLAIN-START")))
          .isEqualTo(plainResponse());
      assertThatThrownBy(() -> authenticator.evaluateChallengeSync(utf8("unexpected")))
          .isInstanceOf(AuthenticationException.class);
      assertThatThrownBy(() -> authenticator.evaluateChallengeSync(null))
          .isInstanceOf(AuthenticationException.class);
    }
  }

  @Test
  public void should_dispatch_initial_response_through_legacy_subclass_hooks() {
    PlainTextAuthenticator authenticator =
        new PlainTextAuthenticator(
            new Credentials("user".toCharArray(), "pass".toCharArray()),
            endPoint,
            PASSWORD_AUTHENTICATOR) {
          @Override
          public ByteBuffer getInitialServerChallenge() {
            return utf8("custom challenge");
          }

          @Override
          public ByteBuffer evaluateChallengeSync(ByteBuffer challenge) {
            assertThat(challenge).isEqualTo(utf8("custom challenge"));
            return utf8("custom response");
          }
        };
    assertThat(authenticator.initialResponseSync()).isEqualTo(utf8("custom response"));
  }

  @Test
  public void should_preserve_plain_hooks_with_independent_buffer_positions() {
    PlainTextAuthenticator authenticator =
        new PlainTextAuthenticator(new Credentials(new char[0], new char[0]), endPoint);
    assertThat(authenticator.getMechanism()).isEqualTo(utf8("PLAIN"));
    assertThat(authenticator.getInitialServerChallenge()).isEqualTo(utf8("PLAIN-START"));
    authenticator.getMechanism().get();
    authenticator.getInitialServerChallenge().get();
    assertThat(authenticator.getMechanism()).isEqualTo(utf8("PLAIN"));
    assertThat(authenticator.getInitialServerChallenge()).isEqualTo(utf8("PLAIN-START"));
  }

  @Test
  public void should_encode_ordinary_response_after_empty_authorization_id_updates() {
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass", "");
    provider.setAuthorizationId("");
    assertThatThrownBy(() -> provider.setAuthorizationId("role"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(
            provider
                .newAuthenticator(endPoint, PASSWORD_AUTHENTICATOR)
                .initialResponse()
                .toCompletableFuture()
                .join())
        .isEqualTo(plainResponse());
  }

  @Test
  public void should_preserve_legacy_plain_provider_subclasses() {
    DsePlainTextAuthProviderBase provider =
        new DsePlainTextAuthProviderBase("") {
          @Override
          protected Credentials getCredentials(EndPoint endpoint, String serverAuthenticator) {
            return new Credentials("user".toCharArray(), "pass".toCharArray(), new char[0]);
          }

          @Override
          public Authenticator newAuthenticator(EndPoint endpoint, String serverAuthenticator) {
            return new PlainTextAuthenticator(
                getCredentials(endpoint, serverAuthenticator), endpoint, serverAuthenticator);
          }
        };
    assertThat(
            provider
                .newAuthenticator(endPoint, PASSWORD_AUTHENTICATOR)
                .initialResponse()
                .toCompletableFuture()
                .join())
        .isEqualTo(plainResponse());
  }

  @Test
  public void should_reject_dse_server_authenticator() {
    Credentials credentials = new Credentials("user".toCharArray(), "pass".toCharArray());
    assertThatThrownBy(() -> new PlainTextAuthenticator(credentials, endPoint, DSE_AUTHENTICATOR))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () ->
                new ProgrammaticPlainTextAuthProvider("user", "pass")
                    .newAuthenticator(endPoint, DSE_AUTHENTICATOR))
        .isInstanceOf(AuthenticationException.class);
    assertThatThrownBy(() -> new PlainTextAuthenticator(credentials, endPoint, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void should_reject_custom_proxy_credentials_before_reading_password() {
    Credentials credentials =
        new Credentials(new char[0], new char[0]) {
          @Override
          public char[] getAuthorizationId() {
            return "role".toCharArray();
          }

          @Override
          public char[] getPassword() {
            throw new AssertionError("Must reject the proxy identity before encoding credentials");
          }
        };
    assertThatThrownBy(() -> new PlainTextAuthenticator(credentials))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new PlainTextAuthenticator(credentials, endPoint))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
            () -> new PlainTextAuthenticator(credentials, endPoint, PASSWORD_AUTHENTICATOR))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_null_authorization_id_from_custom_credentials() {
    Credentials credentials =
        new Credentials(new char[0], new char[0]) {
          @Override
          public char[] getAuthorizationId() {
            return null;
          }
        };
    assertThatThrownBy(() -> new PlainTextAuthenticator(credentials, endPoint))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void should_preserve_ordinary_base_authenticator_behavior() {
    BaseDseAuthenticator authenticator = new LegacyBaseAuthenticator(PASSWORD_AUTHENTICATOR);
    assertThat(authenticator.initialResponseSync()).isEqualTo(plainResponse());
    authenticator.onAuthenticationSuccessSync(null);
    assertThatThrownBy(() -> new LegacyBaseAuthenticator(DSE_AUTHENTICATOR))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static class LegacyBaseAuthenticator extends BaseDseAuthenticator {
    LegacyBaseAuthenticator(String serverAuthenticator) {
      super(serverAuthenticator);
    }

    @Override
    protected ByteBuffer getMechanism() {
      return utf8("PLAIN");
    }

    @Override
    protected ByteBuffer getInitialServerChallenge() {
      return utf8("PLAIN-START");
    }

    @Override
    public ByteBuffer evaluateChallengeSync(ByteBuffer challenge) {
      assertThat(challenge).isEqualTo(utf8("PLAIN-START"));
      return plainResponse();
    }
  }

  private static ByteBuffer utf8(String value) {
    return ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8));
  }

  private static ByteBuffer plainResponse() {
    return ByteBuffer.wrap(new byte[] {0, 'u', 's', 'e', 'r', 0, 'p', 'a', 's', 's'});
  }
}
