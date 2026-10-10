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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase.Credentials;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Strict.class)
public class ProgrammaticPlainTextAuthProviderTest {

  @Mock private EndPoint endpoint;

  @Test
  public void should_encode_standard_sasl_plain_response() {
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass");

    ByteBuffer response =
        provider
            .newAuthenticator(endpoint, "org.apache.cassandra.auth.PasswordAuthenticator")
            .initialResponse()
            .toCompletableFuture()
            .join();

    assertThat(response)
        .isEqualTo(ByteBuffer.wrap(new byte[] {0, 'u', 's', 'e', 'r', 0, 'p', 'a', 's', 's'}));
  }

  @Test
  public void should_use_standard_sasl_plain_response_for_dse_authenticator() {
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass");

    ByteBuffer response =
        provider
            .newAuthenticator(endpoint, "com.datastax.bdp.cassandra.auth.DseAuthenticator")
            .initialResponse()
            .toCompletableFuture()
            .join();

    assertThat(response)
        .isEqualTo(ByteBuffer.wrap(new byte[] {0, 'u', 's', 'e', 'r', 0, 'p', 'a', 's', 's'}));
  }

  @Test
  public void should_reject_dse_plain_start_challenge() {
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass");

    assertThatExceptionOfType(CompletionException.class)
        .isThrownBy(
            () ->
                provider
                    .newAuthenticator(endpoint, "com.datastax.bdp.cassandra.auth.DseAuthenticator")
                    .evaluateChallenge(
                        ByteBuffer.wrap("PLAIN-START".getBytes(StandardCharsets.UTF_8)))
                    .toCompletableFuture()
                    .join())
        .withCauseInstanceOf(AuthenticationException.class);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_preserve_legacy_plaintext_authenticator_constructor() {
    PlainTextAuthProviderBase.PlainTextAuthenticator authenticator =
        new PlainTextAuthProviderBase.PlainTextAuthenticator(
            new Credentials("user".toCharArray(), "pass".toCharArray()));

    assertThat(authenticator.initialResponse().toCompletableFuture().join())
        .isEqualTo(ByteBuffer.wrap(new byte[] {0, 'u', 's', 'e', 'r', 0, 'p', 'a', 's', 's'}));
  }

  @Test
  public void should_return_correct_credentials() {
    // given
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass");
    // when
    Credentials credentials = provider.getCredentials(endpoint, "irrelevant");
    // then
    assertThat(credentials.getUsername()).isEqualTo("user".toCharArray());
    assertThat(credentials.getPassword()).isEqualTo("pass".toCharArray());
  }

  @Test
  public void should_change_username() {
    // given
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass");
    // when
    provider.setUsername("user2");
    Credentials credentials = provider.getCredentials(endpoint, "irrelevant");
    // then
    assertThat(credentials.getUsername()).isEqualTo("user2".toCharArray());
    assertThat(credentials.getPassword()).isEqualTo("pass".toCharArray());
  }

  @Test
  public void should_change_password() {
    // given
    ProgrammaticPlainTextAuthProvider provider =
        new ProgrammaticPlainTextAuthProvider("user", "pass");
    // when
    provider.setPassword("pass2");
    Credentials credentials = provider.getCredentials(endpoint, "irrelevant");
    // then
    assertThat(credentials.getUsername()).isEqualTo("user".toCharArray());
    assertThat(credentials.getPassword()).isEqualTo("pass2".toCharArray());
  }
}
