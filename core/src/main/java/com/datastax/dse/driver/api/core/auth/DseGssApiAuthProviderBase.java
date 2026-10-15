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

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;

/** @deprecated DSE GSSAPI authentication is no longer supported. */
@Deprecated
@ThreadSafe
public abstract class DseGssApiAuthProviderBase implements AuthProvider {

  public static final String DEFAULT_SASL_SERVICE_NAME = "dse";
  public static final String SASL_SERVICE_NAME_PROPERTY = "dse.sasl.service";

  protected DseGssApiAuthProviderBase(@NonNull String logPrefix) {
    throw unsupported();
  }

  @NonNull
  protected abstract GssApiOptions getOptions(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator);

  @NonNull
  @Override
  public Authenticator newAuthenticator(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    throw unsupported();
  }

  @Override
  public void onMissingChallenge(@NonNull EndPoint endPoint) {
    throw unsupported();
  }

  @Override
  public void close() {}

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("DSE GSSAPI authentication is no longer supported");
  }

  /** @deprecated DSE GSSAPI authentication is no longer supported. */
  @Deprecated
  @Immutable
  public static class GssApiOptions {

    private GssApiOptions() {}

    @NonNull
    public static Builder builder() {
      return new Builder();
    }

    @Nullable
    public Configuration getLoginConfiguration() {
      throw unsupported();
    }

    @Nullable
    public Subject getSubject() {
      throw unsupported();
    }

    @Nullable
    public String getSaslProtocol() {
      throw unsupported();
    }

    @Nullable
    public String getAuthorizationId() {
      throw unsupported();
    }

    @NonNull
    public Map<String, String> getSaslProperties() {
      throw unsupported();
    }

    /** @deprecated DSE GSSAPI authentication is no longer supported. */
    @Deprecated
    @NotThreadSafe
    public static class Builder {

      public Builder() {}

      @NonNull
      public Builder withLoginConfiguration(@Nullable Configuration loginConfiguration) {
        throw unsupported();
      }

      @NonNull
      public Builder withLoginConfiguration(@Nullable Map<String, String> loginConfiguration) {
        throw unsupported();
      }

      @NonNull
      public Builder withSubject(@Nullable Subject subject) {
        throw unsupported();
      }

      @NonNull
      public Builder withSaslProtocol(@Nullable String saslProtocol) {
        throw unsupported();
      }

      @NonNull
      public Builder withAuthorizationId(@Nullable String authorizationId) {
        throw unsupported();
      }

      @NonNull
      public Builder addSaslProperty(@NonNull String name, @NonNull String value) {
        throw unsupported();
      }

      @NonNull
      public GssApiOptions build() {
        throw unsupported();
      }

      @SuppressWarnings("DoNotCallSuggester")
      public static Configuration fetchLoginConfiguration(@Nullable Map<String, String> options) {
        throw unsupported();
      }
    }
  }

  /** @deprecated DSE GSSAPI authentication is no longer supported. */
  @Deprecated
  @ThreadSafe
  protected static class GssApiAuthenticator extends BaseDseAuthenticator {

    // Keep the unused parameters for compatibility with existing subclasses.
    @SuppressWarnings("unused")
    protected GssApiAuthenticator(
        @Nullable GssApiOptions options,
        @Nullable EndPoint endPoint,
        @Nullable String serverAuthenticator) {
      // Do not validate the unused server name before rejecting unsupported GSSAPI.
      super("org.apache.cassandra.auth.PasswordAuthenticator");
      throw unsupported();
    }

    @NonNull
    @Override
    protected ByteBuffer getMechanism() {
      throw unsupported();
    }

    @NonNull
    @Override
    protected ByteBuffer getInitialServerChallenge() {
      throw unsupported();
    }

    @Nullable
    @Override
    public ByteBuffer evaluateChallengeSync(@Nullable ByteBuffer challenge) {
      throw unsupported();
    }
  }
}
