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
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.auth;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class PlainTextAuthProviderIT {

  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final Version SCYLLA_CUSTOM_SUPERUSER_VERSION = Version.parse("2026.2.0");
  private static final String SCYLLA_SALTED_PASSWORD =
      "$6$java-driver-test$2JXL/pXZTuamSsYNH/kE9UUSQETMmKIcdP8Xgdu/fiHsTHSN.EhjmLrMHPVuQ3.4w0h8CDsP/f1trG0kujylG0";

  @ClassRule public static final CustomCcmRule CCM_RULE = getCCMRule();

  private static CustomCcmRule getCCMRule() {
    CustomCcmRule.Builder builder =
        CustomCcmRule.builder()
            .withCassandraConfiguration("authenticator", "PasswordAuthenticator");
    if (requiresExplicitScyllaSuperuser()) {
      builder =
          builder
              .withCassandraConfiguration("auth_superuser_name", USERNAME)
              .withCassandraConfiguration("auth_superuser_salted_password", SCYLLA_SALTED_PASSWORD);
    } else if (!CcmBridge.isDistributionOf(BackendType.SCYLLA)) {
      builder = builder.withJvmArgs("-Dcassandra.superuser_setup_delay_ms=0");
    }
    return builder.build();
  }

  private static boolean requiresExplicitScyllaSuperuser() {
    return CcmBridge.isDistributionOf(BackendType.SCYLLA)
        && CcmBridge.getDistributionVersion().compareTo(SCYLLA_CUSTOM_SUPERUSER_VERSION) >= 0;
  }

  @BeforeClass
  public static void sleepForAuth() {
    if (CCM_RULE.getCassandraVersion().compareTo(Version.V2_2_0) < 0) {
      // Sleep for 1 second to allow C* auth to do its work.  This is only needed for 2.1
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

  @Test
  public void should_connect_with_credentials() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
            .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, USERNAME)
            .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, PASSWORD)
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {
      session.execute("select * from system.local where key='local'");
    }
  }

  @Test
  public void should_connect_with_programmatic_credentials() {

    SessionBuilder<?, ?> builder =
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withAuthCredentials(USERNAME, PASSWORD);

    try (CqlSession session = (CqlSession) builder.build()) {
      session.execute("select * from system.local where key='local'");
    }
  }

  @Test
  public void should_connect_with_programmatic_provider() {

    AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider(USERNAME, PASSWORD);
    SessionBuilder<?, ?> builder =
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            // Open more than one connection in order to validate that the provider is creating
            // valid Credentials for every invocation of PlainTextAuthProviderBase.getCredentials.
            .withConfigLoader(
                SessionUtils.configLoaderBuilder()
                    .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 4)
                    .build())
            .withAuthProvider(authProvider);

    try (CqlSession session = (CqlSession) builder.build()) {
      session.execute("select * from system.local where key='local'");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_with_invalid_credentials() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
            .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, "baduser")
            .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, "badpass")
            .build();
    try (CqlSession session = SessionUtils.newSession(CCM_RULE, loader)) {
      session.execute("select * from system.local where key='local'");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_with_invalid_programmatic_credentials() {
    SessionBuilder<?, ?> builder =
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withAuthCredentials("baduser", "badpass");

    try (CqlSession session = (CqlSession) builder.build()) {
      session.execute("select * from system.local where key='local'");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_with_invalid_programmatic_provider() {

    AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider("baduser", "badpass");
    SessionBuilder<?, ?> builder =
        SessionUtils.baseBuilder()
            .addContactEndPoints(CCM_RULE.getContactPoints())
            .withAuthProvider(authProvider);

    try (CqlSession session = (CqlSession) builder.build()) {
      session.execute("select * from system.local where key='local'");
    }
  }

  @Test(expected = AllNodesFailedException.class)
  public void should_not_connect_without_credentials() {
    try (CqlSession session = SessionUtils.newSession(CCM_RULE)) {
      session.execute("select * from system.local where key='local'");
    }
  }
}
