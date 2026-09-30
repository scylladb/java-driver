/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.HostConnectionPoolTest.errorResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OverloadedException;
import com.google.common.util.concurrent.MoreExecutors;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConnectionAuthenticationTest {

  @DataProvider(name = "authenticationProtocolVersions")
  public static Object[][] authenticationProtocolVersions() {
    return new Object[][] {{ProtocolVersion.V1}, {ProtocolVersion.V4}};
  }

  @Test(groups = "unit", dataProvider = "authenticationProtocolVersions")
  public void should_preserve_overloaded_error_during_authentication(ProtocolVersion version)
      throws Exception {
    EndPoint endPoint = EndPoints.forAddress("127.0.0.1", 9042);
    TestConnection testConnection = newConnection(endPoint);

    try {
      applyAuthenticationResponse(
          testConnection.connection,
          version,
          errorResponse(ExceptionCode.OVERLOADED, "Too many requests"));
      fail("Expected an OverloadedException");
    } catch (OverloadedException e) {
      assertThat(e.getEndPoint()).isEqualTo(endPoint);
      assertThat(e).hasMessageContaining("Too many requests");
    }
    assertThat(authenticationErrorCount(testConnection.manager)).isEqualTo(0);
  }

  @Test(groups = "unit", dataProvider = "authenticationProtocolVersions")
  public void should_keep_bad_credentials_as_authentication_error(ProtocolVersion version)
      throws Exception {
    EndPoint endPoint = EndPoints.forAddress("127.0.0.1", 9042);
    TestConnection testConnection = newConnection(endPoint);

    try {
      applyAuthenticationResponse(
          testConnection.connection,
          version,
          errorResponse(ExceptionCode.BAD_CREDENTIALS, "Bad credentials"));
      fail("Expected an AuthenticationException");
    } catch (AuthenticationException e) {
      assertThat(e.getEndPoint()).isEqualTo(endPoint);
      assertThat(e).hasMessageContaining("Bad credentials");
    }
    assertThat(authenticationErrorCount(testConnection.manager)).isEqualTo(1);
  }

  @Test(groups = "unit")
  public void should_try_every_initial_contact_point_after_authentication_overload()
      throws Exception {
    Cluster cluster =
        Cluster.builder()
            .addContactPoints("127.0.0.1", "127.0.0.2")
            .withoutMetrics()
            .withDriverConfigReporting(false)
            .build();
    Cluster.Manager manager = cluster.manager;
    manager.metadata = new Metadata(manager);
    for (EndPoint contactPoint : manager.contactPoints) {
      manager.metadata.addContactPoint(contactPoint);
    }

    Connection.Factory factory = mock(Connection.Factory.class);
    manager.connectionFactory = factory;
    doThrow(new OverloadedException(null, "Too many authentication requests"))
        .when(factory)
        .open(any(Host.class), eq(true));

    try {
      new ControlConnection(manager).connect();
      fail("Expected a NoHostAvailableException");
    } catch (NoHostAvailableException e) {
      Map<EndPoint, Throwable> errors = e.getErrors();
      assertThat(errors).hasSize(2);
      for (Throwable error : errors.values()) {
        assertThat(error).isInstanceOf(OverloadedException.class);
      }
    }

    verify(factory, times(2)).open(any(Host.class), eq(true));
  }

  @Test(groups = "unit")
  public void should_ignore_authentication_overload_when_repreparing_on_recovered_host()
      throws Exception {
    Cluster cluster =
        Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withoutMetrics()
            .withDriverConfigReporting(false)
            .build();
    Cluster.Manager manager = cluster.manager;
    manager.preparedQueries = new ConcurrentHashMap<MD5Digest, PreparedStatement>();
    PreparedStatement statement = mock(PreparedStatement.class);
    when(statement.getQueryString()).thenReturn("SELECT * FROM system.local");
    manager.preparedQueries.put(MD5Digest.wrap(new byte[] {1}), statement);

    Host host = mock(Host.class);
    Connection.Factory factory = mock(Connection.Factory.class);
    manager.connectionFactory = factory;
    doThrow(new OverloadedException(null, "Too many authentication requests"))
        .when(factory)
        .open(host);

    assertThat(manager.prepareAllQueries(host, null)).isNull();
    verify(factory).open(host);
  }

  private static TestConnection newConnection(EndPoint endPoint) throws Exception {
    Cluster cluster =
        Cluster.builder()
            .addContactPoint("127.0.0.1")
            .withoutJMXReporting()
            .withDriverConfigReporting(false)
            .build();
    cluster.manager.metrics = new Metrics(cluster.manager);
    Connection.Factory factory = mock(Connection.Factory.class);
    Field managerField = Connection.Factory.class.getDeclaredField("manager");
    managerField.setAccessible(true);
    managerField.set(factory, cluster.manager);
    Field configurationField = Connection.Factory.class.getDeclaredField("configuration");
    configurationField.setAccessible(true);
    configurationField.set(factory, cluster.manager.configuration);
    return new TestConnection(
        new Connection("authentication-test", endPoint, factory), cluster.manager);
  }

  private static long authenticationErrorCount(Cluster.Manager manager) {
    return manager.metrics.getErrorMetrics().getAuthenticationErrors().getCount();
  }

  private static void applyAuthenticationResponse(
      Connection connection, ProtocolVersion version, Responses.Error response) throws Exception {
    if (version == ProtocolVersion.V1) {
      connection.onV1AuthResponse(version, MoreExecutors.directExecutor()).apply(response);
    } else {
      connection
          .onV2AuthResponse(mock(Authenticator.class), version, MoreExecutors.directExecutor())
          .apply(response);
    }
  }

  private static class TestConnection {
    final Connection connection;
    final Cluster.Manager manager;

    TestConnection(Connection connection, Cluster.Manager manager) {
      this.connection = connection;
      this.manager = manager;
    }
  }
}
