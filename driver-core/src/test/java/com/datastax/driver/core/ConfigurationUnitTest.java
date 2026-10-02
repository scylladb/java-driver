/*
 * Copyright ScyllaDB, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.policies.Policies;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class ConfigurationUnitTest {

  private static final String SSL_ERROR =
      "the configured SSLOptions must implement ExtendedRemoteEndpointAwareSslOptions";
  private static final String AUTH_ERROR =
      "the configured AuthProvider must implement ExtendedAuthProvider";

  @Test(groups = "unit")
  public void should_reject_non_extended_ssl_options_with_sni() {
    assertRejected(
        configuration(true, mock(SSLOptions.class), AuthProvider.NONE),
        "Configuration error: if SNI endpoints are in use, " + SSL_ERROR);
  }

  @Test(groups = "unit")
  public void should_reject_non_extended_auth_provider_with_sni() {
    assertRejected(
        configuration(true, null, mock(AuthProvider.class)),
        "Configuration error: if SNI endpoints are in use, " + AUTH_ERROR);
  }

  @Test(groups = "unit")
  public void should_list_both_errors_with_sni() {
    assertRejected(
        configuration(true, mock(SSLOptions.class), mock(AuthProvider.class)),
        "Configuration error: if SNI endpoints are in use, " + SSL_ERROR + "," + AUTH_ERROR);
  }

  @Test(groups = "unit")
  public void should_accept_extended_options_with_sni() {
    configuration(
            true,
            mock(ExtendedRemoteEndpointAwareSslOptions.class),
            new PlainTextAuthProvider("user", "pass"))
        .register(mock(Cluster.Manager.class));
  }

  @Test(groups = "unit")
  public void should_accept_missing_ssl_and_auth_with_sni() {
    configuration(true, null, null).register(mock(Cluster.Manager.class));
  }

  @Test(groups = "unit")
  public void should_not_check_options_without_sni() {
    configuration(false, mock(SSLOptions.class), mock(AuthProvider.class))
        .register(mock(Cluster.Manager.class));
  }

  @Test(groups = "unit")
  public void should_register_options_and_init_end_point_factory() {
    EndPointFactory endPointFactory = mock(EndPointFactory.class);
    QueryOptions queryOptions = mock(QueryOptions.class);
    Cluster cluster = mock(Cluster.class);
    Cluster.Manager manager = mock(Cluster.Manager.class);
    when(manager.getCluster()).thenReturn(cluster);

    Configuration.builder()
        .withPolicies(Policies.builder().withEndPointFactory(endPointFactory).build())
        .withQueryOptions(queryOptions)
        .build()
        .register(manager);

    verify(queryOptions).register(manager);
    verify(endPointFactory).init(cluster);
  }

  @Test(groups = "unit")
  public void should_drop_default_keyspace_when_cluster_rebuilds_configuration() {
    final Configuration configuration = Configuration.builder().withDefaultKeyspace("ks").build();
    assertThat(configuration.getDefaultKeyspace()).isEqualTo("ks");

    Cluster cluster =
        Cluster.buildFrom(
            new Cluster.Initializer() {
              @Override
              public String getClusterName() {
                return null;
              }

              @Override
              public List<EndPoint> getContactPoints() {
                return Collections.<EndPoint>singletonList(
                    new TranslatedAddressEndPoint(new InetSocketAddress("127.0.0.1", 9042)));
              }

              @Override
              public Configuration getConfiguration() {
                return configuration;
              }

              @Override
              public Collection<Host.StateListener> getInitialListeners() {
                return Collections.emptySet();
              }
            });
    try {
      // Pins current behaviour (#1179): Cluster.Manager rebuilds Configuration without
      // defaultKeyspace.
      assertThat(cluster.getConfiguration().getDefaultKeyspace()).isNull();
    } finally {
      cluster.close();
    }
  }

  private static Configuration configuration(
      boolean sni, SSLOptions sslOptions, AuthProvider authProvider) {
    Policies.Builder policies = Policies.builder();
    if (sni) {
      policies.withEndPointFactory(
          new SniEndPointFactory(new InetSocketAddress("127.0.0.1", 9042)));
    }
    return Configuration.builder()
        .withPolicies(policies.build())
        .withProtocolOptions(
            new ProtocolOptions(
                ProtocolOptions.DEFAULT_PORT,
                null,
                ProtocolOptions.DEFAULT_MAX_SCHEMA_AGREEMENT_WAIT_SECONDS,
                sslOptions,
                authProvider))
        .build();
  }

  private static void assertRejected(Configuration configuration, String expectedMessage) {
    try {
      configuration.register(mock(Cluster.Manager.class));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo(expectedMessage);
    }
  }
}
