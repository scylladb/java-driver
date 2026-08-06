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
package com.datastax.oss.driver.internal.core.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CloudTopologyMonitorTest {

  @Mock private InternalDriverContext context;
  @Mock private ControlConnection controlConnection;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;

  @Before
  public void setup() {
    when(context.getSessionName()).thenReturn("test");
    when(context.getControlConnection()).thenReturn(controlConnection);
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));
    when(defaultProfile.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT)).thenReturn(false);
  }

  @Test
  public void should_keep_endpoint_identity_stable_when_the_proxy_address_caches_a_name()
      throws Exception {
    // The drift itself, observed end to end -- the companion to the test above, which pins the
    // normalization without depending on the environment.
    //
    // SniEndPoint promises a stable equals/hashCode/asMetricPrefix and derives all three from the
    // proxy address's host string. When the InetSocketAddress was built from an InetAddress that
    // carries no hostName -- getByAddress(bytes), as below -- getHostString() renders that
    // address's lazily-populated field, and anything calling getHostName() fills it in.
    // DefaultSslEngineFactory does exactly that under the default allow-dns-reverse-lookup-san.
    // (new InetSocketAddress("host", port) is *not* affected: getByName populates hostName
    // eagerly. That distinction is why this test constructs the address the way it does.)
    //
    // This monitor rebuilds every node's endpoint on every topology refresh, so re-deriving the
    // string each time would move the whole cluster's per-node metrics at once, mid-session, and
    // make endpoints built before and after compare unequal.
    InetSocketAddress proxy =
        new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 9042);
    // Environment-dependent, unavoidably. The guard is observable *only* through this mutation:
    // SniEndPoint#storeUnresolved normalizes a resolved proxy to the same string the constructor
    // would have snapshotted, so any two endpoints built at the same instant match with or without
    // it. What the snapshot buys is that they still match across a mutation of the caller's
    // instance -- and provoking one needs a real reverse lookup, which needs 127.0.0.1 to have a
    // mapping. Where it does not, this skips; a skipped run in the surefire report is the signal
    // that this guard went unverified on that host, not that it passed.
    assumeThat(proxy.getHostString())
        .as("requires a host where 127.0.0.1 starts out rendering as the literal")
        .isEqualTo("127.0.0.1");

    CloudTopologyMonitor monitor = new CloudTopologyMonitor(context, proxy);
    UUID hostId = UUID.randomUUID();
    AdminRow row = Mockito.mock(AdminRow.class);
    when(row.getUuid("host_id")).thenReturn(hostId);
    EndPoint localEndPoint = Mockito.mock(EndPoint.class);

    EndPoint before = monitor.buildNodeEndPoint(row, null, localEndPoint);

    // What the SSL engine factory does to the caller's instance, behind the driver's back.
    proxy.getHostName();
    assumeThat(proxy.getHostString())
        .as("requires a host where 127.0.0.1 reverse-resolves to a name")
        .isNotEqualTo("127.0.0.1");

    EndPoint after = monitor.buildNodeEndPoint(row, null, localEndPoint);

    assertThat(after).isEqualTo(before);
    assertThat(after.hashCode()).isEqualTo(before.hashCode());
    assertThat(after.asMetricPrefix()).isEqualTo(before.asMetricPrefix());
  }
}
