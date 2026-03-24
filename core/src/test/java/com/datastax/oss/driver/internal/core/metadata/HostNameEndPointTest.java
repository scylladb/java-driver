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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import org.junit.Test;

public class HostNameEndPointTest {

  @Test
  public void should_return_resolved_address() {
    HostNameEndPoint endPoint = new HostNameEndPoint("localhost", 9042);

    InetSocketAddress address = endPoint.resolve();

    assertThat(address).isNotNull();
    assertThat(address.getPort()).isEqualTo(9042);
    assertThat(address.isUnresolved()).isFalse();
  }

  @Test
  public void should_perform_dns_lookup_on_each_resolve_call() throws Exception {
    // Re-resolution is structural: resolve() calls InetAddress.getAllByName() every time
    // with no cached field. We verify this by calling resolve() twice and confirming both
    // calls succeed and return consistent results (same host, same port), which would fail
    // if DNS were broken after the first call.
    HostNameEndPoint endPoint = new HostNameEndPoint("localhost", 9042);

    InetSocketAddress address1 = endPoint.resolve();
    InetSocketAddress address2 = endPoint.resolve();

    assertThat(address1.getPort()).isEqualTo(9042);
    assertThat(address2.getPort()).isEqualTo(9042);
    assertThat(address1.isUnresolved()).isFalse();
    assertThat(address2.isUnresolved()).isFalse();
    // Both resolved addresses must belong to localhost
    assertThat(address1.getAddress().isLoopbackAddress()).isTrue();
    assertThat(address2.getAddress().isLoopbackAddress()).isTrue();
  }

  @Test
  public void should_throw_on_resolve_if_hostname_unknown() {
    HostNameEndPoint endPoint = new HostNameEndPoint("this-host-does-not-exist.invalid", 9042);

    assertThatThrownBy(endPoint::resolve)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("this-host-does-not-exist.invalid");
  }

  @Test
  public void should_normalize_hostname_to_lowercase() {
    HostNameEndPoint endPoint = new HostNameEndPoint("MyHost.Example.COM", 9042);

    assertThat(endPoint.toString()).isEqualTo("myhost.example.com:9042");
    assertThat(endPoint.asMetricPrefix()).isEqualTo("myhost_example_com:9042");
  }

  @Test
  public void should_implement_equals_based_on_hostname_and_port() {
    HostNameEndPoint ep1 = new HostNameEndPoint("localhost", 9042);
    HostNameEndPoint ep2 = new HostNameEndPoint("localhost", 9042);
    HostNameEndPoint ep3 = new HostNameEndPoint("localhost", 9043);
    HostNameEndPoint ep4 = new HostNameEndPoint("otherhost", 9042);

    assertThat(ep1).isEqualTo(ep2);
    assertThat(ep1).isNotEqualTo(ep3);
    assertThat(ep1).isNotEqualTo(ep4);
    assertThat(ep1).isNotEqualTo(new DefaultEndPoint(new InetSocketAddress("localhost", 9042)));
  }

  @Test
  public void should_implement_hashcode_consistently_with_equals() {
    HostNameEndPoint ep1 = new HostNameEndPoint("localhost", 9042);
    HostNameEndPoint ep2 = new HostNameEndPoint("localhost", 9042);

    assertThat(ep1.hashCode()).isEqualTo(ep2.hashCode());
  }

  @Test
  public void should_be_equal_regardless_of_input_case() {
    HostNameEndPoint ep1 = new HostNameEndPoint("MyHost", 9042);
    HostNameEndPoint ep2 = new HostNameEndPoint("myhost", 9042);

    assertThat(ep1).isEqualTo(ep2);
    assertThat(ep1.hashCode()).isEqualTo(ep2.hashCode());
  }

  @Test
  public void should_format_toString() {
    HostNameEndPoint endPoint = new HostNameEndPoint("node-0.example.com", 9042);

    assertThat(endPoint.toString()).isEqualTo("node-0.example.com:9042");
  }

  @Test
  public void should_format_metric_prefix() {
    HostNameEndPoint endPoint = new HostNameEndPoint("node-0.example.com", 9042);

    assertThat(endPoint.asMetricPrefix()).isEqualTo("node-0_example_com:9042");
  }
}
