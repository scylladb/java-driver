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
package com.datastax.oss.driver.internal.core.clientroutes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.Test;

public class ClientRouteRecordTest {

  private static final UUID HOST_ID = UUID.randomUUID();

  @Test
  public void should_expose_its_components() {
    ClientRouteRecord record = new ClientRouteRecord(HOST_ID, "proxy.example.com", 9042);

    assertThat(record.getHostId()).isEqualTo(HOST_ID);
    assertThat(record.getHostname()).isEqualTo("proxy.example.com");
    assertThat(record.getPort()).isEqualTo(9042);
  }

  @Test
  public void should_reject_null_components() {
    assertThatThrownBy(() -> new ClientRouteRecord(null, "proxy.example.com", 9042))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("hostId must not be null");
    assertThatThrownBy(() -> new ClientRouteRecord(HOST_ID, null, 9042))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("hostname must not be null");
  }

  @Test
  public void should_reject_empty_hostname() {
    assertThatThrownBy(() -> new ClientRouteRecord(HOST_ID, "", 9042))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("hostname must not be empty");
  }

  @Test
  public void should_reject_port_outside_the_valid_range() {
    assertThatThrownBy(() -> new ClientRouteRecord(HOST_ID, "proxy.example.com", 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("port must be between 1 and 65535, got: 0");
    assertThatThrownBy(() -> new ClientRouteRecord(HOST_ID, "proxy.example.com", -1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("port must be between 1 and 65535");
    assertThatThrownBy(() -> new ClientRouteRecord(HOST_ID, "proxy.example.com", 65536))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("port must be between 1 and 65535");
  }

  @Test
  public void should_accept_ports_at_the_boundaries() {
    assertThat(new ClientRouteRecord(HOST_ID, "proxy.example.com", 1).getPort()).isEqualTo(1);
    assertThat(new ClientRouteRecord(HOST_ID, "proxy.example.com", 65535).getPort())
        .isEqualTo(65535);
  }

  @Test
  public void should_compare_on_all_three_components() {
    ClientRouteRecord record = new ClientRouteRecord(HOST_ID, "proxy.example.com", 9042);
    UUID otherHostId = UUID.randomUUID();

    assertThat(record).isEqualTo(new ClientRouteRecord(HOST_ID, "proxy.example.com", 9042));
    assertThat(record).isEqualTo(record);
    assertThat(record)
        .isNotEqualTo(new ClientRouteRecord(otherHostId, "proxy.example.com", 9042))
        .isNotEqualTo(new ClientRouteRecord(HOST_ID, "other.example.com", 9042))
        .isNotEqualTo(new ClientRouteRecord(HOST_ID, "proxy.example.com", 9043))
        .isNotEqualTo(null)
        .isNotEqualTo("not a record");
  }

  @Test
  public void should_hash_consistently_with_equals() {
    ClientRouteRecord record = new ClientRouteRecord(HOST_ID, "proxy.example.com", 9042);

    assertThat(record.hashCode())
        .isEqualTo(new ClientRouteRecord(HOST_ID, "proxy.example.com", 9042).hashCode());
  }

  @Test
  public void should_print_all_three_components() {
    ClientRouteRecord record = new ClientRouteRecord(HOST_ID, "proxy.example.com", 9042);

    assertThat(record.toString())
        .isEqualTo(
            "ClientRouteRecord{hostId=" + HOST_ID + ", hostname='proxy.example.com', port=9042}");
  }
}
