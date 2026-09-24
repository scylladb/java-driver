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

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

public class ClientRoutesUpdateEventTest {

  @Test
  public void should_be_equal_when_same_fields() {
    ClientRoutesUpdateEvent e1 =
        new ClientRoutesUpdateEvent(
            "UPDATED", Arrays.asList("c1", "c2"), Arrays.asList("h1", "h2"));
    ClientRoutesUpdateEvent e2 =
        new ClientRoutesUpdateEvent(
            "UPDATED", Arrays.asList("c1", "c2"), Arrays.asList("h1", "h2"));

    assertThat(e1).isEqualTo(e2);
    assertThat(e1.hashCode()).isEqualTo(e2.hashCode());
  }

  @Test
  public void should_not_be_equal_when_different_change_type() {
    ClientRoutesUpdateEvent e1 =
        new ClientRoutesUpdateEvent("UPDATED", Collections.emptyList(), Collections.emptyList());
    ClientRoutesUpdateEvent e2 =
        new ClientRoutesUpdateEvent("DELETED", Collections.emptyList(), Collections.emptyList());

    assertThat(e1).isNotEqualTo(e2);
  }

  @Test
  public void should_not_be_equal_when_different_connection_ids() {
    ClientRoutesUpdateEvent e1 =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList("c1"), Collections.emptyList());
    ClientRoutesUpdateEvent e2 =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList("c2"), Collections.emptyList());

    assertThat(e1).isNotEqualTo(e2);
  }

  @Test
  public void should_not_be_equal_when_different_host_ids() {
    ClientRoutesUpdateEvent e1 =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.emptyList(), Collections.singletonList("h1"));
    ClientRoutesUpdateEvent e2 =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.emptyList(), Collections.singletonList("h2"));

    assertThat(e1).isNotEqualTo(e2);
  }

  @Test
  public void should_include_all_fields_in_toString() {
    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent(
            "UPDATED", Collections.singletonList("c1"), Collections.singletonList("h1"));

    String str = event.toString();
    assertThat(str).contains("UPDATED");
    assertThat(str).contains("c1");
    assertThat(str).contains("h1");
  }

  @Test
  public void should_not_be_equal_to_null_or_other_type() {
    ClientRoutesUpdateEvent event =
        new ClientRoutesUpdateEvent("UPDATED", Collections.emptyList(), Collections.emptyList());

    assertThat(event).isNotEqualTo(null);
    assertThat(event).isNotEqualTo("not an event");
  }
}
