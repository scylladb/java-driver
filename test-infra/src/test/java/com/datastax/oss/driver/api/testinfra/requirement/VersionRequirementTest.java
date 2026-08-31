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
package com.datastax.oss.driver.api.testinfra.requirement;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class VersionRequirementTest {
  // backend aliases
  private static BackendType CASSANDRA = BackendType.CASSANDRA;
  private static BackendType SCYLLA = BackendType.SCYLLA;

  // version numbers
  private static Version V_0_0_0 = Version.parse("0.0.0");
  private static Version V_0_1_0 = Version.parse("0.1.0");
  private static Version V_1_0_0 = Version.parse("1.0.0");
  private static Version V_1_0_1 = Version.parse("1.0.1");
  private static Version V_1_1_0 = Version.parse("1.1.0");
  private static Version V_2_0_0 = Version.parse("2.0.0");
  private static Version V_2_0_1 = Version.parse("2.0.1");
  private static Version V_3_0_0 = Version.parse("3.0.0");
  private static Version V_3_1_0 = Version.parse("3.1.0");
  private static Version V_4_0_0 = Version.parse("4.0.0");

  // requirements
  private static VersionRequirement CASSANDRA_ANY = new VersionRequirement(CASSANDRA, "", "", "");
  private static VersionRequirement CASSANDRA_FROM_1_0_0 =
      new VersionRequirement(CASSANDRA, "1.0.0", "", "");
  private static VersionRequirement CASSANDRA_TO_1_0_0 =
      new VersionRequirement(CASSANDRA, "", "1.0.0", "");
  private static VersionRequirement CASSANDRA_FROM_1_0_0_TO_2_0_0 =
      new VersionRequirement(CASSANDRA, "1.0.0", "2.0.0", "");
  private static VersionRequirement CASSANDRA_FROM_1_1_0 =
      new VersionRequirement(CASSANDRA, "1.1.0", "", "");
  private static VersionRequirement CASSANDRA_FROM_3_0_0_TO_3_1_0 =
      new VersionRequirement(CASSANDRA, "3.0.0", "3.1.0", "");
  private static VersionRequirement SCYLLA_ANY = new VersionRequirement(SCYLLA, "", "", "");

  @Test
  public void empty_requirements() {
    List<VersionRequirement> req = Collections.emptyList();

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isTrue();
  }

  @Test
  public void single_requirement_any_version() {
    List<VersionRequirement> anyCassandra = Collections.singletonList(CASSANDRA_ANY);
    List<VersionRequirement> anyScylla = Collections.singletonList(SCYLLA_ANY);

    assertThat(VersionRequirement.meetsAny(anyCassandra, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(anyCassandra, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(anyScylla, SCYLLA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(anyScylla, SCYLLA, V_1_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(anyScylla, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(anyScylla, CASSANDRA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(anyCassandra, SCYLLA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(anyCassandra, SCYLLA, V_1_0_0)).isFalse();
  }

  @Test
  public void single_requirement_min_only() {
    List<VersionRequirement> req = Collections.singletonList(CASSANDRA_FROM_1_0_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_1_0)).isFalse();
  }

  @Test
  public void single_requirement_max_only() {
    List<VersionRequirement> req = Collections.singletonList(CASSANDRA_TO_1_0_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_1_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isFalse();
  }

  @Test
  public void single_requirement_min_and_max() {
    List<VersionRequirement> req = Collections.singletonList(CASSANDRA_FROM_1_0_0_TO_2_0_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_1_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_1)).isFalse();
  }

  @Test
  public void multi_requirement_any_version() {
    List<VersionRequirement> req = ImmutableList.of(CASSANDRA_ANY, SCYLLA_ANY);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isTrue();
  }

  @Test
  public void multi_db_requirement_min_one_any_other() {
    List<VersionRequirement> req = ImmutableList.of(CASSANDRA_FROM_1_0_0, SCYLLA_ANY);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
  }

  @Test
  public void multi_requirement_two_ranges() {
    List<VersionRequirement> req =
        ImmutableList.of(CASSANDRA_FROM_1_0_0_TO_2_0_0, CASSANDRA_FROM_3_0_0_TO_3_1_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_3_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_3_1_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_4_0_0)).isFalse();
  }

  @Test
  public void multi_requirement_overlapping() {
    List<VersionRequirement> req =
        ImmutableList.of(CASSANDRA_FROM_1_0_0_TO_2_0_0, CASSANDRA_FROM_1_1_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, SCYLLA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isFalse();
  }

  @Test
  public void multi_requirement_not_range() {
    List<VersionRequirement> req = ImmutableList.of(CASSANDRA_TO_1_0_0, CASSANDRA_FROM_1_1_0);

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_0_0_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_1_0)).isTrue();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_2_0_0)).isTrue();

    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_0)).isFalse();
    assertThat(VersionRequirement.meetsAny(req, CASSANDRA, V_1_0_1)).isFalse();
  }
}
