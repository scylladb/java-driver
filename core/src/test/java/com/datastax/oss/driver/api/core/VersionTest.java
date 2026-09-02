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
package com.datastax.oss.driver.api.core;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.internal.SerializationHelper;
import java.util.Base64;
import org.junit.Test;

public class VersionTest {

  // Serialized with java-driver-core 4.19.2.1. Keep these fixtures immutable so this test detects
  // accidental changes to Version's serialized field names.
  private static final String SERIALIZED_WITHOUT_REVISION =
      "rO0ABXNyAChjb20uZGF0YXN0YXgub3NzLmRyaXZlci5hcGkuY29yZS5WZXJzaW9uAAAAAAAAAAECAAZJ"
          + "AAhkc2VQYXRjaEkABW1ham9ySQAFbWlub3JJAAVwYXRjaEwABWJ1aWxkdAASTGphdmEvbGFuZy9TdHJp"
          + "bmc7WwALcHJlUmVsZWFzZXN0ABNbTGphdmEvbGFuZy9TdHJpbmc7eHD/////AAAABAAAABMAAAACcHA=";
  private static final String SERIALIZED_WITH_REVISION =
      "rO0ABXNyAChjb20uZGF0YXN0YXgub3NzLmRyaXZlci5hcGkuY29yZS5WZXJzaW9uAAAAAAAAAAECAAZJ"
          + "AAhkc2VQYXRjaEkABW1ham9ySQAFbWlub3JJAAVwYXRjaEwABWJ1aWxkdAASTGphdmEvbGFuZy9TdHJp"
          + "bmc7WwALcHJlUmVsZWFzZXN0ABNbTGphdmEvbGFuZy9TdHJpbmc7eHAAAAABAAAABAAAABMAAAACcHA=";

  @Test
  public void should_parse_release_version() {
    assertThat(Version.parse("1.2.19"))
        .hasMajorMinorPatch(1, 2, 19)
        .hasRevision(-1)
        .hasNoPreReleaseLabels()
        .hasBuildLabel(null)
        .hasNextStable("1.2.19")
        .hasToString("1.2.19");
  }

  @Test
  public void should_parse_release_without_patch() {
    assertThat(Version.parse("1.2")).hasMajorMinorPatch(1, 2, 0);
  }

  @Test
  public void should_parse_pre_release_version() {
    assertThat(Version.parse("1.2.0-beta1-SNAPSHOT"))
        .hasMajorMinorPatch(1, 2, 0)
        .hasRevision(-1)
        .hasPreReleaseLabels("beta1", "SNAPSHOT")
        .hasBuildLabel(null)
        .hasToString("1.2.0-beta1-SNAPSHOT")
        .hasNextStable("1.2.0");
  }

  @Test
  public void should_allow_tilde_as_first_pre_release_delimiter() {
    assertThat(Version.parse("1.2.0~beta1-SNAPSHOT"))
        .hasMajorMinorPatch(1, 2, 0)
        .hasRevision(-1)
        .hasPreReleaseLabels("beta1", "SNAPSHOT")
        .hasBuildLabel(null)
        .hasToString("1.2.0-beta1-SNAPSHOT")
        .hasNextStable("1.2.0");
  }

  @Test
  public void should_parse_revision() {
    assertThat(Version.parse("4.19.2.2-SNAPSHOT"))
        .hasMajorMinorPatch(4, 19, 2)
        .hasRevision(2)
        .hasToString("4.19.2.2-SNAPSHOT")
        .hasNextStable("4.19.2.2");
  }

  @Test
  public void should_deserialize_versions_from_4_19_2_1() {
    assertThat(deserializeFixture(SERIALIZED_WITHOUT_REVISION))
        .hasMajorMinorPatch(4, 19, 2)
        .hasRevision(-1)
        .hasToString("4.19.2");
    assertThat(deserializeFixture(SERIALIZED_WITH_REVISION))
        .hasMajorMinorPatch(4, 19, 2)
        .hasRevision(1)
        .hasToString("4.19.2.1");
  }

  @Test
  public void should_parse_scylla_release_candidates() {
    assertThat(Version.parse("4.3.rc5"))
        .hasMajorMinorPatch(4, 3, 0)
        .hasToString("4.3.0-rc5")
        .hasPreReleaseLabels("rc5");
  }

  @Test
  public void should_order_versions() {
    // by component
    assertOrder("1.2.0", "2.0.0", -1);
    assertOrder("2.0.0", "2.1.0", -1);
    assertOrder("2.0.1", "2.0.2", -1);
    assertOrder("2.0.1.1", "2.0.1.2", -1);

    // shortened vs. longer version
    assertOrder("2.0", "2.0.0", 0);
    assertOrder("2.0", "2.0.1", -1);

    // absent revision vs. present revision
    assertOrder("2.0.0", "2.0.0.0", -1);
    assertOrder("2.0.0", "2.0.0.1", -1);

    // pre-release vs. release
    assertOrder("2.0.0-beta1", "2.0.0", -1);
    assertOrder("2.0.0-SNAPSHOT", "2.0.0", -1);
    assertOrder("2.0.0-beta1-SNAPSHOT", "2.0.0", -1);

    // pre-release vs. pre-release
    assertOrder("2.0.0-a-b-c", "2.0.0-a-b-d", -1);
    assertOrder("2.0.0-a-b-c", "2.0.0-a-b-c-d", -1);

    // build number ignored
    assertOrder("2.0.0+build01", "2.0.0+build02", 0);
  }

  private void assertOrder(String version1, String version2, int expected) {
    assertThat(Version.parse(version1).compareTo(Version.parse(version2))).isEqualTo(expected);
  }

  private Version deserializeFixture(String fixture) {
    return SerializationHelper.deserialize(Base64.getDecoder().decode(fixture));
  }
}
