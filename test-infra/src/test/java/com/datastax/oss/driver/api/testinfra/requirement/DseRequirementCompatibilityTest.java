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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

@SuppressWarnings("deprecation")
public class DseRequirementCompatibilityTest {
  @Test
  public void should_preserve_dse_requirement_metadata() {
    VersionRequirement requirement =
        VersionRequirement.fromDseRequirement(DseOnly.class.getAnnotation(DseRequirement.class));
    assertThat(requirement.getBackendType()).isEqualTo(BackendType.DSE);
    assertThat(requirement.getMinInclusive()).contains(Version.parse("5.0"));
    assertThat(requirement.getMaxExclusive()).contains(Version.parse("7.0"));
    assertThat(requirement.readableString()).contains("legacy requirement");
  }

  @Test
  public void should_skip_class_requirement_on_supported_backend() throws Exception {
    assertSkipped(Description.createSuiteDescription(DseOnly.class));
  }

  @Test
  public void should_skip_method_requirement_on_supported_backend() throws Exception {
    assertSkipped(methodDescription(DseOnly.class));
  }

  @Test
  public void should_allow_matching_class_requirement() throws Throwable {
    assertAllowed(Description.createSuiteDescription(MixedRequirements.class));
  }

  @Test
  public void should_allow_matching_method_requirement() throws Throwable {
    assertAllowed(methodDescription(MixedRequirements.class));
  }

  @Test
  public void should_keep_mixed_requirement_version_limits() {
    Collection<VersionRequirement> requirements =
        VersionRequirement.fromAnnotations(
            Description.createSuiteDescription(VersionLimited.class));
    assertThat(
            VersionRequirement.meetsAny(requirements, BackendType.CASSANDRA, Version.parse("4.0")))
        .isTrue();
    assertThat(
            VersionRequirement.meetsAny(requirements, BackendType.CASSANDRA, Version.parse("3.0")))
        .isFalse();
    assertThat(
            VersionRequirement.meetsAny(requirements, BackendType.CASSANDRA, Version.parse("5.0")))
        .isFalse();
  }

  private static Description methodDescription(Class<?> fixture) throws Exception {
    return Description.createTestDescription(
        fixture, "test", fixture.getMethod("test").getAnnotations());
  }

  private static void assertSkipped(Description description) {
    Collection<VersionRequirement> requirements = VersionRequirement.fromAnnotations(description);
    for (BackendType backend :
        new BackendType[] {BackendType.CASSANDRA, BackendType.SCYLLA, BackendType.HCD}) {
      assertThat(VersionRequirement.meetsAny(requirements, backend, Version.parse("6.0")))
          .isFalse();
    }
    AtomicBoolean executed = new AtomicBoolean();
    Statement statement =
        new BackendRequirementRule().apply(recordExecution(executed), description);
    assertThatThrownBy(statement::evaluate).isInstanceOf(AssumptionViolatedException.class);
    assertThat(executed).isFalse();
  }

  private static void assertAllowed(Description description) throws Throwable {
    Collection<VersionRequirement> requirements = VersionRequirement.fromAnnotations(description);
    for (BackendType backend :
        new BackendType[] {BackendType.CASSANDRA, BackendType.SCYLLA, BackendType.HCD}) {
      assertThat(VersionRequirement.meetsAny(requirements, backend, Version.parse("6.0"))).isTrue();
    }
    AtomicBoolean executed = new AtomicBoolean();
    new BackendRequirementRule().apply(recordExecution(executed), description).evaluate();
    assertThat(executed).isTrue();
  }

  private static Statement recordExecution(AtomicBoolean executed) {
    return new Statement() {
      @Override
      public void evaluate() {
        executed.set(true);
      }
    };
  }

  @DseRequirement(min = "5.0", max = "7.0", description = "legacy requirement")
  public static class DseOnly {
    @DseRequirement
    public void test() {}
  }

  @DseRequirement
  @BackendRequirement(type = BackendType.CASSANDRA)
  @BackendRequirement(type = BackendType.SCYLLA)
  @BackendRequirement(type = BackendType.HCD)
  public static class MixedRequirements {
    @DseRequirement
    @BackendRequirement(type = BackendType.CASSANDRA)
    @BackendRequirement(type = BackendType.SCYLLA)
    @BackendRequirement(type = BackendType.HCD)
    public void test() {}
  }

  @DseRequirement
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0", maxExclusive = "5.0")
  public static class VersionLimited {}
}
