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
package com.datastax.oss.driver.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/** Shared assertions for the driver-config-reporting integration tests. */
class DriverConfigReportingAssertions {

  // FAIL_ON_TRAILING_TOKENS rejects a valid JSON value followed by garbage; the payload is read
  // below via readValue(..), which honors this feature reliably (readTree historically does not).
  private static final ObjectMapper OBJECT_MAPPER =
      JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();

  private DriverConfigReportingAssertions() {}

  /**
   * Asserts that a {@code DRIVER_CONFIG} value is a well-formed stage-2 report: valid JSON whose
   * {@code version} is the integer {@code 1} and that carries the full configuration payload
   * (checked here via the always-present, backend-agnostic {@code query.load-balancing.policy}
   * group). Guards against an incorrect schema version, a malformed blob, or an empty/stage-1-only
   * payload slipping through a mere key-presence check.
   *
   * @return the parsed report, so callers can assert on backend-specific groups too.
   */
  static JsonNode assertDriverConfigPayload(String driverConfig) {
    JsonNode root;
    try {
      root = OBJECT_MAPPER.readValue(driverConfig, JsonNode.class);
    } catch (JsonProcessingException e) {
      throw new AssertionError("DRIVER_CONFIG is not valid JSON: " + driverConfig, e);
    }
    assertThat(root.path("version").isInt())
        .as("version is an integer in %s", driverConfig)
        .isTrue();
    assertThat(root.path("version").intValue()).isEqualTo(1);
    JsonNode loadBalancingPolicy = root.path("query").path("load-balancing").path("policy");
    assertThat(loadBalancingPolicy.isObject())
        .as("query.load-balancing.policy is an object in %s", driverConfig)
        .isTrue();
    assertThat(loadBalancingPolicy.path("type").isTextual())
        .as("query.load-balancing.policy.type is present in %s", driverConfig)
        .isTrue();
    return root;
  }
}
