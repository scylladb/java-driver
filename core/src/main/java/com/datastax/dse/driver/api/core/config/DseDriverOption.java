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
package com.datastax.dse.driver.api.core.config;

import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;

public enum DseDriverOption implements DriverOption {
  /**
   * The name of the application using the session.
   *
   * <p>Value type: {@link String}
   */
  APPLICATION_NAME("basic.application.name"),
  /**
   * The version of the application using the session.
   *
   * <p>Value type: {@link String}
   */
  APPLICATION_VERSION("basic.application.version"),

  /**
   * Proxy authentication for GSSAPI authentication: allows to login as another user or role.
   *
   * <p>Value type: {@link String}
   */
  AUTH_PROVIDER_AUTHORIZATION_ID("advanced.auth-provider.authorization-id"),
  /**
   * Service name for GSSAPI authentication.
   *
   * <p>Value type: {@link String}
   */
  AUTH_PROVIDER_SERVICE("advanced.auth-provider.service"),
  /**
   * Login configuration for GSSAPI authentication.
   *
   * <p>Value type: {@link java.util.Map Map}&#60;{@link String},{@link String}&#62;
   */
  AUTH_PROVIDER_LOGIN_CONFIGURATION("advanced.auth-provider.login-configuration"),
  /**
   * Internal SASL properties, if any, such as QOP, for GSSAPI authentication.
   *
   * <p>Value type: {@link java.util.Map Map}&#60;{@link String},{@link String}&#62;
   */
  AUTH_PROVIDER_SASL_PROPERTIES("advanced.auth-provider.sasl-properties"),

  /**
   * The page size for continuous paging.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_PAGE_SIZE("advanced.continuous-paging.page-size"),
  /**
   * Whether {@link #CONTINUOUS_PAGING_PAGE_SIZE} should be interpreted in number of rows or bytes.
   *
   * <p>Value type: boolean
   */
  CONTINUOUS_PAGING_PAGE_SIZE_BYTES("advanced.continuous-paging.page-size-in-bytes"),
  /**
   * The maximum number of continuous pages to return.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_MAX_PAGES("advanced.continuous-paging.max-pages"),
  /**
   * The maximum number of continuous pages per second.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND("advanced.continuous-paging.max-pages-per-second"),
  /**
   * The maximum number of continuous pages that can be stored in the local queue.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES("advanced.continuous-paging.max-enqueued-pages"),
  /**
   * How long to wait for the coordinator to send the first continuous page.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE("advanced.continuous-paging.timeout.first-page"),
  /**
   * How long to wait for the coordinator to send subsequent continuous pages.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES("advanced.continuous-paging.timeout.other-pages"),

  /**
   * The largest latency that we expect to record for continuous requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST(
      "advanced.metrics.session.continuous-cql-requests.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * continuous requests.
   *
   * <p>Value-type: int
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS(
      "advanced.metrics.session.continuous-cql-requests.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for continuous requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL(
      "advanced.metrics.session.continuous-cql-requests.refresh-interval"),

  /**
   * Whether to send events for Insights monitoring.
   *
   * <p>Value type: boolean
   */
  MONITOR_REPORTING_ENABLED("advanced.monitor-reporting.enabled"),

  /**
   * The shortest latency that we expect to record for continuous requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST(
      "advanced.metrics.session.continuous-cql-requests.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_SLO(
      "advanced.metrics.session.continuous-cql-requests.slo"),

  /**
   * Optional list of percentiles to publish for continuous paging requests metric. Produces an
   * additional time series for each requested percentile. This percentile is computed locally, and
   * so can't be aggregated with percentiles computed across other dimensions (e.g. in a different
   * instance).
   *
   * <p>Value type: {@link java.util.List List}&#60;{@link Double}&#62;
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_PUBLISH_PERCENTILES(
      "advanced.metrics.session.continuous-cql-requests.publish-percentiles"),
  ;

  private final String path;

  DseDriverOption(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }
}
