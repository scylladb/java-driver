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

/** @deprecated DSE-specific configuration is no longer supported. */
@Deprecated
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

  /** @deprecated DSE proxy authentication is no longer supported. */
  @Deprecated
  AUTH_PROVIDER_AUTHORIZATION_ID("advanced.auth-provider.authorization-id"),
  /** @deprecated DSE GSSAPI authentication is no longer supported. */
  @Deprecated
  AUTH_PROVIDER_SERVICE("advanced.auth-provider.service"),
  /** @deprecated DSE GSSAPI authentication is no longer supported. */
  @Deprecated
  AUTH_PROVIDER_LOGIN_CONFIGURATION("advanced.auth-provider.login-configuration"),
  /** @deprecated DSE GSSAPI authentication is no longer supported. */
  @Deprecated
  AUTH_PROVIDER_SASL_PROPERTIES("advanced.auth-provider.sasl-properties"),

  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_PAGE_SIZE("advanced.continuous-paging.page-size"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_PAGE_SIZE_BYTES("advanced.continuous-paging.page-size-in-bytes"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_MAX_PAGES("advanced.continuous-paging.max-pages"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND("advanced.continuous-paging.max-pages-per-second"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES("advanced.continuous-paging.max-enqueued-pages"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE("advanced.continuous-paging.timeout.first-page"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES("advanced.continuous-paging.timeout.other-pages"),

  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST(
      "advanced.metrics.session.continuous-cql-requests.highest-latency"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS(
      "advanced.metrics.session.continuous-cql-requests.significant-digits"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL(
      "advanced.metrics.session.continuous-cql-requests.refresh-interval"),

  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_READ_CONSISTENCY_LEVEL("basic.graph.read-consistency-level"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_WRITE_CONSISTENCY_LEVEL("basic.graph.write-consistency-level"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_TRAVERSAL_SOURCE("basic.graph.traversal-source"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_SUB_PROTOCOL("advanced.graph.sub-protocol"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_IS_SYSTEM_QUERY("basic.graph.is-system-query"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_NAME("basic.graph.name"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_TIMEOUT("basic.graph.timeout"),

  /**
   * Legacy option that controlled DataStax Insights monitoring.
   *
   * <p>This option is retained for source and binary compatibility. Insights monitoring has been
   * removed, so the value has no effect.
   *
   * <p>Value type: boolean
   *
   * @deprecated DataStax Insights monitoring is no longer supported; there is no replacement.
   */
  @Deprecated
  MONITOR_REPORTING_ENABLED("advanced.monitor-reporting.enabled"),

  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_PAGING_ENABLED("advanced.graph.paging-enabled"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_CONTINUOUS_PAGING_PAGE_SIZE("advanced.graph.paging-options.page-size"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_CONTINUOUS_PAGING_MAX_PAGES("advanced.graph.paging-options.max-pages"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND(
      "advanced.graph.paging-options.max-pages-per-second"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES("advanced.graph.paging-options.max-enqueued-pages"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_SESSION_GRAPH_REQUESTS_HIGHEST("advanced.metrics.session.graph-requests.highest-latency"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_SESSION_GRAPH_REQUESTS_DIGITS(
      "advanced.metrics.session.graph-requests.significant-digits"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_SESSION_GRAPH_REQUESTS_INTERVAL(
      "advanced.metrics.session.graph-requests.refresh-interval"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_NODE_GRAPH_MESSAGES_HIGHEST("advanced.metrics.node.graph-messages.highest-latency"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_NODE_GRAPH_MESSAGES_DIGITS("advanced.metrics.node.graph-messages.significant-digits"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_NODE_GRAPH_MESSAGES_INTERVAL("advanced.metrics.node.graph-messages.refresh-interval"),

  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST(
      "advanced.metrics.session.continuous-cql-requests.lowest-latency"),
  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_SLO(
      "advanced.metrics.session.continuous-cql-requests.slo"),

  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_SESSION_GRAPH_REQUESTS_LOWEST("advanced.metrics.session.graph-requests.lowest-latency"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_SESSION_GRAPH_REQUESTS_SLO("advanced.metrics.session.graph-requests.slo"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_NODE_GRAPH_MESSAGES_LOWEST("advanced.metrics.node.graph-messages.lowest-latency"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_NODE_GRAPH_MESSAGES_SLO("advanced.metrics.node.graph-messages.slo"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_SESSION_GRAPH_REQUESTS_PUBLISH_PERCENTILES(
      "advanced.metrics.session.graph-requests.publish-percentiles"),
  /**
   * This option is ignored because DSE Graph support has been removed.
   *
   * @deprecated DSE Graph is no longer supported.
   */
  @Deprecated
  METRICS_NODE_GRAPH_MESSAGES_PUBLISH_PERCENTILES(
      "advanced.metrics.node.graph-messages.publish-percentiles"),

  /** @deprecated Continuous paging is no longer supported; this option is ignored. */
  @Deprecated
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
