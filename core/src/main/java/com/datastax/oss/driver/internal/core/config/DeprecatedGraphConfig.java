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
package com.datastax.oss.driver.internal.core.config;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.TreeSet;

/** Compatibility support for configuration left behind after DSE Graph removal. */
public final class DeprecatedGraphConfig {

  private static final String GRAPH_REQUESTS = "graph-requests";
  private static final String GRAPH_CLIENT_TIMEOUTS = "graph-client-timeouts";
  private static final String GRAPH_MESSAGES = "graph-messages";

  @SuppressWarnings("deprecation")
  private static final Set<DseDriverOption> OPTIONS =
      Collections.unmodifiableSet(
          EnumSet.of(
              DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL,
              DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL,
              DseDriverOption.GRAPH_TRAVERSAL_SOURCE,
              DseDriverOption.GRAPH_SUB_PROTOCOL,
              DseDriverOption.GRAPH_IS_SYSTEM_QUERY,
              DseDriverOption.GRAPH_NAME,
              DseDriverOption.GRAPH_TIMEOUT,
              DseDriverOption.GRAPH_PAGING_ENABLED,
              DseDriverOption.GRAPH_CONTINUOUS_PAGING_PAGE_SIZE,
              DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES,
              DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND,
              DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES,
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_HIGHEST,
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_LOWEST,
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_SLO,
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_PUBLISH_PERCENTILES,
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_DIGITS,
              DseDriverOption.METRICS_SESSION_GRAPH_REQUESTS_INTERVAL,
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_HIGHEST,
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_LOWEST,
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_SLO,
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_PUBLISH_PERCENTILES,
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_DIGITS,
              DseDriverOption.METRICS_NODE_GRAPH_MESSAGES_INTERVAL));

  public static Set<String> findConfiguredOptions(DriverConfig config) {
    Set<String> result = new TreeSet<>();
    for (DriverExecutionProfile profile : config.getProfiles().values()) {
      for (DseDriverOption option : OPTIONS) {
        if (isExplicitlyConfigured(profile, option)) {
          result.add(option.getPath());
        }
      }
    }

    DriverExecutionProfile defaultProfile = config.getDefaultProfile();
    if (defaultProfile.isDefined(DefaultDriverOption.METRICS_SESSION_ENABLED)) {
      for (String path :
          defaultProfile.getStringList(DefaultDriverOption.METRICS_SESSION_ENABLED)) {
        if (isDeprecatedSessionMetric(path)) {
          result.add(DefaultDriverOption.METRICS_SESSION_ENABLED.getPath() + "=" + path);
        }
      }
    }
    if (defaultProfile.isDefined(DefaultDriverOption.METRICS_NODE_ENABLED)) {
      for (String path : defaultProfile.getStringList(DefaultDriverOption.METRICS_NODE_ENABLED)) {
        if (isDeprecatedNodeMetric(path)) {
          result.add(DefaultDriverOption.METRICS_NODE_ENABLED.getPath() + "=" + path);
        }
      }
    }
    return Collections.unmodifiableSet(result);
  }

  public static boolean isDeprecatedSessionMetric(String path) {
    return GRAPH_REQUESTS.equals(path) || GRAPH_CLIENT_TIMEOUTS.equals(path);
  }

  public static boolean isDeprecatedNodeMetric(String path) {
    return GRAPH_MESSAGES.equals(path);
  }

  private static boolean isExplicitlyConfigured(
      DriverExecutionProfile profile, DseDriverOption option) {
    if (!profile.isDefined(option) || TypesafeDriverConfig.isDefault(profile, option)) {
      return false;
    }
    if (TypesafeDriverConfig.getRawConfig(profile) != null) {
      return true;
    }
    try {
      switch (option) {
        case GRAPH_TRAVERSAL_SOURCE:
          return !"g".equals(profile.getString(option));
        case GRAPH_PAGING_ENABLED:
          return !"AUTO".equals(profile.getString(option));
        case GRAPH_CONTINUOUS_PAGING_PAGE_SIZE:
          return profile.getInt(option) != 5000;
        case GRAPH_CONTINUOUS_PAGING_MAX_PAGES:
        case GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND:
          return profile.getInt(option) != 0;
        case GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES:
          return profile.getInt(option) != 4;
        case METRICS_SESSION_GRAPH_REQUESTS_HIGHEST:
          return !Duration.ofSeconds(12).equals(profile.getDuration(option));
        case METRICS_NODE_GRAPH_MESSAGES_HIGHEST:
          return !Duration.ofSeconds(3).equals(profile.getDuration(option));
        case METRICS_SESSION_GRAPH_REQUESTS_LOWEST:
        case METRICS_NODE_GRAPH_MESSAGES_LOWEST:
          return !Duration.ofMillis(1).equals(profile.getDuration(option));
        case METRICS_SESSION_GRAPH_REQUESTS_DIGITS:
        case METRICS_NODE_GRAPH_MESSAGES_DIGITS:
          return profile.getInt(option) != 3;
        case METRICS_SESSION_GRAPH_REQUESTS_INTERVAL:
        case METRICS_NODE_GRAPH_MESSAGES_INTERVAL:
          return !Duration.ofMinutes(5).equals(profile.getDuration(option));
        default:
          return true;
      }
    } catch (RuntimeException ignored) {
      // The option is obsolete, so an invalid value must not prevent session initialization.
      return true;
    }
  }

  private DeprecatedGraphConfig() {}
}
