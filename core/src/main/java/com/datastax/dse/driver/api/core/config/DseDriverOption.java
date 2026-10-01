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
   * Whether to send events for Insights monitoring.
   *
   * <p>Value type: boolean
   */
  MONITOR_REPORTING_ENABLED("advanced.monitor-reporting.enabled"),
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
