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
package com.datastax.oss.driver.internal.core.context;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/**
 * Adds the {@code DRIVER_CONFIG} entry to the control connection's CQL {@code STARTUP} options, so
 * ScyllaDB can store it in {@code system.clients.client_options} and operators can inspect the
 * driver's effective settings while investigating incidents.
 *
 * <p>Most of the blob describes the whole session; its TLS group describes the control connection
 * carrying it. Pooled connections are correlated back to that control connection through the {@link
 * StartupOptionsBuilder#SESSION_ID_KEY SESSION_ID} startup option, which the driver sends on every
 * connection unconditionally and independently of this reporter.
 *
 * <p>Governed by {@code advanced.driver-config-reporting.enabled} (enabled by default).
 */
public interface DriverConfigReporter {

  /** Immutable snapshot of the effective TLS state of the reporting control connection. */
  final class TlsInfo {
    private static final TlsInfo DISABLED = new TlsInfo(false, Optional.empty());
    private static final TlsInfo ENABLED_UNKNOWN = new TlsInfo(true, Optional.empty());

    private final boolean enabled;
    private final Optional<Boolean> hostnameVerification;

    private TlsInfo(boolean enabled, Optional<Boolean> hostnameVerification) {
      this.enabled = enabled;
      this.hostnameVerification = hostnameVerification;
    }

    /** Returns a snapshot for a connection without a Netty {@code SslHandler}. */
    public static TlsInfo disabled() {
      return DISABLED;
    }

    /** Returns a snapshot for a TLS connection whose hostname-verification state is known. */
    public static TlsInfo enabled(boolean hostnameVerification) {
      return new TlsInfo(true, Optional.of(hostnameVerification));
    }

    /** Returns a snapshot for a TLS connection whose hostname-verification state is unknown. */
    public static TlsInfo enabledWithUnknownHostnameVerification() {
      return ENABLED_UNKNOWN;
    }

    public boolean isEnabled() {
      return enabled;
    }

    /** Empty when TLS is disabled or its active engine could not expose this state. */
    @NonNull
    public Optional<Boolean> getHostnameVerification() {
      return hostnameVerification;
    }
  }

  /**
   * Adds the {@code DRIVER_CONFIG} blob to the given startup options, unless configuration
   * reporting is disabled.
   *
   * <p>Called from the protocol-initialization handler for the control connection only.
   *
   * <p><b>Implementations must not throw:</b> this runs on the connection initialization path, so a
   * failure to build the report must be swallowed (and logged) rather than propagated, otherwise it
   * would prevent the session from establishing or reconnecting.
   *
   * <p>The report describes the driver's own configuration and the supplied effective TLS state of
   * the control connection. The caller must capture that state after channel customization and
   * immediately before this method is invoked.
   *
   * @param startupOptions startup options to add the report to
   * @param tlsInfo immutable snapshot of the control connection's effective TLS state
   */
  void populateControlConnectionOptions(
      @NonNull Map<String, String> startupOptions, @NonNull TlsInfo tlsInfo);
}
