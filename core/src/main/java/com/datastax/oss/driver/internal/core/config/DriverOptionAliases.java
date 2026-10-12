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
import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;

/** Resolves retained application metadata aliases to their current option keys. */
public final class DriverOptionAliases {

  @NonNull
  @SuppressWarnings("deprecation")
  public static DriverOption canonicalize(@NonNull DriverOption option) {
    Objects.requireNonNull(option, "option");
    if (option == DseDriverOption.APPLICATION_NAME) {
      return DefaultDriverOption.APPLICATION_NAME;
    }
    if (option == DseDriverOption.APPLICATION_VERSION) {
      return DefaultDriverOption.APPLICATION_VERSION;
    }
    return option;
  }

  /** Resolves a key in a live backing map, preferring current keys over legacy aliases. */
  @NonNull
  @SuppressWarnings("deprecation")
  public static DriverOption resolveKey(
      @NonNull Map<DriverOption, ?> values, @NonNull DriverOption option) {
    DriverOption canonicalOption = canonicalize(option);
    if (!values.containsKey(canonicalOption)) {
      if (canonicalOption == DefaultDriverOption.APPLICATION_NAME) {
        return DseDriverOption.APPLICATION_NAME;
      }
      if (canonicalOption == DefaultDriverOption.APPLICATION_VERSION) {
        return DseDriverOption.APPLICATION_VERSION;
      }
    }
    return canonicalOption;
  }

  private DriverOptionAliases() {}
}
