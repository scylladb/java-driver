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
package com.datastax.oss.driver.internal.core.config.map;

import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.jcip.annotations.ThreadSafe;

/** Live driver-only view of an OptionsMap and its Graph-option write provenance. */
@ThreadSafe
public final class OptionsMapView extends AbstractMap<String, Map<DriverOption, Object>> {

  private final ConcurrentHashMap<String, Map<DriverOption, Object>> values;
  private final ConcurrentHashMap<String, Set<DriverOption>> explicitOptions;
  private final boolean legacyUnknown;

  public OptionsMapView(
      ConcurrentHashMap<String, Map<DriverOption, Object>> values,
      ConcurrentHashMap<String, Set<DriverOption>> explicitOptions,
      boolean legacyUnknown) {
    this.values = values;
    this.explicitOptions = explicitOptions;
    this.legacyUnknown = legacyUnknown;
  }

  @Nullable
  public Boolean wasExplicitlySet(String profile, DriverOption option) {
    Set<DriverOption> options = explicitOptions.get(profile);
    if (options != null && options.contains(option)) {
      return true;
    }
    return legacyUnknown ? null : false;
  }

  @Override
  public Set<Entry<String, Map<DriverOption, Object>>> entrySet() {
    return values.entrySet();
  }

  @Override
  public Map<DriverOption, Object> get(Object key) {
    return values.get(key);
  }

  @Override
  public boolean containsKey(Object key) {
    return values.containsKey(key);
  }

  @Override
  public Map<DriverOption, Object> put(String key, Map<DriverOption, Object> value) {
    return values.put(key, value);
  }

  @Override
  public Map<DriverOption, Object> remove(Object key) {
    return values.remove(key);
  }

  @Override
  public void clear() {
    values.clear();
  }
}
