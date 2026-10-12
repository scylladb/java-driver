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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.internal.core.config.map.MapBasedDriverExecutionProfile;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
@SuppressWarnings("deprecation")
public class DriverOptionAliasesTest {

  @DataProvider
  public static Object[][] aliases() {
    return new Object[][] {
      {DseDriverOption.APPLICATION_NAME, DefaultDriverOption.APPLICATION_NAME},
      {DseDriverOption.APPLICATION_VERSION, DefaultDriverOption.APPLICATION_VERSION}
    };
  }

  @Test
  @UseDataProvider("aliases")
  public void should_read_live_legacy_override_map(DriverOption alias, DriverOption canonical) {
    DriverExecutionProfile base = mock(DriverExecutionProfile.class);
    when(base.getString(canonical)).thenReturn("base");
    when(base.entrySet()).thenReturn(Collections.emptySortedSet());
    Map<DriverOption, Object> overrides = new LinkedHashMap<>();
    overrides.put(alias, "legacy");
    DriverExecutionProfile derived = new DerivedExecutionProfile(base, overrides);

    assertThat(derived.isDefined(alias)).isTrue();
    assertThat(derived.isDefined(canonical)).isTrue();
    assertThat(derived.getString(alias)).isEqualTo("legacy");
    assertThat(derived.getString(canonical)).isEqualTo("legacy");

    overrides.put(alias, "updated");
    assertThat(derived.getString(alias)).isEqualTo("updated");
    overrides.put(canonical, "canonical");
    assertThat(derived.getString(alias)).isEqualTo("canonical");
    assertThat(derived.entrySet())
        .containsExactly(new AbstractMap.SimpleEntry<>(canonical.getPath(), "canonical"));
    overrides.clear();
    assertThat(derived.getString(alias)).isEqualTo("base");
  }

  @Test
  @UseDataProvider("aliases")
  public void should_replace_or_remove_legacy_constructor_override(
      DriverOption alias, DriverOption canonical) {
    DriverExecutionProfile base = mock(DriverExecutionProfile.class);
    when(base.entrySet()).thenReturn(Collections.emptySortedSet());
    DriverExecutionProfile derived =
        new DerivedExecutionProfile(base, Collections.singletonMap(alias, "legacy"));

    DriverExecutionProfile updated = derived.withString(canonical, "updated");
    assertThat(updated.getString(alias)).isEqualTo("updated");
    assertThat(updated.entrySet())
        .containsExactly(new AbstractMap.SimpleEntry<>(canonical.getPath(), "updated"));

    DriverExecutionProfile removed = derived.without(canonical);
    assertThat(removed.isDefined(alias)).isFalse();
    assertThat(removed.isDefined(canonical)).isFalse();
    assertThatThrownBy(() -> removed.getString(alias)).isInstanceOf(IllegalArgumentException.class);
    assertThat(removed.entrySet()).isEmpty();
  }

  @Test
  @UseDataProvider("aliases")
  public void should_prioritize_live_legacy_profile_over_canonical_default(
      DriverOption alias, DriverOption canonical) {
    Map<DriverOption, Object> profile = new LinkedHashMap<>();
    profile.put(alias, "profile");
    DriverExecutionProfile executionProfile =
        new MapBasedDriverExecutionProfile(
            "profile", profile, Collections.singletonMap(canonical, "default"));

    assertThat(executionProfile.isDefined(alias)).isTrue();
    assertThat(executionProfile.getString(alias)).isEqualTo("profile");
    assertThat(executionProfile.getString(canonical)).isEqualTo("profile");
    profile.put(alias, "updated");
    assertThat(executionProfile.getString(canonical)).isEqualTo("updated");
    profile.put(canonical, "canonical");
    assertThat(executionProfile.getString(alias)).isEqualTo("canonical");
    assertThat(executionProfile.entrySet())
        .containsExactly(new AbstractMap.SimpleEntry<>(canonical.getPath(), "canonical"));
    profile.clear();
    assertThat(executionProfile.getString(alias)).isEqualTo("default");
  }

  @Test
  @UseDataProvider("aliases")
  public void should_inherit_live_legacy_default_map(DriverOption alias, DriverOption canonical) {
    Map<DriverOption, Object> defaults = new LinkedHashMap<>();
    defaults.put(alias, "default");
    Map<String, Map<DriverOption, Object>> options = new LinkedHashMap<>();
    options.put(DriverExecutionProfile.DEFAULT_NAME, defaults);
    options.put("profile", Collections.emptyMap());
    DriverExecutionProfile profile = new MapBasedDriverExecutionProfile(options, "profile");

    assertThat(profile.isDefined(alias)).isTrue();
    assertThat(profile.isDefined(canonical)).isTrue();
    assertThat(profile.getString(alias)).isEqualTo("default");
    assertThat(profile.getString(canonical)).isEqualTo("default");
    defaults.put(alias, "updated");
    assertThat(profile.getString(alias)).isEqualTo("updated");
    defaults.clear();
    assertThat(profile.isDefined(alias)).isFalse();
    assertThatThrownBy(() -> profile.getString(alias)).isInstanceOf(IllegalArgumentException.class);
  }
}
