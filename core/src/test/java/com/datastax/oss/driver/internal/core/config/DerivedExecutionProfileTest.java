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
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.DriverOption;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(DataProviderRunner.class)
public class DerivedExecutionProfileTest {

  private static final DriverOption OPTION = () -> "test.option";
  private static final DriverOption OTHER_OPTION = () -> "other.option";

  private DriverExecutionProfile base;

  @Before
  public void setup() {
    base = Mockito.mock(DriverExecutionProfile.class);
  }

  /** One row per typed getter: how to call it, a sample value, and how to stub the base with it. */
  @DataProvider
  public static Object[][] getters() {
    return new Object[][] {
      getter(
          "getBoolean",
          p -> p.getBoolean(OPTION),
          true,
          (b, v) -> when(b.getBoolean(OPTION)).thenReturn((Boolean) v)),
      getter(
          "getBooleanList",
          p -> p.getBooleanList(OPTION),
          Arrays.asList(true, false),
          (b, v) -> when(b.getBooleanList(OPTION)).thenReturn(cast(v))),
      getter(
          "getInt",
          p -> p.getInt(OPTION),
          42,
          (b, v) -> when(b.getInt(OPTION)).thenReturn((Integer) v)),
      getter(
          "getIntList",
          p -> p.getIntList(OPTION),
          Arrays.asList(1, 2),
          (b, v) -> when(b.getIntList(OPTION)).thenReturn(cast(v))),
      getter(
          "getLong",
          p -> p.getLong(OPTION),
          42L,
          (b, v) -> when(b.getLong(OPTION)).thenReturn((Long) v)),
      getter(
          "getLongList",
          p -> p.getLongList(OPTION),
          Arrays.asList(1L, 2L),
          (b, v) -> when(b.getLongList(OPTION)).thenReturn(cast(v))),
      getter(
          "getDouble",
          p -> p.getDouble(OPTION),
          4.2,
          (b, v) -> when(b.getDouble(OPTION)).thenReturn((Double) v)),
      getter(
          "getDoubleList",
          p -> p.getDoubleList(OPTION),
          Arrays.asList(1.0, 2.0),
          (b, v) -> when(b.getDoubleList(OPTION)).thenReturn(cast(v))),
      getter(
          "getString",
          p -> p.getString(OPTION),
          "a value",
          (b, v) -> when(b.getString(OPTION)).thenReturn((String) v)),
      getter(
          "getStringList",
          p -> p.getStringList(OPTION),
          Arrays.asList("a", "b"),
          (b, v) -> when(b.getStringList(OPTION)).thenReturn(cast(v))),
      getter(
          "getStringMap",
          p -> p.getStringMap(OPTION),
          Collections.singletonMap("k", "v"),
          (b, v) -> when(b.getStringMap(OPTION)).thenReturn(cast(v))),
      getter(
          "getBytes",
          p -> p.getBytes(OPTION),
          1024L,
          (b, v) -> when(b.getBytes(OPTION)).thenReturn((Long) v)),
      getter(
          "getBytesList",
          p -> p.getBytesList(OPTION),
          Arrays.asList(512L, 1024L),
          (b, v) -> when(b.getBytesList(OPTION)).thenReturn(cast(v))),
      getter(
          "getDuration",
          p -> p.getDuration(OPTION),
          Duration.ofSeconds(5),
          (b, v) -> when(b.getDuration(OPTION)).thenReturn((Duration) v)),
      getter(
          "getDurationList",
          p -> p.getDurationList(OPTION),
          Arrays.asList(Duration.ofSeconds(1)),
          (b, v) -> when(b.getDurationList(OPTION)).thenReturn(cast(v))),
    };
  }

  @Test
  @UseDataProvider("getters")
  public void should_return_the_override_without_consulting_the_base(
      String name,
      Function<DriverExecutionProfile, Object> invoke,
      Object value,
      BiConsumer<DriverExecutionProfile, Object> stubBase) {

    DriverExecutionProfile derived = DerivedExecutionProfile.with(base, OPTION, value);

    assertThat(invoke.apply(derived)).isEqualTo(value);
    Mockito.verifyNoInteractions(base);
  }

  @Test
  @UseDataProvider("getters")
  public void should_delegate_to_the_base_when_the_option_is_not_overridden(
      String name,
      Function<DriverExecutionProfile, Object> invoke,
      Object value,
      BiConsumer<DriverExecutionProfile, Object> stubBase) {

    stubBase.accept(base, value);
    DriverExecutionProfile derived =
        DerivedExecutionProfile.with(base, OTHER_OPTION, "something else");

    assertThat(invoke.apply(derived)).isEqualTo(value);
  }

  @DataProvider
  @SuppressWarnings("deprecation")
  public static Object[][] applicationOptionAliases() {
    return new Object[][] {
      {DseDriverOption.APPLICATION_NAME, DefaultDriverOption.APPLICATION_NAME},
      {DseDriverOption.APPLICATION_VERSION, DefaultDriverOption.APPLICATION_VERSION}
    };
  }

  @Test
  @UseDataProvider("applicationOptionAliases")
  public void should_read_legacy_alias_from_identity_based_base(
      DriverOption alias, DriverOption canonicalOption) {
    when(base.isDefined(canonicalOption)).thenReturn(true);
    when(base.getString(canonicalOption)).thenReturn("from base");
    DriverExecutionProfile derived = DerivedExecutionProfile.with(base, OTHER_OPTION, "override");

    assertThat(derived.getString(alias)).isEqualTo("from base");
  }

  @Test
  @UseDataProvider("applicationOptionAliases")
  public void should_report_legacy_alias_defined_in_identity_based_base(
      DriverOption alias, DriverOption canonicalOption) {
    when(base.isDefined(canonicalOption)).thenReturn(true);
    DriverExecutionProfile derived = DerivedExecutionProfile.with(base, OTHER_OPTION, "override");

    assertThat(derived.isDefined(alias)).isTrue();
  }

  @Test
  @UseDataProvider("applicationOptionAliases")
  public void should_preserve_legacy_alias_defined_in_identity_based_base(
      DriverOption alias, DriverOption canonicalOption) {
    when(base.isDefined(alias)).thenReturn(true);
    when(base.getString(alias)).thenReturn("legacy value");
    DriverExecutionProfile derived = DerivedExecutionProfile.with(base, OTHER_OPTION, "override");

    assertThat(derived.isDefined(alias)).isTrue();
    assertThat(derived.getString(alias)).isEqualTo("legacy value");
  }

  @Test
  @UseDataProvider("applicationOptionAliases")
  public void should_preserve_requested_key_when_base_defines_both_aliases(
      DriverOption alias, DriverOption canonicalOption) {
    when(base.isDefined(alias)).thenReturn(true);
    when(base.getString(alias)).thenReturn("legacy value");
    when(base.isDefined(canonicalOption)).thenReturn(true);
    when(base.getString(canonicalOption)).thenReturn("current value");
    DriverExecutionProfile derived = DerivedExecutionProfile.with(base, OTHER_OPTION, "override");

    assertThat(derived.isDefined(alias)).isTrue();
    assertThat(derived.getString(alias)).isEqualTo("legacy value");
    assertThat(derived.isDefined(canonicalOption)).isTrue();
    assertThat(derived.getString(canonicalOption)).isEqualTo("current value");
  }

  @Test
  @UseDataProvider("applicationOptionAliases")
  public void should_apply_alias_overrides_and_removals_before_base_fallback(
      DriverOption alias, DriverOption canonicalOption) {
    when(base.isDefined(alias)).thenReturn(true);
    when(base.getString(alias)).thenReturn("legacy value");
    DriverExecutionProfile derived =
        DerivedExecutionProfile.with(base, canonicalOption, "override");

    assertThat(derived.isDefined(alias)).isTrue();
    assertThat(derived.getString(alias)).isEqualTo("override");

    DriverExecutionProfile removed = derived.without(alias);
    assertThat(removed.isDefined(alias)).isFalse();
    assertThat(removed.isDefined(canonicalOption)).isFalse();
    assertThatThrownBy(() -> removed.getString(alias)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> removed.getString(canonicalOption))
        .isInstanceOf(IllegalArgumentException.class);
    Mockito.verifyNoInteractions(base);
  }

  @Test
  public void should_preserve_custom_option_with_application_alias_path() {
    DriverOption customOption = () -> DefaultDriverOption.APPLICATION_NAME.getPath();
    when(base.isDefined(customOption)).thenReturn(true);
    when(base.getString(customOption)).thenReturn("custom value");
    when(base.getString(DefaultDriverOption.APPLICATION_NAME)).thenReturn("current value");
    DriverExecutionProfile derived = DerivedExecutionProfile.with(base, OTHER_OPTION, "override");

    assertThat(derived.isDefined(customOption)).isTrue();
    assertThat(derived.getString(customOption)).isEqualTo("custom value");
  }

  /**
   * {@code without()} is not "fall back to the base": it stores a sentinel that short-circuits the
   * lookup, so the getter throws even though the base still defines the option.
   */
  @Test
  @UseDataProvider("getters")
  public void should_throw_after_the_option_is_removed_even_if_the_base_defines_it(
      String name,
      Function<DriverExecutionProfile, Object> invoke,
      Object value,
      BiConsumer<DriverExecutionProfile, Object> stubBase) {

    stubBase.accept(base, value);
    DriverExecutionProfile derived = DerivedExecutionProfile.without(base, OPTION);

    assertThatThrownBy(() -> invoke.apply(derived))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing configuration option test.option");
  }

  @Test
  public void should_throw_when_neither_the_override_nor_the_base_has_a_value() {
    DriverExecutionProfile derived =
        DerivedExecutionProfile.with(base, OTHER_OPTION, "something else");

    assertThatThrownBy(() -> derived.getString(OPTION))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Missing configuration option test.option");
  }

  /**
   * Deriving from a derived profile reuses the original base and merges the overrides.
   * Re-overriding the same option must replace it — building the map with both copies would throw
   * on the duplicate key.
   */
  @Test
  public void should_replace_an_existing_override_rather_than_duplicate_it() {
    DriverExecutionProfile first = DerivedExecutionProfile.with(base, OPTION, "first");

    DriverExecutionProfile second = DerivedExecutionProfile.with(first, OPTION, "second");

    assertThat(second.getString(OPTION)).isEqualTo("second");
  }

  @Test
  public void should_keep_earlier_overrides_when_deriving_again() {
    DriverExecutionProfile first = DerivedExecutionProfile.with(base, OPTION, "first");

    DriverExecutionProfile second = DerivedExecutionProfile.with(first, OTHER_OPTION, "other");

    assertThat(second.getString(OPTION)).isEqualTo("first");
    assertThat(second.getString(OTHER_OPTION)).isEqualTo("other");
  }

  @Test
  public void should_report_an_overridden_option_as_defined() {
    assertThat(DerivedExecutionProfile.with(base, OPTION, "value").isDefined(OPTION)).isTrue();
  }

  @Test
  public void should_report_a_removed_option_as_undefined() {
    when(base.isDefined(OPTION)).thenReturn(true);

    assertThat(DerivedExecutionProfile.without(base, OPTION).isDefined(OPTION)).isFalse();
  }

  @Test
  public void should_delegate_is_defined_when_the_option_is_not_overridden() {
    when(base.isDefined(OTHER_OPTION)).thenReturn(true);

    assertThat(DerivedExecutionProfile.with(base, OPTION, "value").isDefined(OTHER_OPTION))
        .isTrue();
  }

  @Test
  public void should_take_its_name_from_the_base() {
    when(base.getName()).thenReturn("profile1");

    assertThat(DerivedExecutionProfile.with(base, OPTION, "value").getName()).isEqualTo("profile1");
  }

  @Test
  public void should_let_overrides_outrank_the_base_in_the_entry_set() {
    when(base.entrySet()).thenReturn(baseEntries());

    SortedSet<Map.Entry<String, Object>> entries =
        DerivedExecutionProfile.with(base, OPTION, "from override").entrySet();

    assertThat(entries)
        .extracting(Map.Entry::getKey)
        .containsExactly("other.option", "test.option");
    assertThat(entries).contains(new AbstractMap.SimpleEntry<>("test.option", "from override"));
    assertThat(entries).contains(new AbstractMap.SimpleEntry<>("other.option", "from base"));
  }

  /**
   * A removed option is skipped among the overrides but still arrives from the base, so {@code
   * entrySet()} lists an option that {@code isDefined()} reports as absent and whose getter throws.
   * Recorded as observed behaviour, not endorsed.
   */
  @Test
  public void should_still_list_a_removed_option_that_the_base_defines() {
    when(base.entrySet()).thenReturn(baseEntries());

    SortedSet<Map.Entry<String, Object>> entries =
        DerivedExecutionProfile.without(base, OPTION).entrySet();

    assertThat(entries).contains(new AbstractMap.SimpleEntry<>("test.option", "from base"));
  }

  private static SortedSet<Map.Entry<String, Object>> baseEntries() {
    SortedSet<Map.Entry<String, Object>> entries = new TreeSet<>(Map.Entry.comparingByKey());
    entries.add(new AbstractMap.SimpleEntry<>("test.option", "from base"));
    entries.add(new AbstractMap.SimpleEntry<>("other.option", "from base"));
    return entries;
  }

  private static Object[] getter(
      String name,
      Function<DriverExecutionProfile, Object> invoke,
      Object value,
      BiConsumer<DriverExecutionProfile, Object> stubBase) {
    return new Object[] {name, invoke, value, stubBase};
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(Object value) {
    return (T) value;
  }
}
