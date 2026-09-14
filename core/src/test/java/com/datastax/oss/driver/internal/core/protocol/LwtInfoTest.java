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
package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class LwtInfoTest {

  private static final String KEY = "SCYLLA_LWT_ADD_METADATA_MARK";
  private static final String MASK_PREFIX = "LWT_OPTIMIZATION_META_BIT_MASK=";

  @Test
  public void should_load_mask_from_supported_options() {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(supported(MASK_PREFIX + "32"));

    assertThat(lwtInfo).isNotNull();
    assertThat(lwtInfo.getMask()).isEqualTo(32);
  }

  @Test
  public void should_convert_mask_returned_as_unsigned_int32() {
    // The server sends the mask as an unsigned int32, so anything above Integer.MAX_VALUE has to
    // be folded back into a signed int
    assertThat(maskOf(MASK_PREFIX + "4294967295")).isEqualTo(-1);
    assertThat(maskOf(MASK_PREFIX + "2147483648")).isEqualTo(Integer.MIN_VALUE);
  }

  @Test
  public void should_keep_mask_at_the_signed_boundary_unchanged() {
    assertThat(maskOf(MASK_PREFIX + Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void should_return_null_when_option_is_absent() {
    assertThat(LwtInfo.loadFromSupportedOptions(Collections.emptyMap())).isNull();
    assertThat(LwtInfo.loadFromSupportedOptions(supportedUnder("OTHER_OPTION", "whatever")))
        .isNull();
  }

  @Test
  public void should_return_null_when_value_list_is_null() {
    Map<String, List<String>> supported = new HashMap<>();
    supported.put(KEY, null);

    assertThat(LwtInfo.loadFromSupportedOptions(supported)).isNull();
  }

  @Test
  public void should_return_null_when_value_list_does_not_hold_exactly_one_entry() {
    assertThat(LwtInfo.loadFromSupportedOptions(supportedList(Collections.emptyList()))).isNull();
    assertThat(
            LwtInfo.loadFromSupportedOptions(
                supportedList(Arrays.asList(MASK_PREFIX + "32", MASK_PREFIX + "64"))))
        .isNull();
  }

  @Test
  public void should_return_null_when_value_is_null() {
    assertThat(LwtInfo.loadFromSupportedOptions(supportedList(singletonWithNull()))).isNull();
  }

  @Test
  public void should_return_null_when_value_has_the_wrong_prefix() {
    assertThat(LwtInfo.loadFromSupportedOptions(supported("SOMETHING_ELSE=32"))).isNull();
    assertThat(LwtInfo.loadFromSupportedOptions(supported("32"))).isNull();
  }

  @Test
  public void should_return_null_when_mask_is_not_a_number() {
    assertThat(LwtInfo.loadFromSupportedOptions(supported(MASK_PREFIX + "not-a-number"))).isNull();
    assertThat(LwtInfo.loadFromSupportedOptions(supported(MASK_PREFIX))).isNull();
  }

  @Test
  public void should_detect_lwt_flag() {
    // The mask Scylla actually sends is the sign bit, so a signed comparison would break here
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(supported(MASK_PREFIX + "2147483648"));

    assertThat(lwtInfo.isLwt(Integer.MIN_VALUE)).isTrue();
    assertThat(lwtInfo.isLwt(Integer.MIN_VALUE | 1)).isTrue();
    assertThat(lwtInfo.isLwt(0)).isFalse();
    assertThat(lwtInfo.isLwt(1)).isFalse();
  }

  @Test
  public void should_echo_mask_back_in_startup_options() {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(supported(MASK_PREFIX + "32"));
    Map<String, String> options = new LinkedHashMap<>();

    lwtInfo.populateStartupOptions(options);

    assertThat(options).containsExactly(entry(KEY, "32"));
  }

  @Test
  public void should_echo_the_folded_mask_back_in_its_signed_form() {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(supported(MASK_PREFIX + "4294967295"));
    Map<String, String> options = new LinkedHashMap<>();

    lwtInfo.populateStartupOptions(options);

    // What goes back is the signed form, not the unsigned string the server sent
    assertThat(options).containsExactly(entry(KEY, "-1"));
  }

  private static int maskOf(String value) {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(supported(value));
    assertThat(lwtInfo).isNotNull();
    return lwtInfo.getMask();
  }

  private static Map<String, List<String>> supported(String value) {
    return supportedUnder(KEY, value);
  }

  private static Map<String, List<String>> supportedUnder(String key, String value) {
    return Collections.singletonMap(key, Collections.singletonList(value));
  }

  private static Map<String, List<String>> supportedList(List<String> values) {
    return Collections.singletonMap(KEY, values);
  }

  private static List<String> singletonWithNull() {
    List<String> values = new ArrayList<>();
    values.add(null);
    return values;
  }
}
