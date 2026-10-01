/*
 * Copyright ScyllaDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class LwtInfoTest {

  private static final String KEY = "SCYLLA_LWT_ADD_METADATA_MARK";
  private static final String MASK_PREFIX = "LWT_OPTIMIZATION_META_BIT_MASK=";

  private static Map<String, List<String>> supported(List<String> values) {
    Map<String, List<String>> supported = new HashMap<String, List<String>>();
    supported.put(KEY, values);
    return supported;
  }

  private static LwtInfo parse(String value) {
    return LwtInfo.parseLwtInfo(supported(Collections.singletonList(value)));
  }

  @Test(groups = "unit")
  public void should_parse_mask_and_detect_lwt_flag() {
    LwtInfo info = parse(MASK_PREFIX + "4");

    assertThat(info).isNotNull();
    assertThat(info.getMask()).isEqualTo(4);
    assertThat(info.isLwt(4)).isTrue();
    assertThat(info.isLwt(4 | 1 | 8)).isTrue();
    assertThat(info.isLwt(0)).isFalse();
    assertThat(info.isLwt(1 | 2 | 8)).isFalse();
  }

  @Test(groups = "unit")
  public void should_require_every_mask_bit_for_multi_bit_mask() {
    LwtInfo info = parse(MASK_PREFIX + "6");

    assertThat(info.isLwt(6)).isTrue();
    assertThat(info.isLwt(2)).isFalse();
    assertThat(info.isLwt(4)).isFalse();
  }

  @Test(groups = "unit")
  public void should_add_mask_to_startup_options() {
    Map<String, String> options = new HashMap<String, String>();

    parse(MASK_PREFIX + "4").addOption(options);

    assertThat(options).hasSize(1).containsEntry(KEY, "4");
  }

  @Test(groups = "unit")
  public void should_convert_unsigned_high_bit_mask_to_signed_int() {
    LwtInfo info = parse(MASK_PREFIX + "2147483648");

    assertThat(info.getMask()).isEqualTo(Integer.MIN_VALUE);
    assertThat(info.isLwt(Integer.MIN_VALUE)).isTrue();
    assertThat(info.isLwt(Integer.MIN_VALUE | 1)).isTrue();
    assertThat(info.isLwt(Integer.MAX_VALUE)).isFalse();
    Map<String, String> options = new HashMap<String, String>();
    info.addOption(options);
    // What goes back is the signed form, not the unsigned string the server sent
    assertThat(options).containsEntry(KEY, "-2147483648");
  }

  @Test(groups = "unit")
  public void should_convert_max_unsigned_int_mask_to_all_bits() {
    LwtInfo info = parse(MASK_PREFIX + "4294967295");

    assertThat(info.getMask()).isEqualTo(-1);
  }

  @Test(groups = "unit")
  public void should_return_null_when_key_absent() {
    Map<String, List<String>> supported = new HashMap<String, List<String>>();
    supported.put("SCYLLA_SHARD", Collections.singletonList("0"));

    assertThat(LwtInfo.parseLwtInfo(supported)).isNull();
  }

  @Test(groups = "unit")
  public void should_return_null_when_value_list_is_null_or_not_single() {
    assertThat(LwtInfo.parseLwtInfo(supported(null))).isNull();
    assertThat(LwtInfo.parseLwtInfo(supported(Collections.<String>emptyList()))).isNull();
    assertThat(LwtInfo.parseLwtInfo(supported(Arrays.asList(MASK_PREFIX + "4", MASK_PREFIX + "8"))))
        .isNull();
  }

  @Test(groups = "unit")
  public void should_return_null_when_value_is_null_or_lacks_prefix() {
    assertThat(parse(null)).isNull();
    assertThat(parse("4")).isNull();
    assertThat(parse("LWT_OPTIMIZATION_META_BIT_MASK")).isNull();
    assertThat(parse("OTHER_KEY=4")).isNull();
  }

  @Test(groups = "unit")
  public void should_return_null_when_mask_is_not_a_number() {
    assertThat(parse(MASK_PREFIX)).isNull();
    assertThat(parse(MASK_PREFIX + "abc")).isNull();
    assertThat(parse(MASK_PREFIX + "0x4")).isNull();
    assertThat(parse(MASK_PREFIX + "99999999999999999999")).isNull();
  }

  @Test(groups = "unit")
  public void should_silently_truncate_mask_above_unsigned_int_range() {
    // Pins current behaviour (#1175): masks above 2^32-1 are truncated to 32 bits, not rejected
    assertThat(parse(MASK_PREFIX + "8589934593").getMask()).isEqualTo(1);
    LwtInfo info = parse(MASK_PREFIX + "4294967300");
    assertThat(info.getMask()).isEqualTo(4);
    assertThat(info.isLwt(4)).isTrue();
    assertThat(info.isLwt(0)).isFalse();
  }

  @Test(groups = "unit")
  public void should_accept_negative_mask() {
    // Pins current behaviour (#1175): negative masks are accepted
    LwtInfo info = parse(MASK_PREFIX + "-4");

    assertThat(info.getMask()).isEqualTo(-4);
    assertThat(info.isLwt(-4)).isTrue();
    assertThat(info.isLwt(4)).isFalse();
  }

  @Test(groups = "unit")
  public void should_treat_every_flag_as_lwt_when_mask_is_zero() {
    // Pins current behaviour (#1175): a zero mask is accepted and makes isLwt true for any flags
    LwtInfo info = parse(MASK_PREFIX + "0");

    assertThat(info.getMask()).isEqualTo(0);
    assertThat(info.isLwt(0)).isTrue();
    assertThat(info.isLwt(0x7)).isTrue();
  }
}
