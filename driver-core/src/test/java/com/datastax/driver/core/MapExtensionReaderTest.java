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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.Test;

public class MapExtensionReaderTest {

  @Test(groups = "unit")
  public void should_parse_empty_map() {
    assertThat(new MapExtensionReader(encode()).parse()).isEmpty();
  }

  @Test(groups = "unit")
  public void should_parse_single_entry() {
    Map<String, String> parsed = new MapExtensionReader(encode("enabled", "true")).parse();

    assertThat(parsed).isEqualTo(ImmutableMap.of("enabled", "true"));
  }

  @Test(groups = "unit")
  public void should_parse_several_entries_in_order() {
    Map<String, String> parsed =
        new MapExtensionReader(encode("enabled", "true", "preimage", "false", "ttl", "86400"))
            .parse();

    assertThat(parsed.keySet()).containsExactly("enabled", "preimage", "ttl");
    assertThat(parsed)
        .isEqualTo(ImmutableMap.of("enabled", "true", "preimage", "false", "ttl", "86400"));
  }

  @Test(groups = "unit")
  public void should_parse_empty_strings() {
    assertThat(new MapExtensionReader(encode("", "")).parse()).isEqualTo(ImmutableMap.of("", ""));
  }

  @Test(groups = "unit")
  public void should_decode_non_ascii_utf8() {
    Map<String, String> parsed = new MapExtensionReader(encode("zespół", "żółć ☃")).parse();

    assertThat(parsed).isEqualTo(ImmutableMap.of("zespół", "żółć ☃"));
  }

  @Test(groups = "unit")
  public void should_read_little_endian_lengths() {
    ByteBuffer raw = ByteBuffer.wrap(new byte[] {1, 0, 0, 0, 1, 0, 0, 0, 'k', 1, 0, 0, 0, 'v'});

    assertThat(new MapExtensionReader(raw).parse()).isEqualTo(ImmutableMap.of("k", "v"));
  }

  @Test(groups = "unit")
  public void should_not_move_caller_buffer() {
    ByteBuffer raw = encode("k", "v");
    int limit = raw.limit();

    new MapExtensionReader(raw).parse();

    assertThat(raw.position()).isEqualTo(0);
    assertThat(raw.limit()).isEqualTo(limit);
    assertThat(raw.order()).isEqualTo(ByteOrder.BIG_ENDIAN);
  }

  @Test(groups = "unit")
  public void should_read_from_current_position_of_caller_buffer() {
    ByteBuffer payload = encode("k", "v");
    ByteBuffer raw = ByteBuffer.allocate(3 + payload.remaining());
    raw.put(new byte[] {9, 9, 9}).put(payload);
    raw.position(3);

    assertThat(new MapExtensionReader(raw).parse()).isEqualTo(ImmutableMap.of("k", "v"));
    assertThat(raw.position()).isEqualTo(3);
  }

  @Test(groups = "unit", expectedExceptions = NullPointerException.class)
  public void should_reject_null_buffer() {
    new MapExtensionReader(null);
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void should_reject_negative_element_count() {
    new MapExtensionReader(ints(-1)).parse();
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void should_reject_empty_buffer() {
    new MapExtensionReader(ByteBuffer.allocate(0)).parse();
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void should_reject_count_exceeding_entries() {
    ByteBuffer raw = encode("k", "v");
    raw.order(ByteOrder.LITTLE_ENDIAN).putInt(0, 2);

    new MapExtensionReader(raw).parse();
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void should_reject_truncated_string() {
    new MapExtensionReader(ints(1, 10)).parse();
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void should_reject_negative_string_length() {
    new MapExtensionReader(ints(1, -1)).parse();
  }

  @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
  public void should_fail_on_duplicate_keys() {
    // Pins current behaviour (#1174): duplicate keys escape from ImmutableMap.Builder.build()
    new MapExtensionReader(encode("k", "v1", "k", "v2")).parse();
  }

  @Test(groups = "unit")
  public void should_ignore_trailing_bytes() {
    // Lenient on purpose: bytes after the last entry are ignored, so appended fields don't break
    // parsing
    ByteBuffer payload = encode("k", "v");
    ByteBuffer raw = ByteBuffer.allocate(payload.remaining() + 4);
    raw.put(payload).putInt(42).flip();

    assertThat(new MapExtensionReader(raw).parse()).isEqualTo(ImmutableMap.of("k", "v"));
  }

  @Test(groups = "unit")
  public void should_expose_scylla_map_extensions_in_table_options() {
    Map<String, ByteBuffer> extensions =
        ImmutableMap.of(
            "scylla_tags", encode("owner", "team-a"),
            "cdc", encode("enabled", "true"),
            "scylla_encryption_options", encode("cipher_algorithm", "AES/CBC/PKCS5Padding"),
            "unrelated", ByteBuffer.wrap(new byte[] {1, 2, 3}));

    TableOptionsMetadata options =
        new TableOptionsMetadata(
            rowWithExtensions(extensions), false, VersionNumber.parse("3.0.0"));

    assertThat(options.getMapExtensions())
        .isEqualTo(
            ImmutableMap.of(
                "cdc", ImmutableMap.of("enabled", "true"),
                "scylla_encryption_options",
                    ImmutableMap.of("cipher_algorithm", "AES/CBC/PKCS5Padding"),
                "scylla_tags", ImmutableMap.of("owner", "team-a")));
    assertThat(options.getExtensions()).hasSize(4);
    assertThat(options.isScyllaCDC()).isTrue();
    assertThat(options.getScyllaCDCOptions()).isEqualTo(ImmutableMap.of("enabled", "true"));
    assertThat(options.getScyllaEncryptionOptions())
        .isEqualTo(ImmutableMap.of("cipher_algorithm", "AES/CBC/PKCS5Padding"));
    assertThat(options.getScyllaAlternatorTags()).isEqualTo(ImmutableMap.of("owner", "team-a"));
  }

  @Test(groups = "unit")
  public void should_not_report_cdc_without_cdc_extension() {
    TableOptionsMetadata options =
        new TableOptionsMetadata(
            rowWithExtensions(Collections.<String, ByteBuffer>emptyMap()),
            false,
            VersionNumber.parse("3.0.0"));

    assertThat(options.getMapExtensions()).isEmpty();
    assertThat(options.isScyllaCDC()).isFalse();
    assertThat(options.getScyllaCDCOptions()).isNull();
    assertThat(options.getScyllaEncryptionOptions()).isNull();
    assertThat(options.getScyllaAlternatorTags()).isNull();
  }

  @Test(groups = "unit")
  public void should_skip_malformed_map_extension_in_table_options() {
    Map<String, ByteBuffer> extensions = ImmutableMap.of("cdc", ints(1, 10));

    TableOptionsMetadata options =
        new TableOptionsMetadata(
            rowWithExtensions(extensions), false, VersionNumber.parse("3.0.0"));

    assertThat(options.getMapExtensions()).isEmpty();
    assertThat(options.getExtensions()).isEqualTo(extensions);
    assertThat(options.isScyllaCDC()).isFalse();
    assertThat(options.getScyllaCDCOptions()).isNull();
  }

  @Test(groups = "unit")
  public void should_keep_valid_map_extensions_when_one_is_malformed() {
    Map<String, ByteBuffer> extensions =
        ImmutableMap.of(
            "cdc", ints(1, 10),
            "scylla_tags", encode("owner", "team-a"));

    TableOptionsMetadata options =
        new TableOptionsMetadata(
            rowWithExtensions(extensions), false, VersionNumber.parse("3.0.0"));

    assertThat(options.getMapExtensions())
        .isEqualTo(ImmutableMap.of("scylla_tags", ImmutableMap.of("owner", "team-a")));
    assertThat(options.getExtensions()).isEqualTo(extensions);
    assertThat(options.isScyllaCDC()).isFalse();
    assertThat(options.getScyllaCDCOptions()).isNull();
    assertThat(options.getScyllaAlternatorTags()).isEqualTo(ImmutableMap.of("owner", "team-a"));
  }

  @Test(groups = "unit")
  public void should_skip_duplicate_key_map_extension_in_table_options() {
    Map<String, ByteBuffer> extensions =
        ImmutableMap.of("cdc", encode("enabled", "true", "enabled", "false"));

    TableOptionsMetadata options =
        new TableOptionsMetadata(
            rowWithExtensions(extensions), false, VersionNumber.parse("3.0.0"));

    assertThat(options.getMapExtensions()).isEmpty();
    assertThat(options.getExtensions()).isEqualTo(extensions);
    assertThat(options.isScyllaCDC()).isFalse();
  }

  private static Row rowWithExtensions(Map<String, ByteBuffer> extensions) {
    Row row = mock(Row.class);
    when(row.getColumnDefinitions()).thenReturn(ColumnDefinitions.EMPTY);
    when(row.getMap(anyString(), eq(String.class), eq(String.class)))
        .thenReturn(Collections.<String, String>emptyMap());
    when(row.getMap("extensions", String.class, ByteBuffer.class)).thenReturn(extensions);
    return row;
  }

  private static ByteBuffer encode(String... keysAndValues) {
    int size = 4;
    for (String s : keysAndValues) size += 4 + s.getBytes(UTF_8).length;
    ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(keysAndValues.length / 2);
    for (String s : keysAndValues) {
      byte[] bytes = s.getBytes(UTF_8);
      buffer.putInt(bytes.length).put(bytes);
    }
    buffer.flip();
    return buffer.order(ByteOrder.BIG_ENDIAN);
  }

  private static ByteBuffer ints(int... values) {
    ByteBuffer buffer = ByteBuffer.allocate(4 * values.length).order(ByteOrder.LITTLE_ENDIAN);
    for (int v : values) buffer.putInt(v);
    buffer.flip();
    return buffer.order(ByteOrder.BIG_ENDIAN);
  }
}
