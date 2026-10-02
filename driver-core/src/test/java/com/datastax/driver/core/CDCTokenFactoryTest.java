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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CDCTokenFactoryTest {

  private static final String LOGGER_NAME =
      "com.datastax.driver.core.Token$CDCToken$CDCTokenFactory";
  private static final String INVALID_LENGTH = "CDC partition key has invalid length";
  private static final String UNSUPPORTED_VERSION = "is not supported";

  private final Token.Factory factory = Token.CDCToken.FACTORY;

  private MemoryAppender appender;
  private Logger logger;
  private Level originalLevel;

  @BeforeMethod(groups = "unit")
  public void startCapturingLogs() {
    logger = Logger.getLogger(LOGGER_NAME);
    originalLevel = logger.getLevel();
    logger.setLevel(Level.WARN);
    appender = new MemoryAppender();
    logger.addAppender(appender);
  }

  @AfterMethod(groups = "unit", alwaysRun = true)
  public void stopCapturingLogs() {
    logger.removeAppender(appender);
    logger.setLevel(originalLevel);
  }

  @Test(groups = "unit")
  public void should_resolve_factory_from_partitioner_name() {
    assertThat(Token.getFactory("com.scylladb.dht.CDCPartitioner")).isSameAs(factory);
    assertThat(Token.getFactory("CDCPartitioner")).isSameAs(factory);
  }

  @Test(groups = "unit")
  public void should_hash_versioned_key_to_its_upper_dword() {
    ByteBuffer key = cdcKey(0x0123456789ABCDEFL, 0xFFFFFFFFFFFFFFF1L);

    Token token = factory.hash(key);

    assertCdcToken(token, 0x0123456789ABCDEFL);
    assertThat(key.position()).isEqualTo(0);
    assertThat(key.remaining()).isEqualTo(16);
    assertThat(appender.get()).isEmpty();
  }

  @Test(groups = "unit")
  public void should_hash_key_from_buffer_position() {
    ByteBuffer key = ByteBuffer.allocate(19);
    key.put(new byte[] {9, 9, 9}).putLong(-42L).putLong(1L);
    key.position(3);

    Token token = factory.hash(key);

    assertCdcToken(token, -42L);
    assertThat(key.position()).isEqualTo(3);
    assertThat(appender.get()).isEmpty();
  }

  @Test(groups = "unit")
  public void should_warn_on_unsupported_version_and_still_hash() {
    assertCdcToken(factory.hash(cdcKey(7L, 0L)), 7L);
    assertCdcToken(factory.hash(cdcKey(8L, 2L)), 8L);
    assertCdcToken(factory.hash(cdcKey(9L, 0x1FL)), 9L);

    String logs = appender.get();
    assertThat(countOccurrences(logs, UNSUPPORTED_VERSION)).isEqualTo(3);
    assertThat(logs)
        .contains("CDC partition key version 0 is not supported")
        .contains("CDC partition key version 2 is not supported")
        .contains("CDC partition key version 15 is not supported");
    assertThat(logs).doesNotContain(INVALID_LENGTH);
  }

  @Test(groups = "unit")
  public void should_hash_short_key_to_its_first_eight_bytes_without_version_check() {
    ByteBuffer key = ByteBuffer.allocate(12);
    key.putLong(123L).putInt(0);
    key.flip();

    // Pins current behaviour (#1191): the server routes non-16-byte keys to the minimum token
    Token token = factory.hash(key);

    assertCdcToken(token, 123L);
    assertThat(key.position()).isEqualTo(0);
    String logs = appender.get();
    assertThat(countOccurrences(logs, INVALID_LENGTH)).isEqualTo(1);
    assertThat(logs).contains("expected 16 bytes, but got 12 bytes");
    assertThat(logs).doesNotContain(UNSUPPORTED_VERSION);
  }

  @Test(groups = "unit")
  public void should_hash_long_key_to_its_first_eight_bytes_without_version_check() {
    ByteBuffer key = ByteBuffer.allocate(24);
    key.putLong(-5L).putLong(0L).putLong(0L);
    key.flip();

    // Pins current behaviour (#1191): the server routes non-16-byte keys to the minimum token
    assertCdcToken(factory.hash(key), -5L);
    String logs = appender.get();
    assertThat(countOccurrences(logs, INVALID_LENGTH)).isEqualTo(1);
    assertThat(logs).contains("but got 24 bytes");
    assertThat(logs).doesNotContain(UNSUPPORTED_VERSION);
  }

  @Test(groups = "unit")
  public void should_hash_key_of_exactly_eight_bytes() {
    ByteBuffer key = ByteBuffer.allocate(8);
    key.putLong(77L);
    key.flip();

    // Pins current behaviour (#1191): the server routes non-16-byte keys to the minimum token
    assertCdcToken(factory.hash(key), 77L);
    assertThat(countOccurrences(appender.get(), INVALID_LENGTH)).isEqualTo(1);
  }

  @Test(groups = "unit")
  public void should_hash_key_shorter_than_eight_bytes_to_min_token() {
    Token seven = factory.hash(ByteBuffer.allocate(7));
    Token empty = factory.hash(ByteBuffer.allocate(0));

    assertThat(seven).isSameAs(factory.minToken());
    assertThat(empty).isSameAs(factory.minToken());
    assertCdcToken(seven, Long.MIN_VALUE);
    String logs = appender.get();
    assertThat(countOccurrences(logs, INVALID_LENGTH)).isEqualTo(2);
    assertThat(logs).contains("but got 7 bytes").contains("but got 0 bytes");
  }

  @Test(groups = "unit")
  public void should_create_tokens_from_string_and_bytes() {
    Token token = factory.fromString("-9223372036854775807");
    assertCdcToken(token, -9223372036854775807L);

    ByteBuffer serialized = token.serialize(ProtocolVersion.V4);
    assertThat(serialized.remaining()).isEqualTo(8);
    assertThat(serialized.getLong(serialized.position())).isEqualTo(-9223372036854775807L);

    Token deserialized = factory.deserialize(serialized, ProtocolVersion.V4);
    assertCdcToken(deserialized, -9223372036854775807L);
  }

  @Test(groups = "unit", expectedExceptions = NumberFormatException.class)
  public void should_reject_non_numeric_token_string() {
    factory.fromString("not-a-token");
  }

  @Test(groups = "unit")
  public void should_expose_bigint_type_and_long_value() {
    Token token = factory.fromString("42");

    assertThat(factory.getTokenType()).isEqualTo(DataType.bigint());
    assertThat(token.getType()).isEqualTo(DataType.bigint());
    assertThat(token.getValue()).isEqualTo(42L);
    assertThat(token.toString()).isEqualTo("42");
    assertThat(factory.fromString("-1").toString()).isEqualTo("-1");
  }

  @Test(groups = "unit")
  public void should_hash_like_a_long() {
    long[] values = {0L, 42L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x0000000100000000L};
    for (long value : values) {
      Token token = factory.fromString(Long.toString(value));
      assertThat(token.hashCode()).isEqualTo(Long.valueOf(value).hashCode());
      assertThat(token).isEqualTo(factory.fromString(Long.toString(value)));
    }
  }

  @Test(groups = "unit")
  public void should_order_tokens_by_value() {
    Token min = factory.minToken();
    Token zero = factory.fromString("0");

    assertCdcToken(min, Long.MIN_VALUE);
    assertThat(min.compareTo(zero)).isLessThan(0);
    assertThat(zero.compareTo(min)).isGreaterThan(0);
    assertThat(zero.compareTo(factory.fromString("0"))).isEqualTo(0);
  }

  @Test(groups = "unit")
  public void should_treat_cdc_and_murmur3_tokens_as_the_same_ring_position() {
    Token cdc = factory.fromString("42");
    Token murmur3 = Token.M3PToken.FACTORY.fromString("42");

    // CDC tokens are looked up on the cluster's Murmur3 ring (one TokenMap), so they must
    // compare, equal and hash like Murmur3 tokens of the same value
    assertThat(cdc.equals(murmur3)).isTrue();
    assertThat(murmur3.equals(cdc)).isTrue();
    assertThat(cdc.hashCode()).isEqualTo(murmur3.hashCode());
    assertThat(cdc.compareTo(murmur3)).isEqualTo(0);
    assertThat(murmur3.compareTo(cdc)).isEqualTo(0);
    assertThat(cdc.compareTo(Token.M3PToken.FACTORY.fromString("43"))).isLessThan(0);
  }

  @Test(groups = "unit")
  public void should_split_range() {
    List<Token> splits =
        factory.split(
            factory.fromString("-9223372036854775808"),
            factory.fromString("4611686018427387904"),
            3);
    assertCdcTokens(splits, -4611686018427387904L, 0L);
  }

  @Test(groups = "unit")
  public void should_split_range_that_wraps_around_the_ring() {
    List<Token> splits =
        factory.split(factory.fromString("4611686018427387904"), factory.fromString("0"), 3);
    assertCdcTokens(splits, -9223372036854775807L, -4611686018427387903L);
  }

  @Test(groups = "unit")
  public void should_split_range_when_division_not_integral() {
    List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("11"), 3);
    assertCdcTokens(splits, 4L, 8L);
  }

  @Test(groups = "unit")
  public void should_split_range_producing_empty_splits() {
    List<Token> splits = factory.split(factory.fromString("0"), factory.fromString("2"), 5);
    assertCdcTokens(splits, 1L, 2L, 2L, 2L);
  }

  @Test(groups = "unit")
  public void should_split_range_producing_empty_splits_near_ring_end() {
    Token minToken = factory.fromString("-9223372036854775808");
    Token maxToken = factory.fromString("9223372036854775807");

    assertCdcTokens(factory.split(maxToken, minToken, 3), Long.MAX_VALUE, Long.MAX_VALUE);
    assertCdcTokens(
        factory.split(minToken, factory.fromString("-9223372036854775807"), 3),
        -9223372036854775807L,
        -9223372036854775807L);
  }

  @Test(groups = "unit")
  public void should_split_whole_ring() {
    List<Token> splits = factory.split(factory.minToken(), factory.minToken(), 3);
    assertCdcTokens(splits, -3074457345618258603L, 3074457345618258602L);
  }

  @Test(groups = "unit")
  public void should_split_empty_range_into_its_start() {
    Token start = factory.fromString("10");
    assertCdcTokens(factory.split(start, start, 3), 10L, 10L);
  }

  @Test(groups = "unit")
  public void should_return_no_splits_when_asked_for_one() {
    assertThat(factory.split(factory.fromString("0"), factory.fromString("100"), 1)).isEmpty();
  }

  private static ByteBuffer cdcKey(long upper, long lower) {
    ByteBuffer key = ByteBuffer.allocate(16);
    key.putLong(upper).putLong(lower);
    key.flip();
    return key;
  }

  // CDC tokens equal Murmur3 tokens of the same value, so every check must also pin the class.
  private static void assertCdcToken(Token token, long expectedValue) {
    assertThat(token).isNotNull();
    assertThat(token.getClass()).isEqualTo(Token.CDCToken.class);
    assertThat(token.getValue()).isEqualTo(expectedValue);
  }

  private static void assertCdcTokens(List<Token> tokens, long... expectedValues) {
    assertThat(tokens).hasSize(expectedValues.length);
    for (int i = 0; i < expectedValues.length; i++) {
      assertCdcToken(tokens.get(i), expectedValues[i]);
    }
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int from = 0;
    while ((from = haystack.indexOf(needle, from)) >= 0) {
      count++;
      from += needle.length();
    }
    return count;
  }
}
