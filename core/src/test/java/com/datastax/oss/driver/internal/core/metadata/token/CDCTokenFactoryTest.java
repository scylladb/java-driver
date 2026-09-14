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
package com.datastax.oss.driver.internal.core.metadata.token;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.LoggerFactory;

@RunWith(MockitoJUnitRunner.class)
public class CDCTokenFactoryTest {

  private static final long UPPER_DWORD = 0x0123456789ABCDEFL;

  /** Stream-id bits above the version nibble, as a real CDC key carries them. */
  private static final long LOWER_DWORD_HIGH_BITS = 0x0FEDCBA987654320L;

  private final CDCTokenFactory factory = new CDCTokenFactory();

  @Mock private Appender<ILoggingEvent> appender;
  @Captor private ArgumentCaptor<ILoggingEvent> loggingEventCaptor;

  private Logger logger;
  private Level initialLogLevel;

  @Before
  public void setup() {
    logger = (Logger) LoggerFactory.getLogger(CDCTokenFactory.class);
    initialLogLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    logger.addAppender(appender);
  }

  @After
  public void teardown() {
    logger.detachAppender(appender);
    logger.setLevel(initialLogLevel);
  }

  @Test
  public void should_expose_scylla_cdc_partitioner_name() {
    assertThat(factory.getPartitionerName()).isEqualTo("com.scylladb.dht.CDCPartitioner");
  }

  @Test
  public void should_hash_well_formed_key_to_its_upper_dword() {
    // TokenLong64.equals compares any two TokenLong64s by value, so pin the type as well
    assertThat(factory.hash(cdcKey(UPPER_DWORD, 1)))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(new CDCToken(UPPER_DWORD));

    verifyNoInteractions(appender);
  }

  @Test
  public void should_hash_key_shorter_than_eight_bytes_to_min_token() {
    // TokenLong64.equals compares any two TokenLong64s by value, so pin the type as well
    assertThat(factory.hash(ByteBuffer.allocate(4)))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(CDCTokenFactory.MIN_TOKEN);
    assertThat(factory.hash(ByteBuffer.allocate(0)))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(CDCTokenFactory.MIN_TOKEN);
    // The length complaint is emitted before the short-key guard, so both of these warn
    assertLogContains(Level.WARN, "CDC partition key has invalid length", 2);
  }

  @Test
  public void should_warn_and_use_upper_dword_when_key_length_is_wrong() {
    // 8 bytes: long enough to read the upper dword, but not the expected 16
    ByteBuffer shortKey = ByteBuffer.allocate(8);
    shortKey.putLong(0, UPPER_DWORD);

    // TokenLong64.equals compares any two TokenLong64s by value, so pin the type as well
    assertThat(factory.hash(shortKey))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(new CDCToken(UPPER_DWORD));
    assertLogContains(Level.WARN, "CDC partition key has invalid length", 1);
  }

  @Test
  public void should_warn_when_key_version_is_too_old() {
    // Version lives in the low nibble of the lower dword; only version 1 is supported
    assertThat(factory.hash(cdcKey(UPPER_DWORD, 0)))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(new CDCToken(UPPER_DWORD));
    assertLogContains(Level.WARN, "CDC partition key version 0 is not supported", 1);
  }

  @Test
  public void should_warn_when_key_version_is_too_new() {
    assertThat(factory.hash(cdcKey(UPPER_DWORD, 2)))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(new CDCToken(UPPER_DWORD));
    assertLogContains(Level.WARN, "CDC partition key version 2 is not supported", 1);
  }

  @Test
  public void should_hash_from_the_buffers_current_position() {
    // hash() reads position() into an offset and then indexes absolutely, so a buffer that has
    // already been partially consumed must still hash to the same token.
    ByteBuffer prefixed = ByteBuffer.allocate(4 + 16);
    prefixed.putInt(0xDEADBEEF);
    prefixed.put(cdcKey(UPPER_DWORD, 1));
    prefixed.position(4);

    assertThat(factory.hash(prefixed))
        .isInstanceOf(CDCToken.class)
        .isEqualTo(new CDCToken(UPPER_DWORD));
    // Only the bytes after position() count, so the prefix must not trip the length warning
    verifyNoInteractions(appender);
  }

  @Test
  public void should_parse_and_format_token() {
    Token token = factory.parse("-42");

    assertThat(token).isEqualTo(new CDCToken(-42L));
    assertThat(factory.format(token)).isEqualTo("-42");
  }

  @Test
  public void should_fail_to_format_non_cdc_token() {
    assertThatThrownBy(() -> factory.format(new Murmur3Token(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can only format CDCToken instances");
  }

  @Test
  public void should_expose_min_token() {
    assertThat(factory.minToken()).isEqualTo(CDCTokenFactory.MIN_TOKEN);
    assertThat(((CDCToken) factory.minToken()).getValue()).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  public void should_build_cdc_token_range() {
    TokenRange range = factory.range(new CDCToken(1), new CDCToken(2));

    assertThat(range).isInstanceOf(CDCTokenRange.class);
    assertThat(range.getStart()).isEqualTo(new CDCToken(1));
    assertThat(range.getEnd()).isEqualTo(new CDCToken(2));
  }

  @Test
  public void should_fail_to_build_range_from_non_cdc_tokens() {
    assertThatThrownBy(() -> factory.range(new Murmur3Token(1), new CDCToken(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can only build ranges of CDCToken instances");
    assertThatThrownBy(() -> factory.range(new CDCToken(1), new Murmur3Token(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can only build ranges of CDCToken instances");
  }

  @Test
  public void should_print_value_in_to_string() {
    assertThat(new CDCToken(42).toString()).isEqualTo("CDCToken(42)");
  }

  /**
   * Builds a 16-byte CDC partition key: the upper dword, then a lower dword whose low nibble is the
   * version.
   */
  private static ByteBuffer cdcKey(long upperDword, long version) {
    ByteBuffer key = ByteBuffer.allocate(16);
    key.putLong(0, upperDword);
    key.putLong(8, LOWER_DWORD_HIGH_BITS | version);
    return key;
  }

  /** Counts the matching events: a test that warns twice must not pass on one warning. */
  private void assertLogContains(Level level, String message, int expectedCount) {
    verify(appender, atLeast(1)).doAppend(loggingEventCaptor.capture());
    assertThat(loggingEventCaptor.getAllValues())
        .filteredOn(event -> event.getLevel().equals(level))
        .extracting(ILoggingEvent::getFormattedMessage)
        .filteredOn(formatted -> formatted.contains(message))
        .hasSize(expectedCount);
  }
}
