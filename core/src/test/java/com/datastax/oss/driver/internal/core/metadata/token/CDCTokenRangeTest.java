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

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import java.util.List;
import org.junit.Test;

/** @see TokenRangeTest */
public class CDCTokenRangeTest {

  private static final long MIN = -9223372036854775808L;
  private static final long MAX = 9223372036854775807L;

  @Test
  public void should_split_range() {
    assertThat(splitEvenly(range(MIN, 4611686018427387904L), 3))
        .containsExactly(
            range(MIN, -4611686018427387904L),
            range(-4611686018427387904L, 0),
            range(0, 4611686018427387904L));
  }

  @Test
  public void should_split_range_that_wraps_around_the_ring() {
    assertThat(splitEvenly(range(4611686018427387904L, 0), 3))
        .containsExactly(
            range(4611686018427387904L, -9223372036854775807L),
            range(-9223372036854775807L, -4611686018427387903L),
            range(-4611686018427387903L, 0));
  }

  @Test
  public void should_split_range_when_division_not_integral() {
    assertThat(splitEvenly(range(0, 11), 3))
        .containsExactly(range(0, 4), range(4, 8), range(8, 11));
  }

  @Test
  public void should_split_range_producing_empty_splits() {
    assertThat(splitEvenly(range(0, 2), 5))
        .containsExactly(range(0, 1), range(1, 2), range(2, 2), range(2, 2), range(2, 2));
  }

  @Test
  public void should_split_range_producing_empty_splits_near_ring_end() {
    // Edge cases where we want to make sure we don't accidentally generate the ]min,min] range
    // (which is the whole ring)
    assertThat(splitEvenly(range(MAX, MIN), 3))
        .containsExactly(range(MAX, MAX), range(MAX, MAX), range(MAX, MIN));

    assertThat(splitEvenly(range(MIN, MIN + 1), 3))
        .containsExactly(range(MIN, MIN + 1), range(MIN + 1, MIN + 1), range(MIN + 1, MIN + 1));
  }

  @Test
  public void should_split_whole_ring() {
    // ]min,min] is the whole ring: split() substitutes max for the end token
    assertThat(splitEvenly(range(MIN, MIN), 3))
        .containsExactly(
            range(MIN, -3074457345618258603L),
            range(-3074457345618258603L, 3074457345618258602L),
            range(3074457345618258602L, MIN));
  }

  /**
   * The only use of the minToken handed to super(): unwrap() splits a wrapped range at it and feeds
   * it back through newTokenRange, which casts to CDCToken. Every split test above survives
   * Murmur3TokenFactory.MIN_TOKEN in its place, because TokenLong64.equals compares across
   * subclasses by value.
   */
  @Test
  public void should_unwrap_range_that_wraps_around_the_ring() {
    CDCTokenRange wrapped = range(100, -100);

    assertThat(wrapped).unwrapsTo(range(100, MIN), range(MIN, -100));
    assertThat(wrapped.unwrap())
        .allSatisfy(part -> assertThat(part).isInstanceOf(CDCTokenRange.class));
  }

  /**
   * TokenRangeBase.equals and TokenLong64.equals both compare by value only, so the expected ranges
   * below would also match Murmur3 ones. Pin the types here, once for every case.
   */
  private List<TokenRange> splitEvenly(CDCTokenRange range, int numberOfSplits) {
    List<TokenRange> splits = range.splitEvenly(numberOfSplits);
    assertThat(splits)
        .allSatisfy(
            split -> {
              assertThat(split).isInstanceOf(CDCTokenRange.class);
              assertThat(split.getStart()).isInstanceOf(CDCToken.class);
              assertThat(split.getEnd()).isInstanceOf(CDCToken.class);
            });
    return splits;
  }

  private CDCTokenRange range(long start, long end) {
    return new CDCTokenRange(new CDCToken(start), new CDCToken(end));
  }
}
