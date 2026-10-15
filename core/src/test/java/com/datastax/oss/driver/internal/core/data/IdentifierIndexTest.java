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
package com.datastax.oss.driver.internal.core.data;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class IdentifierIndexTest {
  private static final CqlIdentifier Foo = CqlIdentifier.fromInternal("Foo");
  private static final CqlIdentifier foo = CqlIdentifier.fromInternal("foo");
  private static final CqlIdentifier fOO = CqlIdentifier.fromInternal("fOO");
  private IdentifierIndex index =
      new IdentifierIndex(ImmutableList.of(Foo, foo, fOO, Foo, foo, fOO));

  // The variable definitions a server returns for
  // "SELECT * FROM t WHERE pk = ? AND ck IN ? AND ck IN ?": the markers of the IN relations get a
  // name synthesized from the operator and the column, so repeating the column yields the same name
  // twice. That spelling differs between ScyllaDB release lines rather than along a single version
  // sequence: 2024.1 emits in(ck), 2026.1.8 emits IN(ck), and the lowercase spelling is restored in
  // 2026.1.12 and 2026.2.6 (CUSTOMER-583 / SCYLLADB-3454). An application must therefore not depend
  // on either spelling. See BoundStatementCcmIT for the end-to-end counterpart of the tests below.
  private static final CqlIdentifier pk = CqlIdentifier.fromInternal("pk");
  private static final CqlIdentifier upperCaseInCk = CqlIdentifier.fromInternal("IN(ck)");

  /**
   * Built on demand rather than kept in a field: the constructor folds the names it indexes, so the
   * locale test below has to build the index inside its own locale override to cover that half of
   * the fold. A field initializer would run before the override and leave the indexing side
   * untested.
   */
  private static IdentifierIndex synthesizedIndex() {
    return new IdentifierIndex(ImmutableList.of(pk, upperCaseInCk, upperCaseInCk));
  }

  @Test
  public void should_find_first_index_of_existing_identifier() {
    assertThat(index.firstIndexOf(Foo)).isEqualTo(0);
    assertThat(index.firstIndexOf(foo)).isEqualTo(1);
    assertThat(index.firstIndexOf(fOO)).isEqualTo(2);
  }

  @Test
  public void should_not_find_index_of_nonexistent_identifier() {
    assertThat(index.firstIndexOf(CqlIdentifier.fromInternal("FOO"))).isEqualTo(-1);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_find_first_index_of_case_insensitive_name(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(index.firstIndexOf("foo")).isEqualTo(0);
      assertThat(index.firstIndexOf("FOO")).isEqualTo(0);
      assertThat(index.firstIndexOf("fOO")).isEqualTo(0);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_not_find_first_index_of_nonexistent_case_insensitive_name(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(index.firstIndexOf("bar")).isEqualTo(-1);
      assertThat(index.firstIndexOf("BAR")).isEqualTo(-1);
      assertThat(index.firstIndexOf("bAR")).isEqualTo(-1);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  public void should_find_first_index_of_case_sensitive_name() {
    assertThat(index.firstIndexOf("\"Foo\"")).isEqualTo(0);
    assertThat(index.firstIndexOf("\"foo\"")).isEqualTo(1);
    assertThat(index.firstIndexOf("\"fOO\"")).isEqualTo(2);
  }

  @Test
  public void should_not_find_index_of_nonexistent_case_sensitive_name() {
    assertThat(index.firstIndexOf("\"FOO\"")).isEqualTo(-1);
  }

  @Test
  public void should_find_all_indices_of_existing_identifier() {
    assertThat(index.allIndicesOf(Foo)).containsExactly(0, 3);
    assertThat(index.allIndicesOf(foo)).containsExactly(1, 4);
    assertThat(index.allIndicesOf(fOO)).containsExactly(2, 5);
  }

  @Test
  public void should_not_find_indices_of_nonexistent_identifier() {
    assertThat(index.allIndicesOf(CqlIdentifier.fromInternal("FOO"))).isEmpty();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_find_all_indices_of_case_insensitive_name(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(index.allIndicesOf("foo")).containsExactly(0, 1, 2, 3, 4, 5);
      assertThat(index.allIndicesOf("FOO")).containsExactly(0, 1, 2, 3, 4, 5);
      assertThat(index.allIndicesOf("fOO")).containsExactly(0, 1, 2, 3, 4, 5);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_not_find_indices_of_nonexistent_case_insensitive_name(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(index.allIndicesOf("bar")).isEmpty();
      assertThat(index.allIndicesOf("BAR")).isEmpty();
      assertThat(index.allIndicesOf("bAR")).isEmpty();
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  public void should_find_all_indices_of_case_sensitive_name() {
    assertThat(index.allIndicesOf("\"Foo\"")).containsExactly(0, 3);
    assertThat(index.allIndicesOf("\"foo\"")).containsExactly(1, 4);
    assertThat(index.allIndicesOf("\"fOO\"")).containsExactly(2, 5);
  }

  @Test
  public void should_not_find_indices_of_nonexistent_case_sensitive_name() {
    assertThat(index.allIndicesOf("\"FOO\"")).isEmpty();
  }

  /**
   * The regression guard for CUSTOMER-583: the driver resolves bind-variable names locally, and
   * that lookup must not care how the server spelled a synthesized name. The locales provider
   * matters here rather than incidentally: the letter that flipped is {@code I}, and lowercasing it
   * in the Turkish locale yields a dotless {@code ı}, so a lookup that did not pin {@link
   * Locale#ROOT} would fail for Turkish users only.
   */
  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_find_synthesized_marker_name_whatever_the_server_spelling(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      // Built inside the override deliberately, so that the fold the constructor applies to the
      // names it indexes is exercised under this locale too, not just the fold on the lookup side.
      IdentifierIndex synthesized = synthesizedIndex();
      assertThat(synthesized.firstIndexOf("IN(ck)")).isEqualTo(1);
      assertThat(synthesized.firstIndexOf("in(ck)")).isEqualTo(1);
      assertThat(synthesized.firstIndexOf("In(Ck)")).isEqualTo(1);
    } finally {
      Locale.setDefault(def);
    }
  }

  /** A named setter writes every matching variable, so repeating a column makes names ambiguous. */
  @Test
  public void should_find_all_indices_of_synthesized_marker_name_when_column_is_repeated() {
    assertThat(synthesizedIndex().allIndicesOf("in(ck)")).containsExactly(1, 2);
  }

  /** Double-quoting opts into exact matching, which the synthesized spelling can then break. */
  @Test
  public void should_match_double_quoted_synthesized_marker_name_exactly() {
    IdentifierIndex synthesized = synthesizedIndex();
    assertThat(synthesized.firstIndexOf("\"IN(ck)\"")).isEqualTo(1);
    assertThat(synthesized.firstIndexOf("\"in(ck)\"")).isEqualTo(-1);
  }

  /** Same for the identifier-based lookup, which is always exact. */
  @Test
  public void should_match_synthesized_marker_identifier_exactly() {
    IdentifierIndex synthesized = synthesizedIndex();
    assertThat(synthesized.firstIndexOf(CqlIdentifier.fromInternal("IN(ck)"))).isEqualTo(1);
    assertThat(synthesized.firstIndexOf(CqlIdentifier.fromInternal("in(ck)"))).isEqualTo(-1);
  }
}
