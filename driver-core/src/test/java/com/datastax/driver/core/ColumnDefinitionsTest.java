/*
 * Copyright DataStax, Inc.
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Locale;
import org.testng.annotations.Test;

public class ColumnDefinitionsTest {

  @Test(groups = "unit")
  public void caseTest() {

    ColumnDefinitions defs;

    defs =
        new ColumnDefinitions(
            new ColumnDefinitions.Definition[] {
              new ColumnDefinitions.Definition("ks", "cf", "aColumn", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "fOO", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "anotherColumn", DataType.text())
            },
            CodecRegistry.DEFAULT_INSTANCE);

    assertTrue(defs.contains("foo"));
    assertTrue(defs.contains("fOO"));
    assertTrue(defs.contains("FOO"));

    defs =
        new ColumnDefinitions(
            new ColumnDefinitions.Definition[] {
              new ColumnDefinitions.Definition("ks", "cf", "aColumn", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "foo", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "anotherColumn", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "FOO", DataType.cint()),
              new ColumnDefinitions.Definition("ks", "cf", "with \" quote", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "\"in quote\"", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf", "in quote", DataType.cint()),
            },
            CodecRegistry.DEFAULT_INSTANCE);

    assertTrue(defs.getType("foo").equals(DataType.text()));
    assertTrue(defs.getType("Foo").equals(DataType.text()));
    assertTrue(defs.getType("FOO").equals(DataType.text()));
    assertTrue(defs.getType("\"FOO\"").equals(DataType.cint()));

    assertTrue(defs.contains("with \" quote"));

    assertTrue(defs.getType("in quote").equals(DataType.cint()));
    assertTrue(defs.getType("\"in quote\"").equals(DataType.cint()));
    assertTrue(defs.getType("\"\"in quote\"\"").equals(DataType.text()));
  }

  @Test(groups = "unit")
  public void multiDefinitionTest() {

    ColumnDefinitions defs =
        new ColumnDefinitions(
            new ColumnDefinitions.Definition[] {
              new ColumnDefinitions.Definition("ks", "cf1", "column", DataType.text()),
              new ColumnDefinitions.Definition("ks", "cf2", "column", DataType.cint()),
              new ColumnDefinitions.Definition("ks", "cf3", "column", DataType.cfloat())
            },
            CodecRegistry.DEFAULT_INSTANCE);

    assertTrue(defs.getType("column").equals(DataType.text()));
  }

  /**
   * A hand-built lookup fixture, not a real server response: a server names the marker of an IN
   * relation after the operator and the column, as in "SELECT * FROM t WHERE pk = ? AND v IN ?",
   * and the definition is repeated here so that a name matching more than one variable is
   * reachable. (No server would send both at once — CQL rejects a column restricted by two IN
   * relations.)
   *
   * <p>The synthesized spelling differs between ScyllaDB release lines rather than along a single
   * version sequence: 2024.1 emits in(v), 2026.1.8 emits IN(v), and the lowercase spelling is
   * restored in 2026.1.12 and 2026.2.6 (CUSTOMER-583 / SCYLLADB-3454). An application must
   * therefore not depend on either spelling.
   */
  private static ColumnDefinitions synthesizedInMarkerDefinitions() {
    return new ColumnDefinitions(
        new ColumnDefinitions.Definition[] {
          new ColumnDefinitions.Definition("ks", "cf", "pk", DataType.cint()),
          new ColumnDefinitions.Definition("ks", "cf", "IN(v)", DataType.list(DataType.cint())),
          new ColumnDefinitions.Definition("ks", "cf", "IN(v)", DataType.list(DataType.cint())),
        },
        CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test(groups = "unit")
  public void synthesizedMarkerNameIsMatchedWhateverTheServerSpelling() {
    ColumnDefinitions defs = synthesizedInMarkerDefinitions();

    assertTrue(defs.contains("IN(v)"));
    assertTrue(defs.contains("in(v)"));
    assertTrue(defs.contains("In(V)"));
    assertEquals(defs.getFirstIdx("in(v)"), 1);
  }

  /**
   * The letter that flipped in CUSTOMER-583 is {@code I}, and lowercasing it in the Turkish locale
   * yields a dotless {@code ı}. Matching must not depend on the JVM's default locale, or a Turkish
   * deployment would fail to resolve the name that works everywhere else.
   */
  @Test(groups = "unit")
  public void synthesizedMarkerNameIsMatchedInAnyDefaultLocale() {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      ColumnDefinitions defs = synthesizedInMarkerDefinitions();
      // Probe both spellings. The definitions are built inside the locale override, so the
      // lowercase probe covers the fold applied while indexing; but "in(v)" is left alone by every
      // locale, so it would not catch a lookup that stopped pinning ROOT — the uppercase probe
      // covers that side.
      assertTrue(defs.contains("in(v)"));
      assertEquals(defs.getFirstIdx("in(v)"), 1);
      assertTrue(defs.contains("IN(v)"));
      assertEquals(defs.getFirstIdx("IN(v)"), 1);
    } finally {
      Locale.setDefault(def);
    }
  }

  /** A named setter writes every matching variable, so repeating a column makes names ambiguous. */
  @Test(groups = "unit")
  public void synthesizedMarkerNameMatchesEveryOccurrence() {
    assertEquals(synthesizedInMarkerDefinitions().getAllIdx("in(v)"), new int[] {1, 2});
  }

  /**
   * Double-quoting opts into exact matching, which the synthesized spelling can then break. A name
   * that survives the case-insensitive lookup but no exact comparison must be reported absent, the
   * same way an unquoted name that matches nothing is — otherwise contains() claims the name is
   * there, getIndexOf() throws instead of returning -1, and a setter silently leaves the variable
   * unset, which the server then rejects with "Unexpected unset value for bind variable N".
   */
  @Test(groups = "unit")
  public void doubleQuotedSynthesizedMarkerNameOfDifferentCaseIsNotMatched() {
    ColumnDefinitions defs = synthesizedInMarkerDefinitions();

    assertTrue(defs.contains("\"IN(v)\""));
    assertEquals(defs.getIndexOf("\"IN(v)\""), 1);

    assertFalse(defs.contains("\"in(v)\""));
    assertEquals(defs.getIndexOf("\"in(v)\""), -1);
    try {
      defs.getType("\"in(v)\"");
      fail("expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
