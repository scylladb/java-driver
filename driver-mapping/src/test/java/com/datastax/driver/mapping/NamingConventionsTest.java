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
package com.datastax.driver.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Locale;
import org.testng.annotations.Test;

/** Test for JAVA-1316 - test combinations of different {@link NamingConventions} implementation. */
public class NamingConventionsTest {

  // test lower camel case inputs

  @Test(groups = "unit")
  public void lower_camel_case_to_upper_camel_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.UPPER_CAMEL_CASE,
        "myXmlParser",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void lower_camel_case_to_lower_snake_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.LOWER_SNAKE_CASE,
        "myXmlParser",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void lower_camel_case_to_upper_snake_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.UPPER_SNAKE_CASE,
        "myXmlParser",
        "MY_XML_PARSER");
  }

  @Test(groups = "unit")
  public void lower_camel_case_to_lower_lisp_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.LOWER_LISP_CASE,
        "myXmlParser",
        "my-xml-parser");
  }

  @Test(groups = "unit")
  public void lower_camel_case_to_upper_lisp_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.UPPER_LISP_CASE,
        "myXmlParser",
        "MY-XML-PARSER");
  }

  @Test(groups = "unit")
  public void lower_camel_case_to_lower_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.LOWER_CASE,
        "myXmlParser",
        "myxmlparser");
  }

  @Test(groups = "unit")
  public void lower_camel_case_to_upper_case() {
    test(
        NamingConventions.LOWER_CAMEL_CASE,
        NamingConventions.UPPER_CASE,
        "myXmlParser",
        "MYXMLPARSER");
  }

  // test upper camel case inputs

  @Test(groups = "unit")
  public void upper_camel_case_to_lower_camel_case() {
    test(
        NamingConventions.UPPER_CAMEL_CASE,
        NamingConventions.LOWER_CAMEL_CASE,
        "MyXmlParser",
        "myXmlParser");
  }

  @Test(groups = "unit")
  public void upper_camel_case_to_lower_snake_case() {
    test(
        NamingConventions.UPPER_CAMEL_CASE,
        NamingConventions.LOWER_SNAKE_CASE,
        "MyXmlParser",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void upper_camel_case_to_upper_snake_case() {
    test(
        NamingConventions.UPPER_CAMEL_CASE,
        NamingConventions.UPPER_SNAKE_CASE,
        "MyXmlParser",
        "MY_XML_PARSER");
  }

  @Test(groups = "unit")
  public void upper_camel_case_to_lower_lisp_case() {
    test(
        NamingConventions.UPPER_CAMEL_CASE,
        NamingConventions.LOWER_LISP_CASE,
        "MyXmlParser",
        "my-xml-parser");
  }

  @Test(groups = "unit")
  public void upper_camel_case_to_upper_lisp_case() {
    test(
        NamingConventions.UPPER_CAMEL_CASE,
        NamingConventions.UPPER_LISP_CASE,
        "MyXmlParser",
        "MY-XML-PARSER");
  }

  @Test(groups = "unit")
  public void upper_camel_case_to_lower_case() {
    test(
        NamingConventions.UPPER_CAMEL_CASE,
        NamingConventions.LOWER_CASE,
        "MyXmlParser",
        "myxmlparser");
  }

  // test lower snake case inputs

  @Test(groups = "unit")
  public void lower_snake_case_to_lower_camel_case() {
    test(
        NamingConventions.LOWER_SNAKE_CASE,
        NamingConventions.LOWER_CAMEL_CASE,
        "my_xml_parser",
        "myXmlParser");
  }

  @Test(groups = "unit")
  public void lower_snake_case_to_upper_camel_case() {
    test(
        NamingConventions.LOWER_SNAKE_CASE,
        NamingConventions.UPPER_CAMEL_CASE,
        "my_xml_parser",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void lower_snake_case_to_upper_snake_case() {
    test(
        NamingConventions.LOWER_SNAKE_CASE,
        NamingConventions.UPPER_SNAKE_CASE,
        "my_xml_parser",
        "MY_XML_PARSER");
  }

  @Test(groups = "unit")
  public void lower_snake_case_to_lower_lisp_case() {
    test(
        NamingConventions.LOWER_SNAKE_CASE,
        NamingConventions.LOWER_LISP_CASE,
        "my_xml_parser",
        "my-xml-parser");
  }

  @Test(groups = "unit")
  public void lower_snake_case_to_upper_lisp_case() {
    test(
        NamingConventions.LOWER_SNAKE_CASE,
        NamingConventions.UPPER_LISP_CASE,
        "my_xml_parser",
        "MY-XML-PARSER");
  }

  @Test(groups = "unit")
  public void lower_snake_case_to_lower_case() {
    test(
        NamingConventions.LOWER_SNAKE_CASE,
        NamingConventions.LOWER_CASE,
        "my_xml_parser",
        "myxmlparser");
  }

  // test upper snake case inputs

  @Test(groups = "unit")
  public void upper_snake_case_to_lower_camel_case() {
    test(
        NamingConventions.UPPER_SNAKE_CASE,
        NamingConventions.LOWER_CAMEL_CASE,
        "MY_XML_PARSER",
        "myXmlParser");
  }

  @Test(groups = "unit")
  public void upper_snake_case_to_upper_camel_case() {
    test(
        NamingConventions.UPPER_SNAKE_CASE,
        NamingConventions.UPPER_CAMEL_CASE,
        "MY_XML_PARSER",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void upper_snake_case_to_lower_snake_case() {
    test(
        NamingConventions.UPPER_SNAKE_CASE,
        NamingConventions.LOWER_SNAKE_CASE,
        "MY_XML_PARSER",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void upper_snake_case_to_lower_lisp_case() {
    test(
        NamingConventions.UPPER_SNAKE_CASE,
        NamingConventions.LOWER_LISP_CASE,
        "MY_XML_PARSER",
        "my-xml-parser");
  }

  @Test(groups = "unit")
  public void upper_snake_case_to_upper_lisp_case() {
    test(
        NamingConventions.UPPER_SNAKE_CASE,
        NamingConventions.UPPER_LISP_CASE,
        "MY_XML_PARSER",
        "MY-XML-PARSER");
  }

  @Test(groups = "unit")
  public void upper_snake_case_to_lower_case() {
    test(
        NamingConventions.UPPER_SNAKE_CASE,
        NamingConventions.LOWER_CASE,
        "MY_XML_PARSER",
        "myxmlparser");
  }

  // test lower lisp case inputs

  @Test(groups = "unit")
  public void lower_lisp_case_to_lower_camel_case() {
    test(
        NamingConventions.LOWER_LISP_CASE,
        NamingConventions.LOWER_CAMEL_CASE,
        "my-xml-parser",
        "myXmlParser");
  }

  @Test(groups = "unit")
  public void lower_lisp_case_to_upper_camel_case() {
    test(
        NamingConventions.LOWER_LISP_CASE,
        NamingConventions.UPPER_CAMEL_CASE,
        "my-xml-parser",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void lower_lisp_case_to_lower_snake_case() {
    test(
        NamingConventions.LOWER_LISP_CASE,
        NamingConventions.LOWER_SNAKE_CASE,
        "my-xml-parser",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void lower_lisp_case_to_upper_snake_case() {
    test(
        NamingConventions.LOWER_LISP_CASE,
        NamingConventions.UPPER_SNAKE_CASE,
        "my-xml-parser",
        "MY_XML_PARSER");
  }

  @Test(groups = "unit")
  public void lower_lisp_case_to_upper_lisp_case() {
    test(
        NamingConventions.LOWER_LISP_CASE,
        NamingConventions.UPPER_LISP_CASE,
        "my-xml-parser",
        "MY-XML-PARSER");
  }

  @Test(groups = "unit")
  public void lower_lisp_case_to_lower_case() {
    test(
        NamingConventions.LOWER_LISP_CASE,
        NamingConventions.LOWER_CASE,
        "my-xml-parser",
        "myxmlparser");
  }

  // test upper lisp case inputs

  @Test(groups = "unit")
  public void upper_lisp_case_to_lower_camel_case() {
    test(
        NamingConventions.UPPER_LISP_CASE,
        NamingConventions.LOWER_CAMEL_CASE,
        "MY-XML-PARSER",
        "myXmlParser");
  }

  @Test(groups = "unit")
  public void upper_lisp_case_to_upper_camel_case() {
    test(
        NamingConventions.UPPER_LISP_CASE,
        NamingConventions.UPPER_CAMEL_CASE,
        "MY-XML-PARSER",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void upper_lisp_case_to_lower_snake_case() {
    test(
        NamingConventions.UPPER_LISP_CASE,
        NamingConventions.LOWER_SNAKE_CASE,
        "MY-XML-PARSER",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void upper_lisp_case_to_upper_snake_case() {
    test(
        NamingConventions.UPPER_LISP_CASE,
        NamingConventions.UPPER_SNAKE_CASE,
        "MY-XML-PARSER",
        "MY_XML_PARSER");
  }

  @Test(groups = "unit")
  public void upper_lisp_case_to_lower_lisp_case() {
    test(
        NamingConventions.UPPER_LISP_CASE,
        NamingConventions.LOWER_LISP_CASE,
        "MY-XML-PARSER",
        "my-xml-parser");
  }

  @Test(groups = "unit")
  public void upper_lisp_case_to_lower_case() {
    test(
        NamingConventions.UPPER_LISP_CASE,
        NamingConventions.LOWER_CASE,
        "MY-XML-PARSER",
        "myxmlparser");
  }

  // test special camel case settings

  @Test(groups = "unit")
  public void lower_camel_case_with_prefix_to_upper_camel_case() {
    test(
        new NamingConventions.LowerCamelCase("_"),
        NamingConventions.UPPER_CAMEL_CASE,
        "_myXmlParser",
        "MyXmlParser");
    test(
        new NamingConventions.LowerCamelCase("_"),
        NamingConventions.UPPER_CAMEL_CASE,
        "myXmlParser",
        "MyXmlParser");
    test(
        new NamingConventions.LowerCamelCase("_", "m"),
        NamingConventions.UPPER_CAMEL_CASE,
        "mMyXmlParser",
        "MyXmlParser");
    test(
        new NamingConventions.LowerCamelCase("_", "m"),
        NamingConventions.UPPER_CAMEL_CASE,
        "_MyXmlParser",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void lower_camel_case_with_prefix_to_lower_snake_case() {
    test(
        new NamingConventions.LowerCamelCase("_"),
        NamingConventions.LOWER_SNAKE_CASE,
        "_myXmlParser",
        "my_xml_parser");
    test(
        new NamingConventions.LowerCamelCase("_"),
        NamingConventions.LOWER_SNAKE_CASE,
        "myXmlParser",
        "my_xml_parser");
    test(
        new NamingConventions.LowerCamelCase("_", "m"),
        NamingConventions.LOWER_SNAKE_CASE,
        "mMyXmlParser",
        "my_xml_parser");
    test(
        new NamingConventions.LowerCamelCase("_", "m"),
        NamingConventions.LOWER_SNAKE_CASE,
        "_MyXmlParser",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void upper_camel_case_with_prefix_to_upper_camel_case() {
    test(
        new NamingConventions.UpperCamelCase("_"),
        NamingConventions.UPPER_CAMEL_CASE,
        "_MyXmlParser",
        "MyXmlParser");
    test(
        new NamingConventions.UpperCamelCase("_"),
        NamingConventions.UPPER_CAMEL_CASE,
        "MyXmlParser",
        "MyXmlParser");
    test(
        new NamingConventions.UpperCamelCase("_", "m"),
        NamingConventions.UPPER_CAMEL_CASE,
        "mMyXmlParser",
        "MyXmlParser");
    test(
        new NamingConventions.UpperCamelCase("_", "m"),
        NamingConventions.UPPER_CAMEL_CASE,
        "_MyXmlParser",
        "MyXmlParser");
  }

  @Test(groups = "unit")
  public void upper_camel_case_with_prefix_to_upper_snake_case() {
    test(
        new NamingConventions.UpperCamelCase("_"),
        NamingConventions.LOWER_SNAKE_CASE,
        "_MyXmlParser",
        "my_xml_parser");
    test(
        new NamingConventions.UpperCamelCase("_"),
        NamingConventions.LOWER_SNAKE_CASE,
        "MyXmlParser",
        "my_xml_parser");
    test(
        new NamingConventions.UpperCamelCase("_", "m"),
        NamingConventions.LOWER_SNAKE_CASE,
        "mMyXmlParser",
        "my_xml_parser");
    test(
        new NamingConventions.UpperCamelCase("_", "m"),
        NamingConventions.LOWER_SNAKE_CASE,
        "_MyXmlParser",
        "my_xml_parser");
  }

  @Test(groups = "unit")
  public void lower_camel_case_with_abbr_to_lower_camel_case_with_no_abbr() {
    test(
        new NamingConventions.LowerCamelCase(true),
        new NamingConventions.LowerCamelCase(false),
        "myXMLParser",
        "myXmlParser");
  }

  @Test(groups = "unit")
  public void upper_camel_case_with_abbr_to_lower_camel_case_with_abbr() {
    test(
        new NamingConventions.UpperCamelCase(true),
        new NamingConventions.LowerCamelCase(true),
        "MyXMLParser",
        "myXMLParser");
  }

  /**
   * The conventions fold case to produce a CQL identifier, so the JVM's default locale must not
   * decide the result. In a Turkish locale a lower-case i upper-cases to a dotted I and an
   * upper-case I lower-cases to a dotless one, so every name below would come out unmatchable
   * against the column the server actually holds. Each input deliberately carries an i.
   *
   * <p>The abbreviation branches of the two camel-case joins are pinned as well, but no input
   * discriminates them, which was checked by reverting each one in turn: the splitter only marks an
   * all-upper-case word as an abbreviation, so the fold applied there is a no-op in any locale.
   */
  @Test(groups = "unit")
  public void should_apply_conventions_in_any_default_locale() {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(new Locale("tr", "TR"));
      test(NamingConventions.LOWER_CAMEL_CASE, NamingConventions.UPPER_CASE, "id", "ID");
      test(NamingConventions.LOWER_CAMEL_CASE, NamingConventions.LOWER_CASE, "ID", "id");
      test(
          NamingConventions.LOWER_CAMEL_CASE,
          NamingConventions.UPPER_SNAKE_CASE,
          "minPrice",
          "MIN_PRICE");
      test(
          NamingConventions.LOWER_CAMEL_CASE,
          NamingConventions.UPPER_CAMEL_CASE,
          "itemId",
          "ItemId");
      // The three below drive the lower-camel join, one per fold it applies: the first word, the
      // leading letter of a later word, and that word's tail.
      test(
          NamingConventions.UPPER_SNAKE_CASE,
          NamingConventions.LOWER_CAMEL_CASE,
          "ITEM_ID",
          "itemId");
      test(
          NamingConventions.LOWER_SNAKE_CASE,
          NamingConventions.LOWER_CAMEL_CASE,
          "item_id",
          "itemId");
      test(
          NamingConventions.UPPER_SNAKE_CASE,
          NamingConventions.LOWER_CAMEL_CASE,
          "ITEM_XID",
          "itemXid");
      // Drives the upper-camel join's tail fold, which the itemId case above leaves untouched.
      test(
          NamingConventions.UPPER_SNAKE_CASE,
          NamingConventions.UPPER_CAMEL_CASE,
          "ITEM_XID",
          "ItemXid");
    } finally {
      Locale.setDefault(def);
    }
  }

  private void test(
      NamingConvention inputConvention,
      NamingConvention outputConvention,
      String input,
      String output) {
    DefaultNamingStrategy namingStrategy =
        new DefaultNamingStrategy(inputConvention, outputConvention);
    String result = namingStrategy.toCassandraName(input);
    assertThat(result).isEqualTo(output);
  }
}
