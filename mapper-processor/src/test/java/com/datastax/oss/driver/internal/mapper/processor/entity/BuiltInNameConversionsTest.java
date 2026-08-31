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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.CASE_INSENSITIVE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.EXACT_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.LOWER_CAMEL_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.SNAKE_CASE_INSENSITIVE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_CAMEL_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_CASE;
import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_SNAKE_CASE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.TestDataProviders;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class BuiltInNameConversionsTest {

  @Test
  public void should_convert_to_cql() {
    should_convert_to_cql("Product", CASE_INSENSITIVE, "Product");
    should_convert_to_cql("productId", CASE_INSENSITIVE, "productId");

    should_convert_to_cql("Product", EXACT_CASE, "\"Product\"");
    should_convert_to_cql("productId", EXACT_CASE, "\"productId\"");

    should_convert_to_cql("Product", LOWER_CAMEL_CASE, "\"product\"");
    should_convert_to_cql("productId", LOWER_CAMEL_CASE, "\"productId\"");

    should_convert_to_cql("Product", UPPER_CAMEL_CASE, "\"Product\"");
    should_convert_to_cql("productId", UPPER_CAMEL_CASE, "\"ProductId\"");

    should_convert_to_cql("Product", SNAKE_CASE_INSENSITIVE, "product");
    should_convert_to_cql("productId", SNAKE_CASE_INSENSITIVE, "product_id");

    should_convert_to_cql("Product", UPPER_SNAKE_CASE, "\"PRODUCT\"");
    should_convert_to_cql("productId", UPPER_SNAKE_CASE, "\"PRODUCT_ID\"");

    should_convert_to_cql("Product", UPPER_CASE, "\"PRODUCT\"");
    should_convert_to_cql("productId", UPPER_CASE, "\"PRODUCTID\"");
  }

  /**
   * UPPER_CASE folds the property name itself, so in a Turkish JVM a lower-case i becomes a dotted
   * capital I and the generated code would reference a column that does not exist. The names below
   * deliberately carry a lower-case i: "productId" would not do, since its I is already capital and
   * no locale changes it. The UPPER_SNAKE_CASE case pins the claim that Guava's CaseFormat is
   * genuinely locale-neutral rather than assumed to be; it covers the four camel- and snake-case
   * conventions that delegate to it. CASE_INSENSITIVE and EXACT_CASE fold no case at all.
   */
  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_convert_to_cql_in_any_default_locale(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      should_convert_to_cql("id", UPPER_CASE, "\"ID\"");
      should_convert_to_cql("minPrice", UPPER_CASE, "\"MINPRICE\"");
      should_convert_to_cql("minPrice", UPPER_SNAKE_CASE, "\"MIN_PRICE\"");
    } finally {
      Locale.setDefault(def);
    }
  }

  private void should_convert_to_cql(
      String javaName, NamingConvention convention, String expectedCqlName) {
    String actualCqlName = BuiltInNameConversions.toCassandraName(javaName, convention);
    assertThat(actualCqlName).isEqualTo(expectedCqlName);
  }
}
