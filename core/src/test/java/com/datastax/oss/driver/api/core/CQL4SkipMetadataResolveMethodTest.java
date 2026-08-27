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
package com.datastax.oss.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.TestDataProviders;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Locale;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class CQL4SkipMetadataResolveMethodTest {

  /**
   * {@code fromValue} folds the configured value to compare it, so it must pin {@link Locale#ROOT}:
   * the I of {@code DISABLED} and {@code ENABLED} folds to a dotless small letter in a Turkish JVM,
   * which would match none of the cases and reject a valid configuration at session build.
   */
  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "locales")
  public void should_parse_value_in_any_default_locale(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(CQL4SkipMetadataResolveMethod.fromValue("DISABLED"))
          .isEqualTo(CQL4SkipMetadataResolveMethod.DISABLED);
      assertThat(CQL4SkipMetadataResolveMethod.fromValue("ENABLED"))
          .isEqualTo(CQL4SkipMetadataResolveMethod.ENABLED);
      assertThat(CQL4SkipMetadataResolveMethod.fromValue("SMART"))
          .isEqualTo(CQL4SkipMetadataResolveMethod.SMART);
      assertThat(CQL4SkipMetadataResolveMethod.fromValue("disabled"))
          .isEqualTo(CQL4SkipMetadataResolveMethod.DISABLED);
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  public void should_fail_to_parse_unknown_value() {
    assertThatThrownBy(() -> CQL4SkipMetadataResolveMethod.fromValue("nope"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported value nope");
  }
}
