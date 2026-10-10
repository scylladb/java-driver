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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class SelectWithoutFromTest extends CCMTestsSupport {

  @Test(groups = "short")
  public void should_execute_select_without_from() {
    ResultSet result;
    try {
      result = session().execute("SELECT 1");
    } catch (InvalidQueryException | SyntaxError e) {
      throw new SkipException("Server does not support SELECT without FROM", e);
    }

    assertLiteralResult(result);
    assertNowResult(session().execute("SELECT now()"));

    PreparedStatement prepared = session().prepare("SELECT 1");
    assertLiteralResult(session().execute(prepared.bind()));

    prepared = session().prepare("SELECT now()");
    assertNowResult(session().execute(prepared.bind()));
  }

  private static void assertLiteralResult(ResultSet result) {
    Row row = result.one();
    assertThat(row).isNotNull();
    ColumnDefinitions definitions = result.getColumnDefinitions();
    assertThat(definitions.size()).isEqualTo(1);
    assertThat(definitions.getName(0)).isEqualTo("1");
    assertThat(definitions.getType(0)).isEqualTo(DataType.cint());
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  private static void assertNowResult(ResultSet result) {
    Row row = result.one();
    assertThat(row).isNotNull();
    ColumnDefinitions definitions = result.getColumnDefinitions();
    assertThat(definitions.size()).isEqualTo(1);
    assertThat(definitions.getName(0)).isEqualTo("now()");
    assertThat(definitions.getType(0)).isEqualTo(DataType.timeuuid());
    assertThat(row.getUUID(0)).isNotNull();
  }
}
