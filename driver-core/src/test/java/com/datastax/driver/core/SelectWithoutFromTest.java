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

import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.utils.UUIDs;
import java.util.UUID;
import org.testng.SkipException;
import org.testng.annotations.Test;

@CCMConfig(createKeyspace = false)
public class SelectWithoutFromTest extends CCMTestsSupport {

  @Test(groups = "short")
  public void should_execute_select_without_from() {
    assertThat(session().getLoggedKeyspace()).isNull();
    ResultSet result;
    try {
      result = session().execute("SELECT 1");
    } catch (SyntaxError e) {
      throw new SkipException("Server does not support SELECT without FROM", e);
    }

    assertLiteralResult(result);
    assertNowResult(session().execute("SELECT now()"));

    PreparedStatement prepared = session().prepare("SELECT 1");
    assertThat(prepared.getVariables().size()).isZero();
    assertLiteralResult(session().execute(prepared.bind()));

    prepared = session().prepare("SELECT now()");
    assertThat(prepared.getVariables().size()).isZero();
    assertNowResult(session().execute(prepared.bind()));
    assertThat(session().getLoggedKeyspace()).isNull();
  }

  private static void assertLiteralResult(ResultSet result) {
    Row row = result.one();
    assertThat(row).isNotNull();
    ColumnDefinitions definitions = result.getColumnDefinitions();
    assertThat(definitions.size()).isEqualTo(1);
    assertThat(definitions.getName(0)).isEqualTo("1");
    assertThat(definitions.getType(0)).isEqualTo(DataType.cint());
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(result.one()).isNull();
  }

  private static void assertNowResult(ResultSet result) {
    Row row = result.one();
    assertThat(row).isNotNull();
    ColumnDefinitions definitions = result.getColumnDefinitions();
    assertThat(definitions.size()).isEqualTo(1);
    assertThat(definitions.getName(0)).isEqualTo("system.now()");
    assertThat(definitions.getType(0)).isEqualTo(DataType.timeuuid());
    UUID value = row.getUUID(0);
    assertThat(value).isNotNull();
    assertThat(value.version()).isEqualTo(1);
    assertThat(value.variant()).isEqualTo(2);
    assertThat(UUIDs.unixTimestamp(value)).isPositive();
    assertThat(result.one()).isNull();
  }
}
