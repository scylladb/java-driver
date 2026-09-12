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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeNoException;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SelectWithoutFromIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Test
  public void should_execute_select_without_from() {
    CqlSession session = SESSION_RULE.session();
    ResultSet result;
    try {
      result = session.execute("SELECT 1");
    } catch (InvalidQueryException | SyntaxError e) {
      assumeNoException("Server does not support SELECT without FROM", e);
      return;
    }

    assertLiteralResult(result);
    assertNowResult(session.execute("SELECT now()"));

    PreparedStatement prepared = session.prepare("SELECT 1");
    assertLiteralResult(session.execute(prepared.bind()));

    prepared = session.prepare("SELECT now()");
    assertNowResult(session.execute(prepared.bind()));
  }

  private static void assertLiteralResult(ResultSet result) {
    Row row = result.one();
    assertThat(row).isNotNull();
    ColumnDefinitions definitions = result.getColumnDefinitions();
    assertThat(definitions).hasSize(1);
    assertThat(definitions.get(0).getName().asInternal()).isEqualTo("1");
    assertThat(definitions.get(0).getType()).isEqualTo(DataTypes.INT);
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  private static void assertNowResult(ResultSet result) {
    Row row = result.one();
    assertThat(row).isNotNull();
    ColumnDefinitions definitions = result.getColumnDefinitions();
    assertThat(definitions).hasSize(1);
    assertThat(definitions.get(0).getName().asInternal()).isEqualTo("now()");
    assertThat(definitions.get(0).getType()).isEqualTo(DataTypes.TIMEUUID);
    assertThat(row.getUuid(0)).isNotNull();
  }
}
