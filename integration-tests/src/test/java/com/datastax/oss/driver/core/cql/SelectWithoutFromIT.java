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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.List;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * SELECT without FROM requires Scylla 2026.4.0 or later. Raw CQL is used because the query
 * builder's SELECT API requires a table. Preparation, execution, and result decoding errors on
 * eligible servers fail normally.
 */
@Category(ParallelizableTests.class)
@BackendRequirement(type = BackendType.SCYLLA, minInclusive = "2026.4.0")
public class SelectWithoutFromIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE).withKeyspace(false).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void assertNoKeyspace() {
    assertThat(SESSION_RULE.session().getKeyspace()).isEmpty();
  }

  @Test
  public void should_decode_literal_without_from() {
    assertLiteralResult(SESSION_RULE.session().execute("SELECT 1"));
  }

  @Test
  public void should_decode_now_without_from() {
    assertNowResult(SESSION_RULE.session().execute("SELECT now()"));
  }

  @Test
  public void should_decode_prepared_literal_without_from() {
    PreparedStatement prepared = SESSION_RULE.session().prepare("SELECT 1");
    assertDefinitions(prepared.getResultSetDefinitions(), "1", DataTypes.INT);
    assertLiteralResult(SESSION_RULE.session().execute(assertPrepared(prepared)));
  }

  @Test
  public void should_decode_prepared_now_without_from() {
    PreparedStatement prepared = SESSION_RULE.session().prepare("SELECT now()");
    assertDefinitions(prepared.getResultSetDefinitions(), "system.now()", DataTypes.TIMEUUID);
    assertNowResult(SESSION_RULE.session().execute(assertPrepared(prepared)));
  }

  private static BoundStatement assertPrepared(PreparedStatement prepared) {
    assertThat(prepared.getVariableDefinitions()).isEmpty();
    assertThat(prepared.getPartitionKeyIndices()).isEmpty();
    BoundStatement bound = prepared.bind();
    assertThat(bound.getRoutingKeyspace()).isNull();
    assertThat(bound.getRoutingKey()).isNull();
    return bound;
  }

  private static void assertLiteralResult(ResultSet result) {
    assertDefinitions(result.getColumnDefinitions(), "1", DataTypes.INT);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);
    Row row = rows.get(0);
    assertThat(row.getInt(0)).isEqualTo(1);
    assertThat(row.getInt("1")).isEqualTo(1);
    assertThat(row.getObject(0)).isEqualTo(1);
  }

  private static void assertNowResult(ResultSet result) {
    assertDefinitions(result.getColumnDefinitions(), "system.now()", DataTypes.TIMEUUID);
    List<Row> rows = result.all();
    assertThat(rows).hasSize(1);
    Row row = rows.get(0);
    UUID value = row.getUuid(0);
    assertThat(value).isNotNull();
    assertThat(value.version()).isEqualTo(1);
    assertThat(row.getUuid("system.now()")).isEqualTo(value);
    assertThat(row.getObject(0)).isEqualTo(value);
  }

  private static void assertDefinitions(ColumnDefinitions definitions, String name, DataType type) {
    assertThat(definitions).hasSize(1);
    assertThat(definitions.get(0).getName().asInternal()).isEqualTo(name);
    assertThat(definitions.get(0).getType()).isEqualTo(type);
  }
}
