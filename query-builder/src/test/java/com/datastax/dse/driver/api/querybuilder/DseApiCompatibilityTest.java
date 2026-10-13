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
package com.datastax.dse.driver.api.querybuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionWithLanguage;
import com.datastax.dse.driver.api.querybuilder.schema.DseGraphEdgeSide;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class DseApiCompatibilityTest {

  @Test
  public void should_escape_legacy_function_body_with_both_delimiters() {
    CreateDseFunctionWithLanguage builder = functionBody -> () -> functionBody;
    assertThat(builder.asQuoted("return \"O'Reilly $$\";").asCql())
        .isEqualTo("'return \"O''Reilly $$\";'");
  }

  @Test
  public void should_keep_dml_alias() {
    assertThat(DseQueryBuilder.selectFrom("ks", "tbl").all().asCql())
        .isEqualTo("SELECT * FROM ks.tbl");
  }

  @Test
  public void should_delegate_cassandra_aggregate() {
    assertThat(
            DseSchemaBuilder.createAggregate("ks", "agg")
                .withSFunc("state_function")
                .withSType(DataTypes.INT)
                .asCql())
        .isEqualTo("CREATE AGGREGATE ks.agg () SFUNC state_function STYPE int");
  }

  @Test
  public void should_delegate_cassandra_function() {
    assertThat(
            DseSchemaBuilder.createFunction(
                    CqlIdentifier.fromCql("ks"), CqlIdentifier.fromCql("fn"))
                .calledOnNull()
                .returnsType(DataTypes.INT)
                .withJavaLanguage()
                .asQuoted("return 1;")
                .asCql())
        .isEqualTo(
            "CREATE FUNCTION ks.fn () CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;'");
  }

  @Test
  public void should_inherit_cassandra_table_builder() {
    assertThat(
            DseSchemaBuilder.createTable("ks", "tbl").withPartitionKey("id", DataTypes.INT).asCql())
        .isEqualTo("CREATE TABLE ks.tbl (id int PRIMARY KEY)");
  }

  @Test
  public void should_reject_create_dse_keyspace() {
    assertThatThrownBy(() -> DseSchemaBuilder.createDseKeyspace("name"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSchemaBuilder.createDseKeyspace(CqlIdentifier.fromCql("name")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_alter_dse_keyspace() {
    assertThatThrownBy(() -> DseSchemaBuilder.alterDseKeyspace("name"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSchemaBuilder.alterDseKeyspace(CqlIdentifier.fromCql("name")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_create_dse_table() {
    assertThatThrownBy(() -> DseSchemaBuilder.createDseTable("name"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSchemaBuilder.createDseTable(CqlIdentifier.fromCql("name")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_alter_dse_table() {
    assertThatThrownBy(() -> DseSchemaBuilder.alterDseTable("name"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSchemaBuilder.alterDseTable(CqlIdentifier.fromCql("name")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_create_dse_function() {
    assertThatThrownBy(() -> DseSchemaBuilder.createDseFunction("name"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSchemaBuilder.createDseFunction(CqlIdentifier.fromCql("name")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_create_dse_aggregate() {
    assertThatThrownBy(() -> DseSchemaBuilder.createDseAggregate("name"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSchemaBuilder.createDseAggregate(CqlIdentifier.fromCql("name")))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void should_reject_dse_graph_edge_side() {
    assertThatThrownBy(() -> DseGraphEdgeSide.table("tbl"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseGraphEdgeSide.table(CqlIdentifier.fromCql("tbl")))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
