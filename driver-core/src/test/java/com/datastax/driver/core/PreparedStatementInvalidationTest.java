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

/*
 * Copyright (C) 2012-2017 DataStax Inc.
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

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static junit.framework.TestCase.fail;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PreparedStatementInvalidationTest extends CCMTestsSupport {

  @BeforeMethod(groups = "short", alwaysRun = true)
  public void setup() throws Exception {
    execute("CREATE TABLE prepared_statement_invalidation_test (a int PRIMARY KEY, b int, c int);");
    execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (1, 1, 1);");
    execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (2, 2, 2);");
    execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (3, 3, 3);");
    execute("INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (4, 4, 4);");
  }

  @AfterMethod(groups = "short", alwaysRun = true)
  public void teardown() throws Exception {
    execute("DROP TABLE prepared_statement_invalidation_test");
  }

  @CassandraVersion("4.0")
  @Test(groups = "short")
  public void should_update_statement_id_when_metadata_changed_across_executions() {
    // given
    PreparedStatement ps =
        session().prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");
    MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;
    // when
    session().execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
    BoundStatement bs = ps.bind(1);
    ResultSet rows = session().execute(bs);
    // then
    MD5Digest idAfter = ps.getPreparedId().resultSetMetadata.id;
    assertThat(idBefore).isNotEqualTo(idAfter);
    assertThat(ps.getPreparedId().resultSetMetadata.variables)
        .hasSize(4)
        .containsVariable("d", DataType.cint());
    assertThat(bs.preparedStatement().getPreparedId().resultSetMetadata.variables)
        .hasSize(4)
        .containsVariable("d", DataType.cint());
    assertThat(rows.getColumnDefinitions()).hasSize(4).containsVariable("d", DataType.cint());
  }

  @CassandraVersion("4.0")
  @Test(groups = "short")
  public void should_update_statement_id_when_metadata_changed_across_pages() throws Exception {
    // given
    PreparedStatement ps = session().prepare("SELECT * FROM prepared_statement_invalidation_test");
    ResultSet rows = session().execute(ps.bind().setFetchSize(2));
    assertThat(rows.isFullyFetched()).isFalse();
    MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;
    ColumnDefinitions definitionsBefore = rows.getColumnDefinitions();
    assertThat(definitionsBefore).hasSize(3).doesNotContainVariable("d");
    // consume the first page
    int remaining = rows.getAvailableWithoutFetching();
    while (remaining-- > 0) {
      try {
        rows.one().getInt("d");
        fail("expected an error");
      } catch (IllegalArgumentException e) {
        /*expected*/
      }
    }

    // when
    session().execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");

    // then
    // this should trigger a background fetch of the second page, and therefore update the
    // definitions
    for (Row row : rows) {
      assertThat(row.isNull("d")).isTrue();
    }
    MD5Digest idAfter = ps.getPreparedId().resultSetMetadata.id;
    ColumnDefinitions definitionsAfter = rows.getColumnDefinitions();
    assertThat(idBefore).isNotEqualTo(idAfter);
    assertThat(definitionsAfter).hasSize(4).containsVariable("d", DataType.cint());
  }

  @CassandraVersion("4.0")
  @Test(groups = "short")
  public void should_update_statement_id_when_metadata_changed_across_sessions() {
    Session session1 = cluster().connect();
    useKeyspace(session1, keyspace);
    Session session2 = cluster().connect();
    useKeyspace(session2, keyspace);

    PreparedStatement ps1 =
        session1.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");
    PreparedStatement ps2 =
        session2.prepare("SELECT * FROM prepared_statement_invalidation_test WHERE a = ?");

    MD5Digest id1a = ps1.getPreparedId().resultSetMetadata.id;
    MD5Digest id2a = ps2.getPreparedId().resultSetMetadata.id;

    ResultSet rows1 = session1.execute(ps1.bind(1));
    ResultSet rows2 = session2.execute(ps2.bind(1));

    assertThat(rows1.getColumnDefinitions())
        .hasSize(3)
        .containsVariable("a", DataType.cint())
        .containsVariable("b", DataType.cint())
        .containsVariable("c", DataType.cint());
    assertThat(rows2.getColumnDefinitions())
        .hasSize(3)
        .containsVariable("a", DataType.cint())
        .containsVariable("b", DataType.cint())
        .containsVariable("c", DataType.cint());

    session1.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");

    rows1 = session1.execute(ps1.bind(1));
    rows2 = session2.execute(ps2.bind(1));

    MD5Digest id1b = ps1.getPreparedId().resultSetMetadata.id;
    MD5Digest id2b = ps2.getPreparedId().resultSetMetadata.id;

    assertThat(id1a).isNotEqualTo(id1b);
    assertThat(id2a).isNotEqualTo(id2b);

    assertThat(ps1.getPreparedId().resultSetMetadata.variables)
        .hasSize(4)
        .containsVariable("d", DataType.cint());
    assertThat(ps2.getPreparedId().resultSetMetadata.variables)
        .hasSize(4)
        .containsVariable("d", DataType.cint());
    assertThat(rows1.getColumnDefinitions()).hasSize(4).containsVariable("d", DataType.cint());
    assertThat(rows2.getColumnDefinitions()).hasSize(4).containsVariable("d", DataType.cint());
  }

  @CassandraVersion("4.0")
  @Test(groups = "short", expectedExceptions = NoHostAvailableException.class)
  public void should_not_reprepare_invalid_statements() {
    // given
    session().execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
    PreparedStatement ps =
        session()
            .prepare("SELECT a, b, c, d FROM prepared_statement_invalidation_test WHERE a = ?");
    session().execute("ALTER TABLE prepared_statement_invalidation_test DROP d");
    // when
    session().execute(ps.bind());
  }

  @CassandraVersion("4.0")
  @Test(groups = "short")
  public void should_never_update_statement_id_for_conditional_updates_in_modern_protocol() {
    should_never_update_statement_id_for_conditional_updates(session());
  }

  private void should_never_update_statement_id_for_conditional_updates(Session session) {
    // Given
    PreparedStatement ps =
        session.prepare(
            "INSERT INTO prepared_statement_invalidation_test (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS");

    // Never store metadata in the prepared statement for conditional updates, since the result set
    // can change
    // depending on the outcome.
    assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
    MD5Digest idBefore = ps.getPreparedId().resultSetMetadata.id;

    // When
    ResultSet rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Successful conditional update => only contains the [applied] column
    assertThat(rs.wasApplied()).isTrue();
    assertThat(rs.getColumnDefinitions())
        .hasSize(1)
        .containsVariable("[applied]", DataType.cboolean());
    // However the prepared statement shouldn't have changed
    assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
    assertThat(ps.getPreparedId().resultSetMetadata.id).isEqualTo(idBefore);

    // When
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(4);
    Row row = rs.one();
    assertThat(row.getBool("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    // The prepared statement still shouldn't have changed
    assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
    assertThat(ps.getPreparedId().resultSetMetadata.id).isEqualTo(idBefore);

    // When
    session.execute("ALTER TABLE prepared_statement_invalidation_test ADD d int");
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata that should also contain the new column
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(5);
    row = rs.one();
    assertThat(row.getBool("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    assertThat(row.isNull("d")).isTrue();
    assertThat(ps.getPreparedId().resultSetMetadata.variables).isNull();
    assertThat(ps.getPreparedId().resultSetMetadata.id).isEqualTo(idBefore);
  }

  @CassandraVersion("4.0")
  @Test(groups = "short")
  public void should_never_update_statement_for_conditional_updates_in_legacy_protocols() {
    // Given
    Cluster cluster =
        register(
            Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(ccm().getProtocolVersion(V4))
                .build());
    Session session = cluster.connect(keyspace);
    should_never_update_statement_id_for_conditional_updates(session);
  }

  @DataProvider(name = "resolverName")
  public static Object[][] resolverName() {
    return new Object[][] {
      {
        QueryOptions.CQL4SkipMetadataResolveMethod.SMART,
      },
      {
        QueryOptions.CQL4SkipMetadataResolveMethod.ENABLED,
      },
      {
        QueryOptions.CQL4SkipMetadataResolveMethod.DISABLED,
      }
    };
  }

  @Test(groups = "short", dataProvider = "resolverName")
  public void prepared_stmt_metadata_update_loopholes_test(
      QueryOptions.CQL4SkipMetadataResolveMethod resolver) {
    // v0 is an int column, but we'll bind a String to it
    try (Session session = sessionWithSkipCQL4MetadataResolveMethod(resolver)) {
      String resolverNameFixed = resolver.name().toLowerCase().replace("-", "_");

      String udtName = String.format("skip_metadata_test_%s_udt", resolverNameFixed);
      String udtTable = String.format("skip_metadata_test_%s_udttable", resolverNameFixed);
      String table = String.format("skip_metadata_test_%s_table", resolverNameFixed);
      session.execute(String.format("CREATE TYPE IF NOT EXISTS %s (x int, y int)", udtName));

      session.execute(
          String.format("CREATE TABLE %s (pk int, v %s, PRIMARY KEY (pk))", udtTable, udtName));
      session.execute(String.format("CREATE TABLE %s (pk int, v int, PRIMARY KEY (pk))", table));

      session.execute(String.format("INSERT INTO %s (pk, v) VALUES (1, 1)", table));
      session.execute(String.format("INSERT INTO %s (pk, v) VALUES (1, {x: 1, y: 1})", udtTable));

      PreparedStatement stmtRegularTableWCS =
          session.prepare(String.format("SELECT * FROM %s WHERE pk = ?", table));
      PreparedStatement stmtRegularTableTS =
          session.prepare(String.format("SELECT pk, v FROM %s WHERE pk = ?", table));
      PreparedStatement stmtUDTTableWCS =
          session.prepare(String.format("SELECT * FROM %s WHERE pk = ?", udtTable));
      PreparedStatement stmtUDTTableTS =
          session.prepare(String.format("SELECT pk, v FROM %s WHERE pk = ?", udtTable));

      boolean isCQL4orLower = stmtRegularTableWCS.getPreparedId().resultSetMetadata.id == null;
      boolean isPreparedStatementInvalidationBroken =
          isCQL4orLower && resolver == QueryOptions.CQL4SkipMetadataResolveMethod.ENABLED;

      if (isCQL4orLower) {
        switch (resolver) {
          case ENABLED:
            assertThat(((DefaultPreparedStatement) stmtRegularTableTS).isSkipMetadata())
                .isEqualTo(true);
            assertThat(((DefaultPreparedStatement) stmtRegularTableWCS).isSkipMetadata())
                .isEqualTo(true);
            assertThat(((DefaultPreparedStatement) stmtUDTTableWCS).isSkipMetadata())
                .isEqualTo(true);
            assertThat(((DefaultPreparedStatement) stmtUDTTableTS).isSkipMetadata())
                .isEqualTo(true);
            break;
          case DISABLED:
            assertThat(((DefaultPreparedStatement) stmtRegularTableTS).isSkipMetadata())
                .isEqualTo(false);
            assertThat(((DefaultPreparedStatement) stmtRegularTableWCS).isSkipMetadata())
                .isEqualTo(false);
            assertThat(((DefaultPreparedStatement) stmtUDTTableWCS).isSkipMetadata())
                .isEqualTo(false);
            assertThat(((DefaultPreparedStatement) stmtUDTTableTS).isSkipMetadata())
                .isEqualTo(false);
            break;
          default: // SMART
            assertThat(((DefaultPreparedStatement) stmtRegularTableTS).isSkipMetadata())
                .isEqualTo(true);
            assertThat(((DefaultPreparedStatement) stmtRegularTableWCS).isSkipMetadata())
                .isEqualTo(false);
            assertThat(((DefaultPreparedStatement) stmtUDTTableWCS).isSkipMetadata())
                .isEqualTo(false);
            assertThat(((DefaultPreparedStatement) stmtUDTTableTS).isSkipMetadata())
                .isEqualTo(false);
        }
      }

      Row row = session.execute(stmtUDTTableWCS.bind(1)).one();
      assertThat(row.getColumnDefinitions().size()).isEqualTo(2);
      assertThat(getUDTColumnCount(row.getColumnDefinitions().asList().get(1))).isEqualTo(2);
      row = session.execute(stmtUDTTableTS.bind(1)).one();
      assertThat(getUDTColumnCount(row.getColumnDefinitions().asList().get(1))).isEqualTo(2);
      assertThat(row.getColumnDefinitions().size()).isEqualTo(2);
      row = session.execute(stmtRegularTableWCS.bind(1)).one();
      assertThat(row.getColumnDefinitions().size()).isEqualTo(2);

      session.execute(String.format("ALTER TABLE %s ADD z int;", table));
      session.execute(String.format("ALTER TYPE %s ADD z int;", udtName));

      int expectedUDTColumnCount = 3;
      int expectedTableColumnCount = 3;
      if (isPreparedStatementInvalidationBroken) {
        // When case of CQL4 and skip metadata is set prepared statements will not be invalidated.
        expectedUDTColumnCount = 2;
        expectedTableColumnCount = 2;
      }

      row = session.execute(stmtUDTTableWCS.bind(1)).one();
      assertThat(row.getUDTValue(1).getType().getFieldNames().size())
          .isEqualTo(expectedUDTColumnCount);
      assertThat(row.getColumnDefinitions().size()).isEqualTo(2);
      assertThat(getUDTColumnCount(row.getColumnDefinitions().asList().get(1)))
          .isEqualTo(expectedUDTColumnCount);

      row = session.execute(stmtUDTTableTS.bind(1)).one();
      assertThat(row.getUDTValue(1).getType().getFieldNames().size())
          .isEqualTo(expectedUDTColumnCount);
      assertThat(row.getColumnDefinitions().size()).isEqualTo(2);
      assertThat(getUDTColumnCount(row.getColumnDefinitions().asList().get(1)))
          .isEqualTo(expectedUDTColumnCount);

      row = session.execute(stmtRegularTableWCS.bind(1)).one();
      assertThat(row.getColumnDefinitions().size()).isEqualTo(expectedTableColumnCount);
    }
  }

  private int getUDTColumnCount(ColumnDefinitions.Definition cd) {
    return ((UserType) cd.getType()).getFieldNames().size();
  }

  private Session sessionWithSkipCQL4MetadataResolveMethod(
      QueryOptions.CQL4SkipMetadataResolveMethod resolver) {
    Cluster cluster =
        register(
                Cluster.builder()
                    .addContactPoints(getContactPoints())
                    .withPort(ccm().getBinaryPort())
                    .withProtocolVersion(V4)
                    .withQueryOptions(new QueryOptions().setSkipCQL4MetadataResolveMethod(resolver))
                    .build())
            .init();
    Session session = cluster.connect();
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS cql4_loopholes_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }");
    session.execute("USE cql4_loopholes_test");
    return session;
  }
}
