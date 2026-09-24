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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * Pure unit tests for the column-projection helpers and caching fields in {@link
 * SystemColumnProjection} (DRIVER-368).
 *
 * <p>These tests do not require a running Cassandra/Scylla node. For integration-level tests see
 * {@link ControlConnectionTest}.
 */
public class ControlConnectionUnitTest {

  // ---------------------------------------------------------------------------
  // *_COLUMNS_OF_INTEREST constants
  // ---------------------------------------------------------------------------

  @Test(groups = "unit")
  public void testLocalColumnsOfInterestContainsExpectedColumns() {
    ImmutableSet<String> cols = SystemColumnProjection.LOCAL_COLUMNS_OF_INTEREST;
    assertThat(cols)
        .contains(
            "cluster_name",
            "tokens",
            "host_id",
            "native_address",
            "dse_version",
            "rpc_address",
            "schema_version",
            "data_center",
            "rack",
            "release_version",
            "partitioner");
  }

  @Test(groups = "unit")
  public void testLocalColumnsOfInterestSize() {
    // 21 columns as documented in the constant declaration
    assertThat(SystemColumnProjection.LOCAL_COLUMNS_OF_INTEREST).hasSize(21);
  }

  @Test(groups = "unit")
  public void testPeersColumnsOfInterestContainsExpectedColumns() {
    ImmutableSet<String> cols = SystemColumnProjection.PEERS_COLUMNS_OF_INTEREST;
    assertThat(cols)
        .contains(
            "peer",
            "peer_port",
            "rpc_address",
            "tokens",
            "native_address",
            "native_port",
            "native_transport_address",
            "data_center",
            "rack",
            "host_id",
            "dse_version");
  }

  @Test(groups = "unit")
  public void testPeersColumnsOfInterestSize() {
    // 19 columns: original 16 + peer_port, native_address, native_port
    assertThat(SystemColumnProjection.PEERS_COLUMNS_OF_INTEREST).hasSize(19);
  }

  @Test(groups = "unit")
  public void testPeersV2ColumnsOfInterestContainsExpectedColumns() {
    ImmutableSet<String> cols = SystemColumnProjection.PEERS_V2_COLUMNS_OF_INTEREST;
    assertThat(cols)
        .contains(
            "peer",
            "peer_port",
            "native_address",
            "native_port",
            "rpc_address",
            "native_transport_address",
            "native_transport_port",
            "native_transport_port_ssl",
            "data_center",
            "rack",
            "tokens",
            "host_id",
            "dse_version");
  }

  @Test(groups = "unit")
  public void testPeersV2ColumnsOfInterestSize() {
    // 19 columns: original 15 + rpc_address, native_transport_address/port/port_ssl
    assertThat(SystemColumnProjection.PEERS_V2_COLUMNS_OF_INTEREST).hasSize(19);
  }

  @Test(groups = "unit")
  public void testPeersV2ContainsLegacyColumns() {
    // rpc_address, native_transport_address/port/port_ssl are legacy columns the driver reads
    // with contains() guards. They are included so they are not silently dropped if a server
    // exposes them in peers_v2.
    assertThat(SystemColumnProjection.PEERS_V2_COLUMNS_OF_INTEREST)
        .contains(
            "rpc_address",
            "native_transport_address",
            "native_transport_port",
            "native_transport_port_ssl");
  }

  // ---------------------------------------------------------------------------
  // intersectWithNeeded
  // ---------------------------------------------------------------------------

  /** Helper: build a mock ResultSet whose column definitions contain exactly the given names. */
  private static ResultSet mockResultSetWithColumns(String... columnNames) {
    ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[columnNames.length];
    for (int i = 0; i < columnNames.length; i++) {
      defs[i] =
          new ColumnDefinitions.Definition("system", "local", columnNames[i], DataType.text());
    }
    ColumnDefinitions colDefs = new ColumnDefinitions(defs, CodecRegistry.DEFAULT_INSTANCE);

    ResultSet rs = mock(ResultSet.class);
    when(rs.getColumnDefinitions()).thenReturn(colDefs);
    return rs;
  }

  @Test(groups = "unit")
  public void testIntersectWithNeededReturnsSupersetIntersection() {
    // RS has all LOCAL columns plus some extras; result should be exactly LOCAL_COLUMNS_OF_INTEREST
    ImmutableSet<String> needed = SystemColumnProjection.LOCAL_COLUMNS_OF_INTEREST;
    String[] base = needed.asList().toArray(new String[0]);
    // Append two extra columns not in the interest set
    String[] extended = java.util.Arrays.copyOf(base, base.length + 2);
    extended[base.length] = "extra_col_1";
    extended[base.length + 1] = "extra_col_2";

    ResultSet rs = mockResultSetWithColumns(extended);
    Set<String> result = SystemColumnProjection.intersectWithNeeded(rs, needed);

    assertThat(result).isEqualTo(needed);
    assertThat(result).doesNotContain("extra_col_1", "extra_col_2");
  }

  @Test(groups = "unit")
  public void testIntersectWithNeededHandlesSubset() {
    // RS only exposes a subset of the needed columns
    ImmutableSet<String> needed =
        ImmutableSet.of("cluster_name", "tokens", "host_id", "schema_version");
    ResultSet rs = mockResultSetWithColumns("cluster_name", "tokens");

    Set<String> result = SystemColumnProjection.intersectWithNeeded(rs, needed);

    assertThat(result).containsOnly("cluster_name", "tokens");
    assertThat(result).hasSize(2);
  }

  @Test(groups = "unit")
  public void testIntersectWithNeededNoOverlapReturnsNull() {
    // When no server columns match the needed set, the result should be null so the cache remains
    // in the uninitialized sentinel state (avoids generating an empty-column SELECT projection).
    ImmutableSet<String> needed = ImmutableSet.of("cluster_name", "tokens");
    ResultSet rs = mockResultSetWithColumns("some_other_col", "another_col");

    Set<String> result = SystemColumnProjection.intersectWithNeeded(rs, needed);

    assertThat(result).isNull();
  }

  @Test(groups = "unit")
  public void testIntersectWithNeededEmptyResultSetReturnsNull() {
    // An empty ResultSet has no column definitions, so the intersection is empty → null.
    ImmutableSet<String> needed = SystemColumnProjection.LOCAL_COLUMNS_OF_INTEREST;
    ResultSet rs = mockResultSetWithColumns();

    Set<String> result = SystemColumnProjection.intersectWithNeeded(rs, needed);

    assertThat(result).isNull();
  }

  // ---------------------------------------------------------------------------
  // buildProjectedQuery
  // ---------------------------------------------------------------------------

  @Test(groups = "unit")
  public void testBuildProjectedQueryWithWhereClause() {
    Set<String> columns = ImmutableSet.of("cluster_name", "host_id");
    String query =
        SystemColumnProjection.buildProjectedQuery("system.local", columns, "key='local'");

    assertThat(query).startsWith("SELECT ");
    assertThat(query).contains("cluster_name");
    assertThat(query).contains("host_id");
    assertThat(query).contains(" FROM system.local");
    assertThat(query).contains(" WHERE key='local'");
    // Should not contain SELECT *
    assertThat(query).doesNotContain("*");
  }

  @Test(groups = "unit")
  public void testBuildProjectedQueryWithoutWhereClause() {
    Set<String> columns = ImmutableSet.of("peer", "rpc_address", "tokens");
    String query = SystemColumnProjection.buildProjectedQuery("system.peers", columns, null);

    assertThat(query).startsWith("SELECT ");
    assertThat(query).contains("peer");
    assertThat(query).contains("rpc_address");
    assertThat(query).contains("tokens");
    assertThat(query).contains(" FROM system.peers");
    assertThat(query).doesNotContain("WHERE");
  }

  @Test(groups = "unit")
  public void testBuildProjectedQuerySingleColumn() {
    Set<String> columns = ImmutableSet.of("host_id");
    String query = SystemColumnProjection.buildProjectedQuery("system.local", columns, null);

    assertThat(query).isEqualTo("SELECT host_id FROM system.local");
  }

  @Test(groups = "unit")
  public void testBuildProjectedQueryAllColumnsPresent() {
    // Every column in the needed set must appear as an exact identifier in the projected SELECT
    // list. Use exact parsing to avoid false positives where one column name is a substring of
    // another (e.g. "native_port" inside "native_transport_port").
    Set<String> columns = SystemColumnProjection.PEERS_COLUMNS_OF_INTEREST;
    String query = SystemColumnProjection.buildProjectedQuery("system.peers", columns, null);
    Set<String> selectedColumns = extractSelectedColumns(query);

    for (String col : columns) {
      assertThat(selectedColumns).as("query should project column: " + col).contains(col);
    }
    assertThat(query).contains(" FROM system.peers");
    assertThat(query).doesNotContain("WHERE");
  }

  /**
   * Parses the column identifiers from the {@code SELECT col1, col2, ... FROM ...} portion of a
   * projected query string and returns them as a set of trimmed names.
   */
  private Set<String> extractSelectedColumns(String query) {
    int selectStart = query.indexOf("SELECT ");
    int fromStart = query.indexOf(" FROM ");
    assertThat(selectStart).as("query should start with SELECT").isEqualTo(0);
    assertThat(fromStart).as("query should contain FROM").isGreaterThan(selectStart);
    String columnList = query.substring("SELECT ".length(), fromStart);
    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    for (String col : columnList.split(",")) {
      builder.add(col.trim());
    }
    return builder.build();
  }

  // ---------------------------------------------------------------------------
  // Cache fields: declared as volatile, private, instance-level Set
  // ---------------------------------------------------------------------------

  @Test(groups = "unit")
  public void testCacheFieldsAreVolatilePrivateInstanceSets() throws Exception {
    for (String fieldName : new String[] {"localColumns", "peersColumns", "peersV2Columns"}) {
      Field field = SystemColumnProjection.class.getDeclaredField(fieldName);
      int mods = field.getModifiers();

      assertThat(Modifier.isVolatile(mods)).as(fieldName + " should be volatile").isTrue();
      assertThat(Modifier.isPrivate(mods)).as(fieldName + " should be private").isTrue();
      assertThat(Modifier.isStatic(mods)).as(fieldName + " must be an instance field").isFalse();
      assertThat(Set.class.isAssignableFrom(field.getType()))
          .as(fieldName + " declared type should be Set")
          .isTrue();
    }
  }

  // ---------------------------------------------------------------------------
  // hook: callback populates cache on success, resets all caches on failure
  // ---------------------------------------------------------------------------

  /**
   * A minimal subclass of {@link DefaultResultSetFuture} that exposes the protected {@code
   * setException} method so tests can drive failure scenarios without a real connection.
   */
  private static class SettableResultSetFuture extends DefaultResultSetFuture {
    SettableResultSetFuture() {
      super(null, ProtocolVersion.V4, new Requests.Query("SELECT * FROM system.peers"));
    }

    void failWith(Exception e) {
      setException(e);
    }
  }

  @Test(groups = "unit")
  public void testHookPopulatesCacheOnSuccess() {
    SystemColumnProjection projection = new SystemColumnProjection();
    // Cache is cold — query(PEERS) should return SELECT *.
    assertThat(projection.query(SystemColumnProjection.SystemTable.PEERS))
        .isEqualTo("SELECT * FROM system.peers");

    SettableResultSetFuture future = new SettableResultSetFuture();
    projection.hook(SystemColumnProjection.SystemTable.PEERS, future);

    // Complete the future with a result set that contains known peers columns.
    ResultSet rs =
        mockResultSetWithColumns("peer", "rpc_address", "host_id", "data_center", "tokens");
    future.setResult(rs);

    // Cache should now be warm; query(PEERS) must return a projected query, not SELECT *.
    String query = projection.query(SystemColumnProjection.SystemTable.PEERS);
    assertThat(query).doesNotContain("*");
    assertThat(query).startsWith("SELECT ");
    assertThat(query).contains(" FROM system.peers");
    assertThat(query).doesNotContain("WHERE");
    // All columns from the mock RS that are in PEERS_COLUMNS_OF_INTEREST must be projected.
    Set<String> selected = extractSelectedColumns(query);
    assertThat(selected).containsOnly("peer", "rpc_address", "host_id", "data_center", "tokens");
  }

  @Test(groups = "unit")
  public void testHookResetsCacheOnInvalidQueryException() {
    SystemColumnProjection projection = new SystemColumnProjection();
    // Warm the local and peers_v2 caches so we can verify reset() clears them too.
    ResultSet localRs = mockResultSetWithColumns("cluster_name", "host_id", "tokens");
    projection.populate(SystemColumnProjection.SystemTable.LOCAL, localRs);
    assertThat(projection.query(SystemColumnProjection.SystemTable.LOCAL)).doesNotContain("*");

    SettableResultSetFuture future = new SettableResultSetFuture();
    projection.hook(SystemColumnProjection.SystemTable.PEERS, future);

    // Fail the future with an InvalidQueryException — hook must call reset().
    future.failWith(new InvalidQueryException(null, "Unknown column 'x'"));

    // All caches must be cleared: query(LOCAL) must return SELECT * again.
    assertThat(projection.query(SystemColumnProjection.SystemTable.LOCAL))
        .isEqualTo("SELECT * FROM system.local WHERE key='local'");
    assertThat(projection.query(SystemColumnProjection.SystemTable.PEERS))
        .isEqualTo("SELECT * FROM system.peers");
    assertThat(projection.query(SystemColumnProjection.SystemTable.PEERS_V2))
        .isEqualTo("SELECT * FROM system.peers_v2");
  }

  @Test(groups = "unit")
  public void testHookDoesNotResetCacheOnOtherFailure() {
    SystemColumnProjection projection = new SystemColumnProjection();
    // Warm local cache.
    ResultSet localRs = mockResultSetWithColumns("cluster_name", "host_id", "tokens");
    projection.populate(SystemColumnProjection.SystemTable.LOCAL, localRs);
    String warmLocalQuery = projection.query(SystemColumnProjection.SystemTable.LOCAL);
    assertThat(warmLocalQuery).doesNotContain("*");

    SettableResultSetFuture future = new SettableResultSetFuture();
    projection.hook(SystemColumnProjection.SystemTable.PEERS, future);

    // Fail with a non-InvalidQueryException — reset() must NOT be called.
    future.failWith(new RuntimeException("connection lost"));

    // Local cache must be untouched.
    assertThat(projection.query(SystemColumnProjection.SystemTable.LOCAL))
        .isEqualTo(warmLocalQuery);
  }
}
