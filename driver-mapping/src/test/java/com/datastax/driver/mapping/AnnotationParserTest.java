/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.QueryParameters;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import java.util.Locale;
import org.testng.annotations.Test;

/**
 * {@link AnnotationParser} folds case in five places: the {@code @Table} and {@code @UDT} names it
 * hands to the schema metadata, and the three consistency levels it parses out of annotation
 * strings. None of them may depend on the JVM's default locale. In a Turkish locale an upper-case I
 * folds to a dotless {@code ı} and a lower-case i to a dotted {@code İ}, so {@code @Table(name =
 * "ID_TABLE")} would look up a table no server ever reported, and {@code writeConsistency =
 * "serial"} would fail {@code ConsistencyLevel.valueOf}.
 *
 * <p>Every one of those folds happens before the parser needs anything a live cluster provides, so
 * the tests drive it with a mocked manager and read the folded name back out of the failure the
 * metadata lookup raises. That keeps them in the unit group: the CCM mapper tests that do reach
 * this class never change the default locale, which is what left these five call sites uncovered.
 */
public class AnnotationParserTest {

  private static final Locale TURKISH = new Locale("tr", "TR");
  private static final String KEYSPACE = "ks";

  @Table(name = "ID_TABLE")
  static class IdTable {}

  @Table(name = "t", writeConsistency = "serial", readConsistency = "serial")
  static class SerialConsistencyTable {}

  @UDT(name = "ID_TYPE")
  static class IdType {}

  @Accessor
  interface SerialConsistencyAccessor {
    @Query("SELECT * FROM ks.t")
    @QueryParameters(consistency = "serial")
    ResultSet all();
  }

  /**
   * The {@code @Table} name is folded and then handed straight to {@code getTable}, so an unpinned
   * fold makes the mapper query a dotless table name that cannot match the schema.
   */
  @Test(groups = "unit")
  public void should_fold_table_name_in_any_default_locale() {
    MappingManager manager = managerReporting(mock(KeyspaceMetadata.class));

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(TURKISH);
      assertThat(parseEntityFailure(IdTable.class, manager))
          .contains("Table or materialized view id_table does not exist");
    } finally {
      Locale.setDefault(def);
    }
  }

  /**
   * Both {@code @Table} consistency levels are upper-cased before {@code valueOf}. Unpinned, a
   * Turkish locale turns "serial" into SERİAL, which is not a constant — reaching the table lookup
   * at all is what proves the two parses survived.
   */
  @Test(groups = "unit")
  public void should_parse_table_consistency_levels_in_any_default_locale() {
    MappingManager manager = managerReporting(mock(KeyspaceMetadata.class));

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(TURKISH);
      assertThat(parseEntityFailure(SerialConsistencyTable.class, manager))
          .contains("Table or materialized view t does not exist");
    } finally {
      Locale.setDefault(def);
    }
  }

  /** The {@code @UDT} name reaches {@code getUserType} the same way the table name does. */
  @Test(groups = "unit")
  public void should_fold_udt_name_in_any_default_locale() {
    MappingManager manager = managerReporting(mock(KeyspaceMetadata.class));

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(TURKISH);
      assertThat(parseUdtFailure(IdType.class, manager))
          .contains("User type id_type does not exist");
    } finally {
      Locale.setDefault(def);
    }
  }

  /**
   * The accessor's {@code @QueryParameters} consistency is a third, independent {@code valueOf}
   * call site, and it is reached without touching the session at all.
   */
  @Test(groups = "unit")
  public void should_parse_accessor_consistency_in_any_default_locale() {
    MappingManager manager = mock(MappingManager.class);

    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(TURKISH);
      assertThat(AnnotationParser.parseAccessor(SerialConsistencyAccessor.class, manager))
          .isNotNull();
    } finally {
      Locale.setDefault(def);
    }
  }

  /** A manager whose cluster reports the given keyspace, and no tables or user types in it. */
  private static MappingManager managerReporting(KeyspaceMetadata keyspace) {
    Metadata metadata = mock(Metadata.class);
    when(metadata.getKeyspace(KEYSPACE)).thenReturn(keyspace);
    Cluster cluster = mock(Cluster.class);
    when(cluster.getMetadata()).thenReturn(metadata);
    Session session = mock(Session.class);
    when(session.getCluster()).thenReturn(cluster);
    MappingManager manager = mock(MappingManager.class);
    when(manager.getSession()).thenReturn(session);
    return manager;
  }

  /** Returns the message of the failure the missing table raises, which carries the folded name. */
  private static String parseEntityFailure(Class<?> entityClass, MappingManager manager) {
    try {
      AnnotationParser.parseEntity(entityClass, KEYSPACE, manager);
      throw new AssertionError("expected the parse to fail on the missing table");
    } catch (IllegalArgumentException e) {
      return e.getMessage();
    }
  }

  /** As above, for the missing user type. */
  private static String parseUdtFailure(Class<?> udtClass, MappingManager manager) {
    try {
      AnnotationParser.parseUDT(udtClass, KEYSPACE, manager);
      throw new AssertionError("expected the parse to fail on the missing user type");
    } catch (IllegalArgumentException e) {
      return e.getMessage();
    }
  }
}
