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
/*
 * Copyright (C) 2024 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DefaultSchemaQueriesFactoryTest {

  enum Expected {
    CASS_21(Cassandra21SchemaQueries.class),
    CASS_22(Cassandra22SchemaQueries.class),
    CASS_3(Cassandra3SchemaQueries.class),
    CASS_4(Cassandra4SchemaQueries.class);

    final Class<? extends SchemaQueries> clz;

    Expected(Class<? extends SchemaQueries> clz) {
      this.clz = clz;
    }

    public Class<? extends SchemaQueries> getClz() {
      return clz;
    }
  }

  private static ImmutableList<ImmutableList<Object>> cassandraVersions =
      ImmutableList.<ImmutableList<Object>>builder()
          .add(ImmutableList.of("2.1.0", Optional.empty(), Expected.CASS_21))
          .add(ImmutableList.of("2.2.0", Optional.empty(), Expected.CASS_22))
          .add(ImmutableList.of("2.2.1", Optional.empty(), Expected.CASS_22))
          // Not a real version, just documenting behaviour of existing impl
          .add(ImmutableList.of("2.3.0", Optional.empty(), Expected.CASS_22))
          // We now return you to real versions
          .add(ImmutableList.of("3.0.0", Optional.empty(), Expected.CASS_3))
          .add(ImmutableList.of("3.0.1", Optional.empty(), Expected.CASS_3))
          .add(ImmutableList.of("3.1.0", Optional.empty(), Expected.CASS_3))
          .add(ImmutableList.of("4.0.0", Optional.empty(), Expected.CASS_4))
          .add(ImmutableList.of("4.0.1", Optional.empty(), Expected.CASS_4))
          .add(ImmutableList.of("4.1.0", Optional.empty(), Expected.CASS_4))
          .build();

  @DataProvider(format = "%m %p[1] => %p[0]")
  public static Iterable<?> expected() {

    return cassandraVersions;
  }

  @Test
  @UseDataProvider("expected")
  public void should_return_correct_schema_queries_impl(
      String cassandraVersion, Optional<String> ignored, Expected expected) {

    final Node mockNode = mock(Node.class);
    when(mockNode.getCassandraVersion()).thenReturn(Version.parse(cassandraVersion));
    DefaultSchemaQueriesFactory factory = buildFactory();

    @SuppressWarnings("unchecked")
    SchemaQueries queries = factory.newInstance(mockNode, mock(DriverChannel.class));

    assertThat(queries.getClass()).isEqualTo(expected.getClz());
  }

  private DefaultSchemaQueriesFactory buildFactory() {

    final DriverExecutionProfile mockProfile = mock(DriverExecutionProfile.class);
    final DriverConfig mockConfig = mock(DriverConfig.class);
    when(mockConfig.getDefaultProfile()).thenReturn(mockProfile);
    final InternalDriverContext mockInternalCtx = mock(InternalDriverContext.class);
    when(mockInternalCtx.getConfig()).thenReturn(mockConfig);
    when(mockProfile.getDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT))
        .thenReturn(Duration.of(5, ChronoUnit.SECONDS));

    return new DefaultSchemaQueriesFactory(mockInternalCtx);
  }
}
