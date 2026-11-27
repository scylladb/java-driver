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
package com.datastax.oss.driver.api.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.junit.Test;

public class BoundStatementBuilderTest {

  @Test
  public void should_not_modify_built_statements_when_builder_is_reused() {
    ColumnDefinitions variableDefinitions = mock(ColumnDefinitions.class);
    ColumnDefinition variableDefinition = mock(ColumnDefinition.class);
    when(variableDefinitions.size()).thenReturn(1);
    when(variableDefinitions.get(0)).thenReturn(variableDefinition);
    when(variableDefinition.getType()).thenReturn(DataTypes.INT);
    BoundStatementBuilder builder =
        new BoundStatementBuilder(
            mock(PreparedStatement.class),
            variableDefinitions,
            new ByteBuffer[1],
            null,
            null,
            null,
            null,
            null,
            Collections.emptyMap(),
            null,
            false,
            Statement.NO_DEFAULT_TIMESTAMP,
            null,
            5000,
            null,
            null,
            null,
            CodecRegistry.DEFAULT,
            ProtocolVersion.DEFAULT,
            null);

    BoundStatement first = builder.setInt(0, 1).build();
    BoundStatement second = builder.setInt(0, 2).build();

    assertThat(first.getInt(0)).isEqualTo(1);
    assertThat(second.getInt(0)).isEqualTo(2);
  }
}
