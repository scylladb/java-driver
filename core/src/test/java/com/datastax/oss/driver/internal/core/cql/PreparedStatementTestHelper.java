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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.util.Collections;

/** Builds minimally-valid {@link DefaultPreparedStatement} instances for tests. */
public class PreparedStatementTestHelper {

  /** Returns a statement with no consistency levels and no routing type configured. */
  public static DefaultPreparedStatement newPreparedStatement() {
    return newPreparedStatement(null, null, null);
  }

  public static DefaultPreparedStatement newPreparedStatement(
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel serialConsistencyLevel,
      RequestRoutingType requestRoutingType) {
    ColumnDefinitions variableDefinitions =
        DefaultColumnDefinitions.valueOf(Collections.emptyList());
    return new DefaultPreparedStatement(
        Bytes.fromHexString("0x"),
        "SELECT * FROM test.foo WHERE pk = ?",
        variableDefinitions,
        Collections.emptyList(),
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        null,
        null,
        Integer.MIN_VALUE,
        consistencyLevel,
        serialConsistencyLevel,
        false,
        CodecRegistry.DEFAULT,
        DefaultProtocolVersion.DEFAULT,
        requestRoutingType);
  }

  private PreparedStatementTestHelper() {}
}
