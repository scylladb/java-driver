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

import static com.datastax.oss.driver.internal.core.cql.PreparedStatementTestHelper.newPreparedStatement;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.RequestRoutingType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import org.junit.Test;

public class DefaultPreparedStatementTest {

  @Test
  public void should_not_keep_inferred_routing_type_after_bound_consistency_override() {
    DefaultPreparedStatement preparedStatement =
        newPreparedStatement(DefaultConsistencyLevel.LOCAL_SERIAL, null, null);

    assertThat(preparedStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);

    BoundStatement boundStatement =
        preparedStatement.bind().setConsistencyLevel(DefaultConsistencyLevel.ONE);

    assertThat(boundStatement.getConsistencyLevel()).isEqualTo(DefaultConsistencyLevel.ONE);
    assertThat(boundStatement.getRequestRoutingType()).isNull();
  }

  @Test
  public void should_not_infer_routing_type_from_prepared_serial_consistency_level_option() {
    DefaultPreparedStatement preparedStatement =
        newPreparedStatement(null, DefaultConsistencyLevel.LOCAL_SERIAL, null);

    assertThat(preparedStatement.getRequestRoutingType()).isNull();
    assertThat(preparedStatement.bind().getRequestRoutingType()).isNull();
  }

  @Test
  public void should_not_infer_routing_type_from_bound_serial_consistency_level_override() {
    DefaultPreparedStatement preparedStatement = newPreparedStatement(null, null, null);

    BoundStatement boundStatement =
        preparedStatement.bind().setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL);

    assertThat(boundStatement.getRequestRoutingType()).isNull();
  }

  @Test
  public void should_keep_detected_lwt_routing_type_after_bound_consistency_override() {
    DefaultPreparedStatement preparedStatement =
        newPreparedStatement(DefaultConsistencyLevel.LOCAL_SERIAL, null, RequestRoutingType.LWT);

    BoundStatement boundStatement =
        preparedStatement.bind().setConsistencyLevel(DefaultConsistencyLevel.ONE);

    assertThat(boundStatement.getRequestRoutingType()).isEqualTo(RequestRoutingType.LWT);
  }
}
