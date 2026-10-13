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
package com.datastax.dse.driver.api.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveSession;
import com.datastax.dse.driver.api.core.data.time.DateRangeBound;
import com.datastax.dse.driver.api.core.type.codec.DseTypeCodecs;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.concurrent.CompletionException;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class RemainingDseApiCompatibilityTest {

  @Test
  public void should_keep_legacy_signatures_and_local_value_helpers() throws Exception {
    assertThat(ContinuousSession.class).isAssignableFrom(CqlSession.class);
    assertThat(ContinuousReactiveSession.class).isAssignableFrom(CqlSession.class);
    assertThat(ProtocolVersion.DSE_V1).isSameAs(DseProtocolVersion.DSE_V1);
    assertThat(TypedDriverOption.GRAPH_NAME.getRawOption()).isSameAs(DseDriverOption.GRAPH_NAME);
    assertThat(Version.parse("4.19.2.3").getDSEPatch()).isEqualTo(3);
    assertThat(DateRangeBound.parseLowerBound("2020-01-01")).isNotNull();
  }

  @Test
  public void should_fail_fast_for_unsupported_dse_execution() {
    ContinuousSession continuousSession = mock(ContinuousSession.class, CALLS_REAL_METHODS);
    ContinuousReactiveSession reactiveSession =
        mock(ContinuousReactiveSession.class, CALLS_REAL_METHODS);
    SimpleStatement statement = SimpleStatement.newInstance("SELECT 1");

    assertThatThrownBy(() -> continuousSession.executeContinuously(statement))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> reactiveSession.executeContinuouslyReactive(statement))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseSession.builder().buildAsync().toCompletableFuture().join())
        .isInstanceOf(CompletionException.class)
        .hasCauseInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> DseTypeCodecs.DATE_RANGE.encode(null, ProtocolVersion.V4))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
