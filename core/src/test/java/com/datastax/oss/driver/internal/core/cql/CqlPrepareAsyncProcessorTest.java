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

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import io.netty.util.Timer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link CqlPrepareAsyncProcessor} focusing on the caching behavior of {@link
 * CqlPrepareAsyncProcessor#process} with respect to defensive copies and weak-value retention.
 */
public class CqlPrepareAsyncProcessorTest {

  private CqlPrepareAsyncProcessor processor;
  private Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache;

  @Before
  public void setup() {
    processor = new CqlPrepareAsyncProcessor(Optional.empty());
    cache = processor.getCache();
  }

  /**
   * When the cached future is already completed, process() should return the exact same instance
   * (identity). This ensures callers hold a strong reference to the cached CF, preventing
   * weak-value eviction under GC pressure.
   */
  @Test
  public void should_return_cached_future_directly_when_already_completed() throws Exception {
    PrepareRequest request = new DefaultPrepareRequest("SELECT 1");
    PreparedStatement ps = Mockito.mock(PreparedStatement.class);

    // Pre-populate cache with a completed future
    CompletableFuture<PreparedStatement> completed = CompletableFuture.completedFuture(ps);
    cache.put(request, completed);

    // process() should return the exact same object
    CompletionStage<PreparedStatement> returned = processor.process(request, null, null, "test");

    assertThat(returned).isSameAs(completed);
  }

  /**
   * When the cached future is still in-flight (not yet done), process() should return a defensive
   * copy to protect the cache from cancellation by the caller.
   */
  @Test
  public void should_return_defensive_copy_when_future_is_in_flight() throws Exception {
    PrepareRequest request = new DefaultPrepareRequest("SELECT 1");

    // Pre-populate cache with an incomplete future
    CompletableFuture<PreparedStatement> inFlight = new CompletableFuture<>();
    cache.put(request, inFlight);

    CompletionStage<PreparedStatement> returned = processor.process(request, null, null, "test");

    // Should NOT be the same instance
    assertThat(returned).isNotSameAs(inFlight);

    // Cancelling the returned copy should NOT affect the cached future
    returned.toCompletableFuture().cancel(false);
    assertThat(inFlight.isCancelled()).isFalse();
  }

  @Test
  public void should_complete_and_invalidate_cached_future_if_handler_construction_fails() {
    PrepareRequest request = new DefaultPrepareRequest("SELECT 1");
    IllegalArgumentException failure = new IllegalArgumentException("mock timer failure");

    try (RequestHandlerTestHarness harness = RequestHandlerTestHarness.builder().build()) {
      // A construction failure must clear the cache so a later prepare can retry.
      Timer timer = mock(Timer.class);
      when(harness.getContext().getNettyOptions().getTimer()).thenReturn(timer);
      when(timer.newTimeout(any(), anyLong(), any())).thenThrow(failure);

      CompletionStage<PreparedStatement> first =
          processor.process(request, harness.getSession(), harness.getContext(), "test");

      assertThatStage(first).isFailed(error -> assertThat(error).isSameAs(failure));
      assertThat(cache.getIfPresent(request)).isNull();

      CompletionStage<PreparedStatement> retry =
          processor.process(request, harness.getSession(), harness.getContext(), "test");

      assertThatStage(retry).isFailed(error -> assertThat(error).isSameAs(failure));
      assertThat(cache.getIfPresent(request)).isNull();
    }
  }

  @Test
  public void should_match_udt_by_name_when_field_definitions_differ() {
    UserDefinedType oldType =
        new UserDefinedTypeBuilder("ks", "test_type_2")
            .withField("c", DataTypes.INT)
            .withField("d", DataTypes.TEXT)
            .build();
    UserDefinedType resultType =
        new UserDefinedTypeBuilder("ks", "test_type_2")
            .withField("c", DataTypes.INT)
            .withField("d", DataTypes.TEXT)
            .withField("i", DataTypes.BLOB)
            .build();

    assertThat(CqlPrepareAsyncProcessor.typeMatches(oldType, DataTypes.listOf(resultType)))
        .isTrue();
  }
}
