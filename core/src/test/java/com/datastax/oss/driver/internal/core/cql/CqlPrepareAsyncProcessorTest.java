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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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

  /**
   * The anchor is what keeps a weakly-held cache entry alive: after a successful prepare through
   * process(), the entry survives GC while the application holds the statement, even though it
   * dropped the stage process() returned. An unanchored entry collected in the same run proves GC
   * actually happened.
   */
  @Test
  public void should_keep_cache_entry_alive_while_prepared_statement_is_held() {
    PrepareRequest request = new DefaultPrepareRequest("SELECT 1");
    PrepareRequest controlRequest = new DefaultPrepareRequest("SELECT 2");
    Node node = Mockito.mock(Node.class);

    try (RequestHandlerTestHarness harness =
        RequestHandlerTestHarness.builder()
            .withResponse(node, CqlRequestHandlerTestBase.defaultFrameOf(prepared()))
            .build()) {

      PreparedStatement ps = prepareAndDropStage(request, harness);
      cache.put(
          controlRequest,
          CompletableFuture.completedFuture(PreparedStatementTestHelper.newPreparedStatement()));

      awaitGarbageCollection(() -> cache.getIfPresent(controlRequest) == null);

      assertThat(cache.getIfPresent(request)).isCompletedWithValue(ps);
    }
  }

  /**
   * The reverse: once the statement becomes unreachable the whole cycle is collectible, so the
   * anchor cannot turn the cache into a leak.
   */
  @Test
  public void should_evict_cache_entry_when_prepared_statement_is_unreachable() throws Exception {
    PrepareRequest request = new DefaultPrepareRequest("SELECT 1");

    // Wrapping in a WeakReference lets us drop the statement without keeping it in a local.
    WeakReference<DefaultPreparedStatement> ps = new WeakReference<>(anchorNewEntry(request));

    awaitGarbageCollection(() -> ps.get() == null);

    cache.cleanUp();
    assertThat(cache.getIfPresent(request)).isNull();
  }

  /**
   * Prepares through the real success path of {@link CqlPrepareAsyncProcessor#process}, keeping
   * only the statement, as applications routinely do.
   */
  private PreparedStatement prepareAndDropStage(
      PrepareRequest request, RequestHandlerTestHarness harness) {
    CompletableFuture<PreparedStatement> stage =
        processor
            .process(request, harness.getSession(), harness.getContext(), "test")
            .toCompletableFuture();
    assertThat(stage).isDone();
    return stage.join();
  }

  /**
   * Reproduces what {@link CqlPrepareAsyncProcessor#process} does on a successful prepare: cache
   * the future, anchor it on the resulting statement, then complete it. Once this frame returns,
   * the future is reachable only through the returned statement's anchor.
   */
  private DefaultPreparedStatement anchorNewEntry(PrepareRequest request) {
    CompletableFuture<PreparedStatement> cachedFuture = new CompletableFuture<>();
    cache.put(request, cachedFuture);

    DefaultPreparedStatement ps = PreparedStatementTestHelper.newPreparedStatement();
    ps.setPrepareCacheAnchor(cachedFuture);
    cachedFuture.complete(ps);
    return ps;
  }

  /** Requests GC until {@code collected} holds, failing if it does not within a bounded time. */
  private void awaitGarbageCollection(Callable<Boolean> collected) {
    await()
        .atMost(10, TimeUnit.SECONDS)
        .pollInterval(50, TimeUnit.MILLISECONDS)
        .until(
            () -> {
              System.gc();
              cache.cleanUp();
              return collected.call();
            });
  }

  private static Prepared prepared() {
    RowsMetadata noColumns = new RowsMetadata(Collections.emptyList(), null, new int[] {}, null);
    return new Prepared(Bytes.fromHexString("0xffff").array(), null, noColumns, noColumns);
  }
}
