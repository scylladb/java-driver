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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.fail;

import com.datastax.oss.driver.api.core.DriverExecutionException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

public class CompletableFuturesTest {
  @Test
  public void should_not_suppress_identical_exceptions() throws Exception {
    RuntimeException error = new RuntimeException();
    CompletableFuture<Void> future1 = new CompletableFuture<>();
    future1.completeExceptionally(error);
    CompletableFuture<Void> future2 = new CompletableFuture<>();
    future2.completeExceptionally(error);
    try {
      // if timeout exception is thrown, it indicates that CompletableFutures.allSuccessful()
      // did not complete the returned future and potentially caller will wait infinitely
      CompletableFutures.allSuccessful(Arrays.asList(future1, future2))
          .toCompletableFuture()
          .get(1, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause()).isEqualTo(error);
    }
  }

  @Test
  public void should_get_uninterruptibly_with_timeout_on_completed_future() {
    CompletableFuture<String> future = CompletableFuture.completedFuture("result");
    String result = CompletableFutures.getUninterruptibly(future, Duration.ofSeconds(1));
    assertThat(result).isEqualTo("result");
  }

  @Test
  public void should_timeout_on_incomplete_future() {
    CompletableFuture<String> future = new CompletableFuture<>();
    assertThatThrownBy(() -> CompletableFutures.getUninterruptibly(future, Duration.ofMillis(100)))
        .isInstanceOf(DriverExecutionException.class)
        .hasCauseInstanceOf(TimeoutException.class);
  }

  @Test
  public void should_propagate_exception_with_timeout() {
    CompletableFuture<String> future = new CompletableFuture<>();
    RuntimeException error = new RuntimeException("test error");
    future.completeExceptionally(error);
    assertThatThrownBy(() -> CompletableFutures.getUninterruptibly(future, Duration.ofSeconds(1)))
        .isInstanceOf(DriverExecutionException.class)
        .hasCause(error);
  }
}
