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
package com.datastax.oss.driver.internal.core.channel;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class RejectionSafeEventExecutorTest {

  @Test
  public void should_use_event_loops_parent_as_executor_group() {
    EventLoop eventLoop = mock(EventLoop.class);
    EventLoopGroup parent = mock(EventLoopGroup.class);
    when(eventLoop.parent()).thenReturn(parent);

    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);

    assertThat(executor.parent()).isSameAs(parent);
  }

  @Test
  public void should_trampoline_nested_tasks_when_event_loop_rejects_them() {
    EventLoop eventLoop = mock(EventLoop.class);
    doThrow(new RejectedExecutionException()).when(eventLoop).execute(any(Runnable.class));
    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);
    List<String> events = new ArrayList<>();

    executor.execute(
        () -> {
          events.add("outer start");
          executor.execute(() -> events.add("inner"));
          events.add("outer end");
        });

    assertThat(events).containsExactly("outer start", "outer end", "inner");
  }

  @Test
  public void should_run_task_on_event_loop_when_it_accepts_it() {
    EventLoop eventLoop = mock(EventLoop.class);
    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);
    Runnable task = () -> {};

    executor.execute(task);

    // The task is handed straight to the event loop; no trampolining
    verify(eventLoop).execute(task);
  }

  @Test
  public void should_delegate_event_loop_membership_checks() {
    EventLoop eventLoop = mock(EventLoop.class);
    Thread thread = Thread.currentThread();
    when(eventLoop.inEventLoop()).thenReturn(true);
    when(eventLoop.inEventLoop(thread)).thenReturn(false);
    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);

    assertThat(executor.inEventLoop()).isTrue();
    assertThat(executor.inEventLoop(thread)).isFalse();
  }

  @Test
  public void should_delegate_lifecycle_state() {
    EventLoop eventLoop = mock(EventLoop.class);
    when(eventLoop.isShuttingDown()).thenReturn(true);
    when(eventLoop.isShutdown()).thenReturn(true);
    when(eventLoop.isTerminated()).thenReturn(false);
    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);

    assertThat(executor.isShuttingDown()).isTrue();
    assertThat(executor.isShutdown()).isTrue();
    assertThat(executor.isTerminated()).isFalse();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void should_delegate_shutdown() {
    EventLoop eventLoop = mock(EventLoop.class);
    Future<?> gracefully = mock(Future.class);
    Future<?> termination = mock(Future.class);
    when(eventLoop.shutdownGracefully(1L, 2L, TimeUnit.SECONDS)).thenAnswer(i -> gracefully);
    when(eventLoop.terminationFuture()).thenAnswer(i -> termination);
    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);

    assertThat(executor.shutdownGracefully(1L, 2L, TimeUnit.SECONDS)).isSameAs(gracefully);
    assertThat(executor.terminationFuture()).isSameAs(termination);

    // The deprecated shutdown() is mapped onto the graceful variant
    executor.shutdown();
    verify(eventLoop).shutdownGracefully();
  }

  @Test
  public void should_delegate_await_termination() throws InterruptedException {
    EventLoop eventLoop = mock(EventLoop.class);
    when(eventLoop.awaitTermination(3L, TimeUnit.SECONDS)).thenReturn(true);
    RejectionSafeEventExecutor executor = new RejectionSafeEventExecutor(eventLoop);

    assertThat(executor.awaitTermination(3L, TimeUnit.SECONDS)).isTrue();
  }
}
