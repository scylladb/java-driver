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

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.AbstractEventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Runs promise listeners on their channel's event loop in the normal case, but falls back to the
 * completing thread when that event loop rejects the notification task during shutdown.
 */
class RejectionSafeEventExecutor extends AbstractEventExecutor {
  private final EventLoop eventLoop;

  RejectionSafeEventExecutor(EventLoop eventLoop) {
    super(eventLoop.parent());
    this.eventLoop = eventLoop;
  }

  @Override
  public boolean inEventLoop() {
    return eventLoop.inEventLoop();
  }

  @Override
  public boolean inEventLoop(Thread thread) {
    return eventLoop.inEventLoop(thread);
  }

  @Override
  public void execute(Runnable command) {
    try {
      eventLoop.execute(command);
    } catch (RejectedExecutionException e) {
      ImmediateEventExecutor.INSTANCE.execute(command);
    }
  }

  @Override
  public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
    return eventLoop.shutdownGracefully(quietPeriod, timeout, unit);
  }

  @Override
  public Future<?> terminationFuture() {
    return eventLoop.terminationFuture();
  }

  @Override
  @Deprecated
  public void shutdown() {
    eventLoop.shutdownGracefully();
  }

  @Override
  public boolean isShuttingDown() {
    return eventLoop.isShuttingDown();
  }

  @Override
  public boolean isShutdown() {
    return eventLoop.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return eventLoop.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return eventLoop.awaitTermination(timeout, unit);
  }
}
