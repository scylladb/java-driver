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

import com.datastax.oss.driver.api.core.context.DriverContext;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.jcip.annotations.ThreadSafe;

/** No-op implementation of the write coalescer: each write is flushed immediately. */
@ThreadSafe
public class PassThroughWriteCoalescer implements WriteCoalescer {

  private final ConcurrentMap<EventLoop, RejectionSafeEventExecutor> listenerExecutors =
      new ConcurrentHashMap<>();

  public PassThroughWriteCoalescer(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public EventExecutor listenerNotificationExecutor(Channel channel) {
    return listenerExecutors.computeIfAbsent(channel.eventLoop(), RejectionSafeEventExecutor::new);
  }

  @Override
  public ChannelFuture writeAndFlush(Channel channel, Object message) {
    ChannelPromise promise =
        new DefaultChannelPromise(channel, listenerNotificationExecutor(channel));
    return channel.writeAndFlush(message, promise);
  }
}
