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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PassThroughWriteCoalescerTest {

  @Mock private Channel channel;
  @Mock private EventLoop eventLoop;
  @Mock private EventLoopGroup eventLoopGroup;

  @Test
  public void should_notify_write_listener_when_event_loop_rejects_notification() {
    Object message = new Object();
    RejectedExecutionException failure = new RejectedExecutionException("mock failure");
    PassThroughWriteCoalescer coalescer = new PassThroughWriteCoalescer(null);

    when(channel.eventLoop()).thenReturn(eventLoop);
    when(eventLoop.parent()).thenReturn(eventLoopGroup);
    when(eventLoop.inEventLoop()).thenReturn(false);
    doThrow(failure).when(eventLoop).execute(any(Runnable.class));
    doAnswer(
            invocation -> {
              ChannelPromise promise = invocation.getArgument(1);
              promise.setFailure(failure);
              return promise;
            })
        .when(channel)
        .writeAndFlush(eq(message), any(ChannelPromise.class));

    ChannelFuture writeFuture = coalescer.writeAndFlush(channel, message);
    AtomicReference<Throwable> listenerFailure = new AtomicReference<>();
    writeFuture.addListener(future -> listenerFailure.set(future.cause()));

    assertThat(writeFuture).isFailed(error -> assertThat(error).isSameAs(failure));
    assertThat(listenerFailure.get()).isSameAs(failure);
    verify(channel).writeAndFlush(message, (ChannelPromise) writeFuture);
  }
}
