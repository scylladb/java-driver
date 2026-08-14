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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ChannelHandlerRequestTest {

  @Mock private ChannelHandlerContext context;
  @Mock private Channel channel;
  @Mock private ChannelPipeline pipeline;
  @Mock private EventLoop eventLoop;
  @Mock private InFlightHandler inFlightHandler;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(context.channel()).thenReturn(channel);
    when(context.pipeline()).thenReturn(pipeline);
    when(pipeline.get(InFlightHandler.class)).thenReturn(inFlightHandler);
    when(channel.eventLoop()).thenReturn(eventLoop);
    when(eventLoop.inEventLoop()).thenReturn(true);
    when(inFlightHandler.preAcquireId()).thenReturn(true);
  }

  @Test
  public void should_cancel_pre_acquired_id_when_request_construction_fails() {
    RuntimeException failure = new RuntimeException("mock failure");
    TestRequest request = new TestRequest(failure);

    assertThatThrownBy(request::send).isSameAs(failure);

    verify(inFlightHandler).cancelPreAcquireId();
    verify(channel, never()).writeAndFlush(any());
  }

  @Test
  public void should_cancel_pre_acquired_id_when_raw_write_throws() {
    RuntimeException failure = new RuntimeException("mock failure");
    TestRequest request = new TestRequest(null);
    doThrow(failure).when(channel).writeAndFlush(any());

    assertThatThrownBy(request::send).isSameAs(failure);

    verify(inFlightHandler).cancelPreAcquireId();
  }

  @Test
  public void should_cancel_pre_acquired_id_when_raw_write_fails_asynchronously() {
    RuntimeException failure = new RuntimeException("mock failure");
    TestRequest request = new TestRequest(null);
    request.expectFailure = true;
    ChannelPromise writePromise =
        new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
    when(channel.writeAndFlush(any())).thenReturn(writePromise);

    request.send();
    writePromise.setFailure(failure);

    verify(inFlightHandler).cancelPreAcquireId();
    assertThat(request.failureCause).isSameAs(failure);
  }

  private class TestRequest extends ChannelHandlerRequest {

    private final RuntimeException requestFailure;
    private boolean expectFailure;
    private Throwable failureCause;

    private TestRequest(RuntimeException requestFailure) {
      super(context, 1000);
      this.requestFailure = requestFailure;
    }

    @Override
    String describe() {
      return "test request";
    }

    @Override
    Message getRequest() {
      if (requestFailure != null) {
        throw requestFailure;
      }
      return new Query("mock query");
    }

    @Override
    void onResponse(Message response) {}

    @Override
    void fail(String message, Throwable cause) {
      if (!expectFailure) {
        throw new AssertionError("Unexpected failure callback", cause);
      }
      failureCause = cause;
    }
  }
}
