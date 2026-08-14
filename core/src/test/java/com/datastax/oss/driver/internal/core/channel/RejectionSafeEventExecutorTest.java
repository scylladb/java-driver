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

import io.netty.channel.EventLoop;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Test;

public class RejectionSafeEventExecutorTest {

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
}
