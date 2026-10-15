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
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import io.netty.bootstrap.Bootstrap;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

/** What a caller of {@link ChannelFactory#connect} can rely on whatever the bootstrap does. */
public class ChannelFactoryConnectTest extends ChannelFactoryTestBase {

  @Test
  public void should_fail_the_stage_rather_than_throw_when_the_bootstrap_hook_throws() {
    // Given -- the hook is user code
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    doThrow(new IllegalStateException("hook failure"))
        .when(nettyOptions)
        .afterBootstrapInitialized(any(Bootstrap.class));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS,
            null,
            null,
            DriverChannelOptions.DEFAULT,
            NoopNodeMetricUpdater.INSTANCE);

    // Then -- a throw at the caller would lose the attempt, or hang a caller that only waits on
    // the stage (the control connection's contact-point expansion does)
    assertThatStage(channelFuture)
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("hook failure"));
  }
}
