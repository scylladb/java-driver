/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datastax.oss.driver.internal.core.pool;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.protocol.ShardingInfo;
import io.netty.channel.ChannelPromise;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class ChannelPoolReshardTest extends ChannelPoolTestBase {

  @Test
  public void should_rebuild_pool_when_node_comes_back_with_more_shards() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofMillis(50));
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(1);

    ShardingInfo oneShard = newShardingInfo(1);
    ShardingInfo twoShards = newShardingInfo(2);
    DriverChannel channel1 = newMockDriverChannel(1, oneShard, 0);
    DriverChannel channel2 = newMockDriverChannel(2, twoShards, 1);
    DriverChannel channel3 = newMockDriverChannel(3, twoShards, 0);
    when(channelFactory.connect(eq(node), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel1))
        .thenReturn(CompletableFuture.completedFuture(channel2))
        .thenReturn(CompletableFuture.completedFuture(channel3));

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");
    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    ((ChannelPromise) channel1.closeFuture()).setSuccess();

    verify(eventBus, VERIFY_TIMEOUT.times(3)).fire(ChannelEvent.channelOpened(node));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));
    assertThat(pool.channels).hasSize(2);
    assertThat(pool.channels[0]).containsOnly(channel3);
    assertThat(pool.channels[1]).containsOnly(channel2);
    assertThat(node.getShardingInfo()).isSameAs(twoShards);
  }

  @Test
  public void should_close_old_channels_once_when_node_reports_fewer_shards() throws Exception {
    when(reconnectionSchedule.nextDelay()).thenReturn(Duration.ofMillis(50));
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(2);

    ShardingInfo twoShards = newShardingInfo(2);
    ShardingInfo oneShard = newShardingInfo(1);
    DriverChannel channel1 = newMockDriverChannel(1, twoShards, 0);
    DriverChannel channel2 = newMockDriverChannel(2, twoShards, 1);
    DriverChannel channel3 = newMockDriverChannel(3, oneShard, 0);
    DriverChannel channel4 = newMockDriverChannel(4, oneShard, 0);
    when(channelFactory.connect(eq(node), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel1))
        .thenReturn(CompletableFuture.completedFuture(channel2))
        .thenReturn(CompletableFuture.completedFuture(channel3))
        .thenReturn(CompletableFuture.completedFuture(channel4));

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");
    assertThatStage(poolFuture).isSuccess();
    ChannelPool pool = poolFuture.toCompletableFuture().get();

    // Only one of the two channels is lost, the other one still has the old layout
    ((ChannelPromise) channel2.closeFuture()).setSuccess();

    verify(eventBus, VERIFY_TIMEOUT.times(4)).fire(ChannelEvent.channelOpened(node));
    verify(eventBus, VERIFY_TIMEOUT).fire(ChannelEvent.reconnectionStopped(node));
    verify(channel1, VERIFY_TIMEOUT).close();
    assertThat(pool.channels).hasSize(1);
    assertThat(pool.channels[0]).containsOnly(channel3, channel4);

    // The old channel then goes through its shutdown, it must not be counted as closed again
    ((ChannelPromise) channel1.closeStartedFuture()).setSuccess();
    ((ChannelPromise) channel1.closeFuture()).setSuccess();

    verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelClosed(node));
    Thread.sleep(200);
    verify(eventBus, VERIFY_TIMEOUT.times(2)).fire(ChannelEvent.channelClosed(node));
    verify(channel3, never()).close();
    verify(channel4, never()).close();
  }

  private static ShardingInfo newShardingInfo(int shardsCount) {
    ShardingInfo shardingInfo = mock(ShardingInfo.class);
    when(shardingInfo.getShardsCount()).thenReturn(shardsCount);
    return shardingInfo;
  }

  private DriverChannel newMockDriverChannel(int id, ShardingInfo shardingInfo, int shardId) {
    DriverChannel channel = newMockDriverChannel(id);
    when(channel.getShardingInfo()).thenReturn(shardingInfo);
    when(channel.getShardId()).thenReturn(shardId);
    return channel;
  }
}
