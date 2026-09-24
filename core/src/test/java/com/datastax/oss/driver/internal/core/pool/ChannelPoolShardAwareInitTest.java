package com.datastax.oss.driver.internal.core.pool;

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.protocol.ShardingInfo;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class ChannelPoolShardAwareInitTest extends ChannelPoolTestBase {

  @Before
  @Override
  public void setup() {
    super.setup();
    when(defaultProfile.getBoolean(DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED))
        .thenReturn(true);
    when(defaultProfile.getInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_LOW))
        .thenReturn(10000);
    when(defaultProfile.getInt(DefaultDriverOption.ADVANCED_SHARD_AWARENESS_PORT_HIGH))
        .thenReturn(60000);
  }

  @Test
  public void should_initialize_when_all_channels_succeed() throws Exception {
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE)).thenReturn(4);
    int shardsPerNode = 2;
    DriverChannel channel1 = newMockDriverChannel(1);
    DriverChannel channel2 = newMockDriverChannel(2);
    DriverChannel channel3 = newMockDriverChannel(3);
    DriverChannel channel4 = newMockDriverChannel(4);

    ShardingInfo shardingInfo = mock(ShardingInfo.class);
    when(shardingInfo.getShardsCount()).thenReturn(shardsPerNode);
    node.setShardingInfo(shardingInfo);

    when(channel1.getShardingInfo()).thenReturn(shardingInfo);
    when(channel2.getShardingInfo()).thenReturn(shardingInfo);
    when(channel3.getShardingInfo()).thenReturn(shardingInfo);
    when(channel4.getShardingInfo()).thenReturn(shardingInfo);

    when(channel1.getShardId()).thenReturn(0);
    when(channel2.getShardId()).thenReturn(0);
    when(channel3.getShardId()).thenReturn(1);
    when(channel4.getShardId()).thenReturn(1);

    when(channelFactory.connect(eq(node), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel1));
    when(channelFactory.connect(eq(node), eq(0), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel2));
    when(channelFactory.connect(eq(node), eq(1), any(DriverChannelOptions.class)))
        .thenReturn(CompletableFuture.completedFuture(channel3))
        .thenReturn(CompletableFuture.completedFuture(channel4));

    CompletionStage<ChannelPool> poolFuture =
        ChannelPool.init(node, null, NodeDistance.LOCAL, context, "test");

    ArgumentCaptor<DriverChannelOptions> optionsCaptor =
        ArgumentCaptor.forClass(DriverChannelOptions.class);
    InOrder inOrder = Mockito.inOrder(channelFactory);
    inOrder
        .verify(channelFactory, timeout(500).atLeast(1))
        .connect(eq(node), optionsCaptor.capture());
    int num = optionsCaptor.getAllValues().size();
    assertThat(num).isEqualTo(1);
    inOrder
        .verify(channelFactory, timeout(500).atLeast(3))
        .connect(eq(node), anyInt(), optionsCaptor.capture());
    int num2 = optionsCaptor.getAllValues().size();
    assertThat(num2).isEqualTo(4);

    assertThatStage(poolFuture)
        .isSuccess(
            pool -> {
              assertThat(pool.channels[0]).containsOnly(channel1, channel2);
              assertThat(pool.channels[1]).containsOnly(channel3, channel4);
            });
    verify(eventBus, VERIFY_TIMEOUT.times(4)).fire(ChannelEvent.channelOpened(node));

    inOrder
        .verify(channelFactory, timeout(500).times(0))
        .connect(any(Node.class), any(DriverChannelOptions.class));
    inOrder
        .verify(channelFactory, timeout(500).times(0))
        .connect(any(Node.class), anyInt(), any(DriverChannelOptions.class));
  }
}
