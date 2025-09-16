package com.datastax.oss.driver.internal.core.protocol;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.util.List;
import java.util.Map;

public class ProtocolFeatureStore {
  private static final AttributeKey<ProtocolFeatureStore> CHANNEL_KEY =
      AttributeKey.valueOf("protocol_feature_store");

  private final LwtInfo lwtInfo;
  private final ShardingInfo.ConnectionShardingInfo shardingInfo;
  private final TabletInfo tabletInfo;

  public static final ProtocolFeatureStore Empty = new ProtocolFeatureStore(null, null, null);

  ProtocolFeatureStore(
      LwtInfo lwtInfo, ShardingInfo.ConnectionShardingInfo shardingInfo, TabletInfo tabletInfo) {
    this.lwtInfo = lwtInfo;
    this.shardingInfo = shardingInfo;
    this.tabletInfo = tabletInfo;
  }

  public LwtInfo getLwtFeatureInfo() {
    return lwtInfo;
  }

  public ShardingInfo.ConnectionShardingInfo getShardingInfo() {
    return shardingInfo;
  }

  public TabletInfo getTabletFeatureInfo() {
    return tabletInfo;
  }

  public static ProtocolFeatureStore parseSupportedOptions(
      @NonNull Map<String, List<String>> options) {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(options);
    ShardingInfo.ConnectionShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(options);
    TabletInfo tabletInfo = TabletInfo.loadFromSupportedOptions(options);
    return new ProtocolFeatureStore(lwtInfo, shardingInfo, tabletInfo);
  }

  public void populateStartupOptions(@NonNull Map<String, String> options) {
    if (lwtInfo != null) {
      lwtInfo.populateStartupOptions(options);
    }
    if (tabletInfo != null && tabletInfo.isEnabled()) {
      TabletInfo.populateStartupOptions(options);
    }
  }

  public static ProtocolFeatureStore loadFromChannel(@NonNull Channel channel) {
    return channel.attr(ProtocolFeatureStore.CHANNEL_KEY).get();
  }

  public void storeInChannel(@NonNull Channel channel) {
    channel.attr(ProtocolFeatureStore.CHANNEL_KEY).set(this);
  }
}
