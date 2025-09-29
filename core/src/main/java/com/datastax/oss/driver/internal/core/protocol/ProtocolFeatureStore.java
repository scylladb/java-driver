package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.protocol.internal.ProtocolFeatures;
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
  private final boolean metadataIdEnabled;
  private ProtocolFeatures protocolFeatures;

  public static final ProtocolFeatureStore EMPTY =
      new ProtocolFeatureStore(null, null, null, false);

  ProtocolFeatureStore(
      LwtInfo lwtInfo,
      ShardingInfo.ConnectionShardingInfo shardingInfo,
      TabletInfo tabletInfo,
      boolean metadataIdEnabled) {
    this.lwtInfo = lwtInfo;
    this.shardingInfo = shardingInfo;
    this.tabletInfo = tabletInfo;
    this.metadataIdEnabled = metadataIdEnabled;
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

  public boolean isMetadataIdEnabled() {
    return metadataIdEnabled;
  }

  public static ProtocolFeatureStore parseSupportedOptions(
      @NonNull Map<String, List<String>> options) {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(options);
    ShardingInfo.ConnectionShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(options);
    TabletInfo tabletInfo = TabletInfo.loadFromSupportedOptions(options);
    boolean metadataIdEnabled = MetadataIdInfo.loadFromSupportedOptions(options);
    return new ProtocolFeatureStore(lwtInfo, shardingInfo, tabletInfo, metadataIdEnabled);
  }

  public void populateStartupOptions(@NonNull Map<String, String> options) {
    if (lwtInfo != null) {
      lwtInfo.populateStartupOptions(options);
    }
    if (tabletInfo != null && tabletInfo.isEnabled()) {
      TabletInfo.populateStartupOptions(options);
    }
    if (metadataIdEnabled) {
      MetadataIdInfo.populateStartupOptions(options);
    }
  }

  public static ProtocolFeatureStore loadFromChannel(@NonNull Channel channel) {
    return channel.attr(ProtocolFeatureStore.CHANNEL_KEY).get();
  }

  public void storeInChannel(@NonNull Channel channel) {
    channel.attr(ProtocolFeatureStore.CHANNEL_KEY).set(this);
  }

  public ProtocolFeatures getProtocolFeatures() {
    if (protocolFeatures == null) {
      protocolFeatures = buildProtocolFeatures();
    }
    return protocolFeatures;
  }

  private ProtocolFeatures buildProtocolFeatures() {
    if (metadataIdEnabled) {
      return new ProtocolFeatures.Builder().setScyllaUseMetadataId().build();
    } else {
      return ProtocolFeatures.EMPTY;
    }
  }
}
