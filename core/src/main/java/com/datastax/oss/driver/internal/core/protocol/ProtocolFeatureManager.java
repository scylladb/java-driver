package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import io.netty.channel.Channel;
import java.util.List;
import java.util.Map;

/**
 * <code>ProtocolFeatureManager</code> is a helper class storing and managing protocol related info
 * such as:
 *
 * <ul>
 *   <li>{@link ShardingInfo}
 *   <li>{@link LwtInfo}
 *   <li>{@link TabletInfo}
 *   <li>whether any of {@link com.datastax.oss.protocol.internal.ProtocolFeatures.Feature} is
 *       negotiated
 * </ul>
 */
public class ProtocolFeatureManager {
  private final Map<String, List<String>> options;
  private final ShardingInfo.ConnectionShardingInfo shardingInfo;
  private final LwtInfo lwtInfo;
  private final TabletInfo tabletInfo;
  private final boolean metadataIdEnabled;

  public ProtocolFeatureManager(
      Map<String, List<String>> options,
      ShardingInfo.ConnectionShardingInfo shardingInfo,
      LwtInfo lwtInfo,
      TabletInfo tabletInfo,
      boolean metadataIdEnabled) {
    this.options = options;
    this.shardingInfo = shardingInfo;
    this.lwtInfo = lwtInfo;
    this.tabletInfo = tabletInfo;
    this.metadataIdEnabled = metadataIdEnabled;
  }

  public Map<String, List<String>> getOptions() {
    return options;
  }

  public ShardingInfo.ConnectionShardingInfo getShardingInfo() {
    return shardingInfo;
  }

  public LwtInfo getLwtInfo() {
    return lwtInfo;
  }

  public TabletInfo getTabletInfo() {
    return tabletInfo;
  }

  public boolean isMetadataIdEnabled() {
    return metadataIdEnabled;
  }

  public void updateOptionsAttributeForChannel(Channel channel) {
    if (channel != null) {
      channel.attr(DriverChannel.OPTIONS_KEY).set(options);
    }
  }

  public void updateShardingInfoAttributeForChannel(Channel channel) {
    if (shardingInfo != null) {
      channel.attr(DriverChannel.SHARDING_INFO_KEY).set(shardingInfo);
    }
  }

  public void updateLwtInfoAttributeForChannel(Channel channel) {
    if (lwtInfo != null) {
      channel.attr(DriverChannel.LWT_INFO_KEY).set(lwtInfo);
    }
  }

  public void optionallyAddLwtInfoOption(Map<String, String> options) {
    if (lwtInfo != null) {
      lwtInfo.addOption(options);
    }
  }

  public void optionallyAddMetadataIdOption(Map<String, String> options) {
    if (metadataIdEnabled) {
      MetadataIdInfo.addOption(options);
    }
  }

  public void optionallyAddTabletInfoOption(Map<String, String> options) {
    if (tabletInfo != null && tabletInfo.isEnabled()) {
      TabletInfo.addOption(options);
    }
  }
}
