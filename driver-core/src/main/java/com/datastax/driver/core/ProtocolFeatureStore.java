/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.util.List;
import java.util.Map;

public class ProtocolFeatureStore {

  public static final AttributeKey<ProtocolFeatureStore> CHANNEL_KEY =
      AttributeKey.valueOf("com.datastax.driver.core.ProtocolFeatureStore");

  /** Instance of {@link ProtocolFeatureStore} initialized with default values. */
  public static final ProtocolFeatureStore EMPTY =
      new ProtocolFeatureStore(null, null, null, false);

  private final ShardingInfo.ConnectionShardingInfo connectionShardingInfo;
  private final LwtInfo lwtInfo;
  private final TabletInfo tabletInfo;
  private final boolean useMetadataId;

  public ProtocolFeatureStore(
      ShardingInfo.ConnectionShardingInfo connectionShardingInfo,
      LwtInfo lwtInfo,
      TabletInfo tabletInfo,
      boolean useMetadataId) {
    this.connectionShardingInfo = connectionShardingInfo;
    this.lwtInfo = lwtInfo;
    this.tabletInfo = tabletInfo;
    this.useMetadataId = useMetadataId;
  }

  public static ProtocolFeatureStore parseSupportedOptions(Map<String, List<String>> supported) {
    ShardingInfo.ConnectionShardingInfo connectionShardingInfo =
        ShardingInfo.parseShardingInfo(supported);
    LwtInfo lwtInfo = LwtInfo.parseLwtInfo(supported);
    TabletInfo tabletInfo = TabletInfo.parseTabletInfo(supported);
    boolean metadataIdSupported = MetadataIdInfo.parseUseMetadataId(supported);
    return new ProtocolFeatureStore(
        connectionShardingInfo, lwtInfo, tabletInfo, metadataIdSupported);
  }

  public void populateStartupOptions(ProtocolVersion protocolVersion, Map<String, String> options) {
    if (lwtInfo != null) {
      lwtInfo.addOption(options);
    }
    if (tabletInfo != null
        && tabletInfo.isEnabled()
        && ProtocolFeatures.CUSTOM_PAYLOADS.isSupportedBy(protocolVersion)) {
      TabletInfo.addOption(options);
    }
    if (useMetadataId) {
      MetadataIdInfo.addOption(options);
    }
  }

  /**
   * Stores features in a {@link Host}.
   *
   * @param channel an instance of {@link Channel}
   */
  public void storeInChannel(Channel channel) {

    channel.attr(ProtocolFeatureStore.CHANNEL_KEY).set(this);
  }

  public static ProtocolFeatureStore loadFromChannel(Channel channel) {
    return channel.attr(ProtocolFeatureStore.CHANNEL_KEY).get();
  }

  public ShardingInfo.ConnectionShardingInfo getConnectionShardingInfo() {
    return connectionShardingInfo;
  }

  public LwtInfo getLwtInfo() {
    return lwtInfo;
  }

  public boolean isUseMetadataId() {
    return useMetadataId;
  }
}
