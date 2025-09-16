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

  ProtocolFeatureStore(LwtInfo lwtInfo) {
    this.lwtInfo = lwtInfo;
  }

  public LwtInfo getLwtFeatureInfo() {
    return lwtInfo;
  }

  public static ProtocolFeatureStore parseSupportedOptions(
      @NonNull Map<String, List<String>> options) {
    LwtInfo lwtInfo = LwtInfo.loadFromSupportedOptions(options);
    return new ProtocolFeatureStore(lwtInfo);
  }

  public void populateStartupOptions(@NonNull Map<String, String> options) {
    if (lwtInfo != null) {
      lwtInfo.populateStartupOptions(options);
    }
  }

  public static ProtocolFeatureStore loadFromChannel(@NonNull Channel channel) {
    return channel.attr(ProtocolFeatureStore.CHANNEL_KEY).get();
  }

  public void storeInChannel(@NonNull Channel channel) {
    channel.attr(ProtocolFeatureStore.CHANNEL_KEY).set(this);
  }
}
