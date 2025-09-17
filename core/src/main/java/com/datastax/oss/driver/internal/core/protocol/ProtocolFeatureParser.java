package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.protocol.internal.response.Supported;
import java.util.List;
import java.util.Map;

/**
 * <code>ProtocolFeatureParser</code> is a utility class handling parsing of {@link Supported}
 * response options providing an API to check for supported features
 */
public class ProtocolFeatureParser {

  /** A builder class for a {@link ProtocolFeatureParser}. */
  public static class Builder {

    private final Supported supported;

    private Builder(Supported supported) {
      this.supported = supported;
    }

    public static Builder fromOptions(Supported supported) {
      return new Builder(supported);
    }

    public ProtocolFeatureParser build() {
      return new ProtocolFeatureParser(supported.options);
    }
  }

  private final Map<String, List<String>> options;

  private ProtocolFeatureParser(Map<String, List<String>> options) {
    this.options = options;
  }

  /**
   * Parses {@link Supported#options} field of {@link Supported} message
   *
   * @return instance of {@link ProtocolFeatureManager} containing parsed properties
   */
  public ProtocolFeatureManager parse() {
    ShardingInfo.ConnectionShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(options);
    LwtInfo lwtInfo = LwtInfo.parseLwtInfo(options);
    TabletInfo tabletInfo = TabletInfo.parseTabletInfo(options);
    boolean metadataIdEnabled = MetadataIdInfo.parseMetadataId(options);
    return new ProtocolFeatureManager(
        options, shardingInfo, lwtInfo, tabletInfo, metadataIdEnabled);
  }
}
