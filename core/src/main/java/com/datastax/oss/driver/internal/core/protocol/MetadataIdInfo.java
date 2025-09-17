package com.datastax.oss.driver.internal.core.protocol;

import java.util.List;
import java.util.Map;

public class MetadataIdInfo {
  private static final String SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY = "SCYLLA_USE_METADATA_ID";
  private static final String SCYLLA_USE_METADATA_ID_STARTUP_OPTION_VALUE = "";

  private final boolean enabled;

  private MetadataIdInfo(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public static MetadataIdInfo parseMetadataId(Map<String, List<String>> supported) {
    if (!supported.containsKey(SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY)) {
      return new MetadataIdInfo(false);
    }
    List<String> values = supported.get(SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY);
    return new MetadataIdInfo(
        values != null
            && values.size() == 1
            && values.get(0).equals(SCYLLA_USE_METADATA_ID_STARTUP_OPTION_VALUE));
  }

  public static void addOption(Map<String, String> options) {
    options.put(
        SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY, SCYLLA_USE_METADATA_ID_STARTUP_OPTION_VALUE);
  }
}
