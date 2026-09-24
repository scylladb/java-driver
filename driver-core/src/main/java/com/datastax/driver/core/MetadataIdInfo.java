package com.datastax.driver.core;

import java.util.List;
import java.util.Map;

public class MetadataIdInfo {
  private static final String SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY = "SCYLLA_USE_METADATA_ID";
  private static final String SCYLLA_USE_METADATA_ID_STARTUP_OPTION_VALUE = "";

  public static boolean parseUseMetadataId(Map<String, List<String>> supported) {
    if (!supported.containsKey(SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY)) {
      return false;
    }
    List<String> values = supported.get(SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY);
    return values != null
        && values.size() == 1
        && values.get(0).equals(SCYLLA_USE_METADATA_ID_STARTUP_OPTION_VALUE);
  }

  public static void addOption(Map<String, String> options) {
    options.put(
        SCYLLA_USE_METADATA_ID_STARTUP_OPTION_KEY, SCYLLA_USE_METADATA_ID_STARTUP_OPTION_VALUE);
  }
}
