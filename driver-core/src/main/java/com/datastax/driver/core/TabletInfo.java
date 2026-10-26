package com.datastax.driver.core;

import java.util.List;
import java.util.Map;

/** Tablet routing extension negotiated independently on each connection. */
public class TabletInfo {
  private static final String V1 = "TABLETS_ROUTING_V1";
  private static final String V2 = "TABLETS_ROUTING_V2_EXPERIMENTAL";
  public static final String TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY = "tablets-routing-v1";
  public static final String TABLETS_ROUTING_V2_CUSTOM_PAYLOAD_KEY = "tablets-routing-v2";

  private final int version;

  private TabletInfo(int version) {
    this.version = version;
  }

  public boolean isEnabled() {
    return version != 0;
  }

  public boolean isV2() {
    return version == 2;
  }

  public static TabletInfo parseTabletInfo(Map<String, List<String>> supported) {
    return new TabletInfo(advertises(supported, V2) ? 2 : advertises(supported, V1) ? 1 : 0);
  }

  private static boolean advertises(Map<String, List<String>> supported, String key) {
    List<String> values = supported.get(key);
    // Scylla advertises these extensions with an empty list. Also accept the legacy
    // singleton empty string representation used by older nodes and protocol test servers.
    return values != null && (values.isEmpty() || (values.size() == 1 && values.get(0).isEmpty()));
  }

  /** Adds the legacy V1 option. */
  public static void addOption(Map<String, String> options) {
    options.put(V1, "");
  }

  void addNegotiatedOption(Map<String, String> options) {
    options.put(isV2() ? V2 : V1, "");
  }
}
