package com.datastax.oss.driver.internal.core.protocol;

import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
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

  public boolean isVersion2() {
    return version == 2;
  }

  public static TabletInfo loadFromSupportedOptions(Map<String, List<String>> supported) {
    return new TabletInfo(isSupported(supported, V2) ? 2 : isSupported(supported, V1) ? 1 : 0);
  }

  private static boolean isSupported(Map<String, List<String>> supported, String key) {
    List<String> values = supported.get(key);
    return values != null
        && ((key.equals(V2) && values.isEmpty())
            || (values.size() == 1 && values.get(0).isEmpty()));
  }

  public void populateStartupOptions(Map<String, String> options) {
    if (isEnabled()) {
      options.put(isVersion2() ? V2 : V1, "");
    }
  }
}
