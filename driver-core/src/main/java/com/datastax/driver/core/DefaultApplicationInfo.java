package com.datastax.driver.core;

import java.util.Map;

public class DefaultApplicationInfo implements ApplicationInfo {
  private String applicationName;
  private String applicationVersion;
  private String clientId;

  public DefaultApplicationInfo(
      String applicationName, String applicationVersion, String clientId) {
    this.applicationName = applicationName;
    this.applicationVersion = applicationVersion;
    this.clientId = clientId;
  }

  @Override
  public void addOption(Map<String, String> options) {
    if (applicationName != null && !applicationName.isEmpty()) {
      options.put("APPLICATION_NAME", applicationName);
    }
    if (applicationVersion != null && !applicationVersion.isEmpty()) {
      options.put("APPLICATION_VERSION", applicationVersion);
    }
    if (clientId != null && !clientId.isEmpty()) {
      options.put("CLIENT_ID", clientId);
    }
  }
}
