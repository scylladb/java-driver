package com.datastax.driver.core;

import java.util.Map;

public interface ApplicationInfo {
  /** Adds application information to startup options. */
  void addOption(Map<String, String> options);
}
