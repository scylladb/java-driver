package com.datastax.oss.driver.api.core;

import edu.umd.cs.findbugs.annotations.NonNull;

public enum CQL4SkipMetadataResolveMethod {
  // SMART (Default) - Disables the skip metadata flag only for wildcard selects (`SELECT * FROM`)
  // and queries
  //  that return UDTs (including UDT collections and maps containing UDTs).
  SMART("smart"),
  // ENABLED – Enables the `skip metadata` flag, preventing metadata from being sent
  ENABLED("enabled"),
  // DISABLED - Disables the `skip metadata` flag, ensuring metadata is included in every RESULT
  // frame for bound statement execution.
  DISABLED("disabled");

  private final String value;

  CQL4SkipMetadataResolveMethod(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }

  // Case in-sensitive version of `valueOf`. To be used at all times instead of `valueOf`
  @NonNull
  public static CQL4SkipMetadataResolveMethod fromValue(@NonNull String value)
      throws IllegalArgumentException {
    switch (value.toLowerCase()) {
      case "smart":
        return SMART;
      case "enabled":
        return ENABLED;
      case "disabled":
        return DISABLED;
      default:
        throw new IllegalArgumentException("Unsupported value " + value);
    }
  }
}
