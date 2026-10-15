package com.datastax.oss.driver.api.core;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Locale;

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
    // ROOT, not the default locale: in a Turkish JVM the I of "DISABLED" folds to a dotless
    // small letter, which matches no case below and rejects a valid configuration.
    switch (value.toLowerCase(Locale.ROOT)) {
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
