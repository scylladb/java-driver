package com.datastax.oss.driver.internal.core.config.scyllacloud;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("unused")
public class Parameters {
  private final ConsistencyLevel defaultConsistency;
  private final ConsistencyLevel defaultSerialConsistency;

  @JsonCreator
  public Parameters(
      @JsonProperty(value = "defaultConsistency") DefaultConsistencyLevel defaultConsistency,
      @JsonProperty(value = "defaultSerialConsistency")
          DefaultConsistencyLevel defaultSerialConsistency) {
    this.defaultConsistency = defaultConsistency;
    this.defaultSerialConsistency = defaultSerialConsistency;
  }
}
