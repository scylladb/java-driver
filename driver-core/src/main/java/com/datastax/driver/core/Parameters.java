package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Parameters {
  private final ConsistencyLevel defaultConsistency;
  private final ConsistencyLevel defaultSerialConsistency;

  @JsonCreator
  public Parameters(
      @JsonProperty(value = "defaultConsistency") ConsistencyLevel defaultConsistency,
      @JsonProperty(value = "defaultSerialConsistency") ConsistencyLevel defaultSerialConsistency) {
    this.defaultConsistency = defaultConsistency;
    this.defaultSerialConsistency = defaultSerialConsistency;
  }
}
