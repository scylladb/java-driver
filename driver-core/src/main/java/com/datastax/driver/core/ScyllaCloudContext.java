/*
 * Copyright (C) 2022 ScyllaDB
 */
package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonProperty;

class ScyllaCloudContext {
  private final String datacenterName;
  private final String authInfoName;

  public ScyllaCloudContext(
      @JsonProperty(value = "datacenterName", required = true) String datacenterName,
      @JsonProperty(value = "authInfoName", required = true) String authInfoName) {
    this.datacenterName = datacenterName;
    this.authInfoName = authInfoName;
  }

  public String getDatacenterName() {
    return datacenterName;
  }

  public String getAuthInfoName() {
    return authInfoName;
  }
}
