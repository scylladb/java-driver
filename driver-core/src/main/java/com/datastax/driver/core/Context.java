package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;

public class Context {
  private final String datacenterName;
  private final String authInfoName;

  public Context(
      @JsonProperty(value = "datacenterName", required = true) String datacenterName,
      @JsonProperty(value = "authInfoName", required = true) String authInfoName) {
    this.datacenterName = datacenterName;
    this.authInfoName = authInfoName;
  }

}
