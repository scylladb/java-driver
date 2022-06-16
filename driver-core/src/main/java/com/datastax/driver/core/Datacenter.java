package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;

public class Datacenter {
  private final String certificateAuthorityPath;
  private final byte[] certificateAuthorityData;
  private final String server;
  private final String TLSServerName;
  private final String nodeDomain;
  private final Boolean InsecureSkipTLSVerify;
  private final String proxyURL;

  @JsonCreator
  public Datacenter(
      @JsonProperty(value = "certificateAuthorityPath") String certificateAuthorityPath,
      @JsonProperty(value = "certificateAuthorityData") byte[] certificateAuthorityData,
      @JsonProperty(value = "server") String server,
      @JsonProperty(value = "TLSServerName") String TLSServerName,
      @JsonProperty(value = "nodeDomain") String nodeDomain,
      @JsonProperty(value = "insecureSkipTLSVerify") Boolean insecureSkipTLSVerify,
      @JsonProperty(value = "proxyURL") String proxyURL) {
    this.certificateAuthorityPath = certificateAuthorityPath;
    this.certificateAuthorityData = certificateAuthorityData;
    this.server = server;
    this.TLSServerName = TLSServerName;
    this.nodeDomain = nodeDomain;
    InsecureSkipTLSVerify = insecureSkipTLSVerify;
    this.proxyURL = proxyURL;
  }

  public void validate() {
    if (certificateAuthorityData == null) {
      if (certificateAuthorityPath == null) {
        throw new IllegalArgumentException(
            "Either certificateAuthorityData or certificateAuthorityPath must be provided for datacenter description.");
      }
      File file = new File(certificateAuthorityPath);
      if (!file.canRead()) {
        throw new IllegalArgumentException(
            "Cannot read file at given certificateAuthorityPath ("
                + certificateAuthorityPath
                + ").");
      }
    }
    if (server == null) {
      throw new IllegalArgumentException("server property is required in datacenter description.");
    }
    if (nodeDomain == null) {
      throw new IllegalArgumentException(
          "nodeDomain property is required in datacenter description.");
    }
  }

  String getCertificateAuthorityPath() {
    return certificateAuthorityPath;
  }

  byte[] getCertificateAuthorityData() {
    return certificateAuthorityData;
  }

}
