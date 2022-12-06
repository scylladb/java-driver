package com.datastax.oss.driver.internal.core.config.scyllacloud;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;

public class ScyllaCloudAuthInfo {
  private final byte[] clientCertificateData;
  private final String clientCertificatePath;
  private final byte[] clientKeyData;
  private final String clientKeyPath;
  private final String username;
  private final String password;

  @JsonCreator
  public ScyllaCloudAuthInfo(
      @JsonProperty(value = "clientCertificateData") byte[] clientCertificateData,
      @JsonProperty(value = "clientCertificatePath") String clientCertificatePath,
      @JsonProperty(value = "clientKeyData") byte[] clientKeyData,
      @JsonProperty(value = "clientKeyPath") String clientKeyPath,
      @JsonProperty(value = "username") String username,
      @JsonProperty(value = "password") String password) {
    this.clientCertificateData = clientCertificateData;
    this.clientCertificatePath = clientCertificatePath;
    this.clientKeyData = clientKeyData;
    this.clientKeyPath = clientKeyPath;
    this.username = username;
    this.password = password;
  }

  public void validate() {
    if (clientCertificateData == null) {
      if (clientCertificatePath == null) {
        throw new IllegalArgumentException(
            "Either clientCertificateData or clientCertificatePath has to be provided for authInfo.");
      }
      File file = new File(clientCertificatePath);
      if (!file.canRead()) {
        throw new IllegalArgumentException(
            "Cannot read file at given clientCertificatePath (" + clientCertificatePath + ").");
      }
    }

    if (clientKeyData == null) {
      if (clientKeyPath == null) {
        throw new IllegalArgumentException(
            "Either clientKeyData or clientKeyPath has to be provided for authInfo.");
      }
      File file = new File(clientKeyPath);
      if (!file.canRead()) {
        throw new IllegalArgumentException(
            "Cannot read file at given clientKeyPath (" + clientKeyPath + ").");
      }
    }
  }

  public byte[] getClientCertificateData() {
    return clientCertificateData;
  }

  public String getClientCertificatePath() {
    return clientCertificatePath;
  }

  public byte[] getClientKeyData() {
    return clientKeyData;
  }

  public String getClientKeyPath() {
    return clientKeyPath;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
}
