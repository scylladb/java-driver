/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;

class ScyllaCloudAuthInfo {
  private final byte[] clientCertificateData;
  private final String clientCertificatePath;
  private final byte[] clientKeyData;
  private final String clientKeyPath;
  private final String username;
  private final String password;
  private final boolean insecureSkipTlsVerify;

  @JsonCreator
  public ScyllaCloudAuthInfo(
      @JsonProperty(value = "clientCertificateData") byte[] clientCertificateData,
      @JsonProperty(value = "clientCertificatePath") String clientCertificatePath,
      @JsonProperty(value = "clientKeyData") byte[] clientKeyData,
      @JsonProperty(value = "clientKeyPath") String clientKeyPath,
      @JsonProperty(value = "username") String username,
      @JsonProperty(value = "password") String password,
      @JsonProperty(value = "insecureSkipTlsVerify", defaultValue = "false")
          boolean insecureSkipTlsVerify) {
    this.clientCertificateData = clientCertificateData;
    this.clientCertificatePath = clientCertificatePath;
    this.clientKeyData = clientKeyData;
    this.clientKeyPath = clientKeyPath;
    this.username = username;
    this.password = password;
    this.insecureSkipTlsVerify = insecureSkipTlsVerify;
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

  public boolean isInsecureSkipTlsVerify() {
    return insecureSkipTlsVerify;
  }
}
