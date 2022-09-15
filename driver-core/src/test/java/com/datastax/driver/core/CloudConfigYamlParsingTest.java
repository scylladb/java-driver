/*
 * Copyright (C) 2022 ScyllaDB
 */
package com.datastax.driver.core;

import java.io.IOException;
import java.net.URL;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import org.testng.annotations.Test;

public class CloudConfigYamlParsingTest {
  @Test(groups = "short")
  public void read_simple_config_and_create_bundle()
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException,
          InvalidKeySpecException {
    final String CONFIG_PATH = "/scylla_cloud/testConf.yaml";
    URL url = getClass().getResource(CONFIG_PATH);
    ScyllaCloudConnectionConfig scyllaCloudConnectionConfig =
        ScyllaCloudConnectionConfig.fromInputStream(url.openStream());
    ConfigurationBundle bundle = scyllaCloudConnectionConfig.createBundle();
  }

  @Test(
      groups = {"short"},
      expectedExceptions = IllegalArgumentException.class)
  public void read_incomplete_config() throws IOException {
    // This config does not contain certificates which are required
    final String CONFIG_PATH = "/scylla_cloud/config_w_missing_data.yaml";
    URL url = getClass().getResource(CONFIG_PATH);
    ScyllaCloudConnectionConfig scyllaCloudConnectionConfig =
        ScyllaCloudConnectionConfig.fromInputStream(url.openStream());
  }
}
