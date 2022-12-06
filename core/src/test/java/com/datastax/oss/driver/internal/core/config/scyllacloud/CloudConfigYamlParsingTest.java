package com.datastax.oss.driver.internal.core.config.scyllacloud;

import java.io.IOException;
import java.net.URL;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import org.junit.Test;

public class CloudConfigYamlParsingTest {
  @Test
  public void read_simple_config_and_create_bundle()
      throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException,
          InvalidKeySpecException {
    final String CONFIG_PATH = "/config/scyllacloud/testConf.yaml";
    URL url = getClass().getResource(CONFIG_PATH);
    ScyllaCloudConnectionConfig scyllaCloudConnectionConfig =
        ScyllaCloudConnectionConfig.fromInputStream(url.openStream());
    scyllaCloudConnectionConfig.validate();
    scyllaCloudConnectionConfig.createBundle();
  }

  @Test(expected = IllegalArgumentException.class)
  public void read_incomplete_config() throws IOException {
    // This config does not contain certificates which are required
    final String CONFIG_PATH = "/config/scyllacloud/incompleteConf.yaml";
    URL url = getClass().getResource(CONFIG_PATH);
    ScyllaCloudConnectionConfig.fromInputStream(url.openStream());
  }
}
