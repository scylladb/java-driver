package com.datastax.driver.core;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import org.junit.Test;



// WIP
public class CloudConfigYamlParsingTest {

  private static String CONFIG_PATH = "/scylla_cloud/testConf.yaml";

  @Test
  public void read_simple_config_and_create_bundle() throws CertificateException, KeyStoreException, IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    CONFIG_PATH = "/scylla_cloud/testConf.yaml";
    URL url = getClass().getResource(CONFIG_PATH);
    File file = new File(url.getPath());

    ConnectionConfig connectionConfig = ConnectionConfig.fromFile(file);
    System.out.println(connectionConfig);

    ConfigurationBundle bundle = connectionConfig.createBundle();

    return;
  }

  @Test(expected = IllegalArgumentException.class)
  public void read_incomplete_config() throws IOException {
    // This config does not contain certificates which are required
    CONFIG_PATH = "/scylla_cloud/config_w_missing_data.yaml";
    URL url = getClass().getResource(CONFIG_PATH);
    File file = new File(url.getPath());
    ConnectionConfig connectionConfig = ConnectionConfig.fromFile(file);
    return;
  }

}
