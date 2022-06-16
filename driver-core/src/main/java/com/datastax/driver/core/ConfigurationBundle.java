package com.datastax.driver.core;

import java.io.*;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class ConfigurationBundle {
  private final KeyStore identity;
  private final KeyStore trustStore;

  public ConfigurationBundle(KeyStore identity, KeyStore trustStore) {
    this.identity = identity;
    this.trustStore = trustStore;
  }

  private void writeKeystore(String path, KeyStore ks, char[] password) throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
    File file = new File(path);
    OutputStream os = new FileOutputStream(file);
    ks.store(os, password);
    os.close();
  }

  public void writeIdentity(String path, char[] password) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
    writeKeystore(path, identity, password);
  }

  public void writeTrustStore(String path, char[] password) throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
    writeKeystore(path, trustStore, password);
  }
}
