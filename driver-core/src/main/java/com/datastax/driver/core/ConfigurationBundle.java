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

import java.io.*;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

class ConfigurationBundle {
  private final KeyStore identity;
  private final KeyStore trustStore;

  public ConfigurationBundle(KeyStore identity, KeyStore trustStore) {
    this.identity = identity;
    this.trustStore = trustStore;
  }

  public KeyStore getIdentity() {
    return identity;
  }

  public KeyStore getTrustStore() {
    return trustStore;
  }

  private void writeKeystore(String path, KeyStore ks, char[] password)
      throws IOException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
    File file = new File(path);
    OutputStream os = new FileOutputStream(file);
    ks.store(os, password);
    os.close();
  }

  public void writeIdentity(String path, char[] password)
      throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
    writeKeystore(path, identity, password);
  }

  public void writeTrustStore(String path, char[] password)
      throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
    writeKeystore(path, trustStore, password);
  }

  protected SSLContext getSSLContext() throws IOException, GeneralSecurityException {
    KeyManagerFactory kmf = createKeyManagerFactory(identity);
    TrustManagerFactory tmf = createTrustManagerFactory(trustStore);
    SSLContext sslContext = SSLContext.getInstance("SSL");
    sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
    return sslContext;
  }

  protected SSLContext getInsecureSSLContext() throws IOException, GeneralSecurityException {
    KeyManagerFactory kmf = createKeyManagerFactory(identity);
    SSLContext sslContext = SSLContext.getInstance("SSL");
    TrustManager[] trustManager =
        new TrustManager[] {
          new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
                throws CertificateException {}

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
                throws CertificateException {}

            @Override
            public X509Certificate[] getAcceptedIssuers() {
              return new X509Certificate[0];
            }
          }
        };
    sslContext.init(kmf.getKeyManagers(), trustManager, new SecureRandom());
    return sslContext;
  }

  protected KeyManagerFactory createKeyManagerFactory(KeyStore ks)
      throws IOException, GeneralSecurityException {
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, "cassandra".toCharArray());
    return kmf;
  }

  protected TrustManagerFactory createTrustManagerFactory(KeyStore ts)
      throws IOException, GeneralSecurityException {
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    return tmf;
  }

  protected SSLOptions getSSLOptions() throws GeneralSecurityException, IOException {
    return SniSSLOptions.builder().withSSLContext(getSSLContext()).build();
  }

  protected SSLOptions getInsecureSSLOptions() throws GeneralSecurityException, IOException {
    return SniSSLOptions.builder().withSSLContext(getInsecureSSLContext()).build();
  }
}
