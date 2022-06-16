package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.BaseEncoding;
import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Map;

public class ConnectionConfig {
  private final String kind;
  private final String apiVersion;
  private final Map<String, Datacenter> datacenters;
  private final Map<String, AuthInfo> authInfos;
  private final Map<String, Context> contexts;
  private final String currentContext;
  private final Parameters parameters;

  @JsonCreator
  public ConnectionConfig(
      @JsonProperty(value = "kind") String kind,
      @JsonProperty(value = "apiVersion") String apiVersion,
      @JsonProperty(value = "datacenters", required = true) Map<String, Datacenter> datacenters,
      @JsonProperty(value = "authInfos", required = true) Map<String, AuthInfo> authInfos,
      @JsonProperty(value = "contexts", required = true) Map<String, Context> contexts,
      @JsonProperty(value = "currentContext", required = true) String currentContext,
      @JsonProperty(value = "parameters") Parameters parameters) {
    this.kind = kind;
    this.apiVersion = apiVersion;
    this.datacenters = datacenters;
    this.authInfos = authInfos;
    this.contexts = contexts;
    this.currentContext = currentContext;
    this.parameters = parameters;
  }

  public static ConnectionConfig fromFile(File yamlFile) throws IOException {
    try {
      ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
      ConnectionConfig connectionConfig = mapper.readValue(yamlFile, ConnectionConfig.class);
      connectionConfig.validate();
      return connectionConfig;
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Configuration yaml located at " + yamlFile.getAbsolutePath() + " has incorrect format. See cause's message and stacktrace. ", e);
    } catch (JsonMappingException e) {
      throw e;
    } catch (JsonParseException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }

  }

  public void validate() {
    if (this.datacenters == null) {
      throw new IllegalArgumentException(
          "Please provide datacenters (datacenters:) in the configuration yaml.");
    }
    for (Datacenter x : datacenters.values()) {
      x.validate();
    }

    if (this.authInfos == null) {
      throw new IllegalArgumentException(
          "Please provide any authentication config (authInfos:) in the configuration yaml.");
    }
    for (AuthInfo x : authInfos.values()) {
      x.validate();
    }

    if (this.contexts == null) {
      throw new IllegalArgumentException(
          "Please provide any configuration (contexts:) context in the configuration yaml.");
    }

    if (this.currentContext == null) {
      throw new IllegalArgumentException(
          "Please set default context (currentContext:) in the configuration yaml.");
    }
  }

  public ConfigurationBundle createBundle()
      throws KeyStoreException, CertificateException, IOException, NoSuchAlgorithmException,
          InvalidKeySpecException {
    this.validate();
    KeyStore identity = KeyStore.getInstance(KeyStore.getDefaultType());
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    identity.load(null, "example123".toCharArray());
    trustStore.load(null, "example123".toCharArray());

    for (Map.Entry<String, Datacenter> x : datacenters.entrySet()) {
      Datacenter datacenter = x.getValue();
      InputStream certificateDataStream;
      if (datacenter.getCertificateAuthorityData() != null) {
        certificateDataStream = new ByteArrayInputStream(datacenter.getCertificateAuthorityData());
      } else if (datacenter.getCertificateAuthorityPath() != null) {
        certificateDataStream = new FileInputStream(datacenter.getCertificateAuthorityPath());
      } else {
        // impossible
        throw new RuntimeException(
            "Neither CertificateAuthorityPath nor CertificateAuthorityData are set in this Datacenter object. "
                + "Validation should have prevented this.");
      }
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Certificate cert = cf.generateCertificate(certificateDataStream);
      // identity.setCertificateEntry(x.getKey(), cert);
      trustStore.setCertificateEntry(x.getKey(), cert);
    }

    for (Map.Entry<String, AuthInfo> x : authInfos.entrySet()) {
      AuthInfo authInfo = x.getValue();
      InputStream certificateDataStream;
      String keyString;

      if (authInfo.getClientCertificateData() != null) {
        certificateDataStream = new ByteArrayInputStream(authInfo.getClientCertificateData());
      } else if (authInfo.getClientCertificatePath() != null) {
        certificateDataStream = new FileInputStream(authInfo.getClientCertificatePath());
      } else {
        // impossible
        throw new RuntimeException(
            "Neither CertificateAuthorityPath nor CertificateAuthorityData are set in this AuthInfo object. "
                + "Validation should have prevented this.");
      }

      if (authInfo.getClientKeyData() != null) {
        keyString = new String(authInfo.getClientKeyData());
      } else if (authInfo.getClientKeyPath() != null) {
        BufferedReader br = new BufferedReader(new FileReader(authInfo.getClientKeyPath()));
        StringBuilder sb = new StringBuilder();
        String line = br.readLine();
        while (line != null) {
          sb.append(line);
          line = br.readLine();
        }
        keyString = sb.toString();
      } else {
        // impossible
        throw new RuntimeException(
            "Neither ClientKeyData nor ClientKeyPath are set in this AuthInfo object. "
                + "Validation should have prevented this.");
      }

      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Certificate cert = cf.generateCertificate(certificateDataStream);

      Certificate[] certArr = new Certificate[1];
      certArr[0] = cert;

      keyString =
          keyString
              .replace("-----BEGIN PRIVATE KEY-----", "")
              .replace("-----END PRIVATE KEY-----", "")
              .replaceAll("\\s+", "");
      byte[] arr = BaseEncoding.base64().decode(keyString);

      PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(arr);
      KeyFactory kf = KeyFactory.getInstance("RSA");
      PrivateKey privateKey = kf.generatePrivate(keySpec);
      identity.setKeyEntry(x.getKey(), privateKey, "example123".toCharArray(), certArr);
    }

    return new ConfigurationBundle(identity, trustStore);
  }
}
