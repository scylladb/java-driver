package com.datastax.oss.driver.internal.core.config.scyllacloud;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;
import java.util.NoSuchElementException;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

public class ScyllaCloudConnectionConfig {
  private final String kind;
  private final String apiVersion;
  private final Map<String, ScyllaCloudDatacenter> datacenters;
  private final Map<String, ScyllaCloudAuthInfo> authInfos;
  private final Map<String, ScyllaCloudContext> contexts;
  private final String currentContext;
  private final Parameters parameters;

  @JsonCreator
  public ScyllaCloudConnectionConfig(
      @JsonProperty(value = "kind") String kind,
      @JsonProperty(value = "apiVersion") String apiVersion,
      @JsonProperty(value = "datacenters", required = true)
          Map<String, ScyllaCloudDatacenter> datacenters,
      @JsonProperty(value = "authInfos", required = true)
          Map<String, ScyllaCloudAuthInfo> authInfos,
      @JsonProperty(value = "contexts", required = true) Map<String, ScyllaCloudContext> contexts,
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

  public static ScyllaCloudConnectionConfig fromInputStream(InputStream inputStream)
      throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    ScyllaCloudConnectionConfig scyllaCloudConnectionConfig =
        mapper.readValue(inputStream, ScyllaCloudConnectionConfig.class);
    scyllaCloudConnectionConfig.validate();
    return scyllaCloudConnectionConfig;
  }

  public void validate() {
    if (this.datacenters == null) {
      throw new IllegalArgumentException(
          "Please provide datacenters (datacenters:) in the configuration yaml.");
    }
    for (ScyllaCloudDatacenter datacenter : datacenters.values()) {
      datacenter.validate();
    }

    if (this.authInfos == null) {
      throw new IllegalArgumentException(
          "Please provide any authentication config (authInfos:) in the configuration yaml.");
    }
    for (ScyllaCloudAuthInfo authInfo : authInfos.values()) {
      authInfo.validate();
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
    identity.load(null, null);
    trustStore.load(null, null);
    for (Map.Entry<String, ScyllaCloudDatacenter> datacenterEntry : datacenters.entrySet()) {
      ScyllaCloudDatacenter datacenter = datacenterEntry.getValue();
      InputStream certificateDataStream;
      if (datacenter.getCertificateAuthorityData() != null) {
        certificateDataStream = new ByteArrayInputStream(datacenter.getCertificateAuthorityData());
      } else if (datacenter.getCertificateAuthorityPath() != null) {
        certificateDataStream = new FileInputStream(datacenter.getCertificateAuthorityPath());
      } else {
        // impossible
        throw new IllegalStateException(
            "Neither CertificateAuthorityPath nor CertificateAuthorityData are set in this Datacenter object. "
                + "Validation should have prevented this.");
      }
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Certificate cert = cf.generateCertificate(certificateDataStream);
      trustStore.setCertificateEntry(datacenterEntry.getKey(), cert);
    }

    for (Map.Entry<String, ScyllaCloudAuthInfo> authInfoEntry : authInfos.entrySet()) {
      ScyllaCloudAuthInfo authInfo = authInfoEntry.getValue();
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
        keyString = new String(authInfo.getClientKeyData(), Charset.defaultCharset());
      } else if (authInfo.getClientKeyPath() != null) {
        BufferedReader br =
            Files.newBufferedReader(
                Paths.get(authInfo.getClientKeyPath()), Charset.defaultCharset());
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

      Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
      PEMParser pemParser = new PEMParser(new StringReader(keyString));
      JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
      Object object = pemParser.readObject();
      PrivateKey privateKey;
      if (object instanceof PrivateKeyInfo) {
        privateKey = converter.getPrivateKey((PrivateKeyInfo) object);
      } else if (object instanceof PEMKeyPair) {
        KeyPair kp = converter.getKeyPair((PEMKeyPair) object);
        privateKey = kp.getPrivate();
      } else if (object == null) {
        // Should not ever happen
        throw new IllegalStateException(
            "Error parsing authInfo "
                + authInfoEntry.getKey()
                + ". "
                + "Somehow no objects are left in the stream. Is passed Client Key empty?");
      } else {
        throw new InvalidKeySpecException(
            "Error parsing authInfo "
                + authInfoEntry.getKey()
                + ". "
                + "Make sure provided key signature is either 'RSA PRIVATE KEY' or 'PRIVATE KEY'");
      }

      identity.setKeyEntry(authInfoEntry.getKey(), privateKey, "cassandra".toCharArray(), certArr);
    }

    return new ConfigurationBundle(identity, trustStore);
  }

  public ScyllaCloudDatacenter getCurrentDatacenter() {
    return getDatacenter(getCurrentContext().getDatacenterName());
  }

  public ScyllaCloudAuthInfo getCurrentAuthInfo() {
    return getAuthInfo(getCurrentContext().getAuthInfoName());
  }

  public String getKind() {
    return kind;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public Map<String, ScyllaCloudDatacenter> getDatacenters() {
    return datacenters;
  }

  public Map<String, ScyllaCloudAuthInfo> getAuthInfos() {
    return authInfos;
  }

  public Map<String, ScyllaCloudContext> getContexts() {
    return contexts;
  }

  public ScyllaCloudContext getContext(String ctx) {
    if (!contexts.containsKey(ctx)) {
      throw new NoSuchElementException(
          String.format(
              "There is no context named %s. Check your Scylla Cloud configuration file.", ctx));
    }
    return contexts.get(ctx);
  }

  public ScyllaCloudAuthInfo getAuthInfo(String authInfo) {
    if (!authInfos.containsKey(authInfo)) {
      throw new NoSuchElementException(
          String.format(
              "There is no authInfo named %s. Check your Scylla Cloud configuration file.",
              authInfo));
    }
    return authInfos.get(authInfo);
  }

  public ScyllaCloudDatacenter getDatacenter(String datacenter) {
    if (!datacenters.containsKey(datacenter)) {
      throw new NoSuchElementException(
          String.format(
              "There is no datacenter named %s. Check your Scylla Cloud configuration file.",
              datacenter));
    }
    return datacenters.get(datacenter);
  }

  public ScyllaCloudContext getCurrentContext() {
    return getContext(currentContext);
  }

  public Parameters getParameters() {
    return parameters;
  }
}
