package com.datastax.oss.driver.internal.core.config.scyllacloud;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.net.HostAndPort;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

public class ScyllaCloudDatacenter {
  private final String certificateAuthorityPath;
  private final byte[] certificateAuthorityData;
  private final String server;
  private final String tlsServerName;
  private final String nodeDomain;
  private final String proxyURL;

  private final boolean insecureSkipTlsVerify;

  // Full hostname has limit of 255 chars.
  // Host UUID takes 32 chars for hex digits and 4 dashes. Additional 1 is for separator dot before
  // nodeDomain
  private static final int NODE_DOMAIN_MAX_LENGTH = 255 - 32 - 4 - 1;

  @JsonCreator
  public ScyllaCloudDatacenter(
      @JsonProperty(value = "certificateAuthorityPath") String certificateAuthorityPath,
      @JsonProperty(value = "certificateAuthorityData") byte[] certificateAuthorityData,
      @JsonProperty(value = "server") String server,
      @JsonProperty(value = "tlsServerName") String tlsServerName,
      @JsonProperty(value = "nodeDomain") String nodeDomain,
      @JsonProperty(value = "proxyURL") String proxyURL,
      @JsonProperty(value = "insecureSkipTlsVerify", defaultValue = "false")
          boolean insecureSkipTlsVerify) {
    this.certificateAuthorityPath = certificateAuthorityPath;
    this.certificateAuthorityData = certificateAuthorityData;
    this.server = server;
    this.tlsServerName = tlsServerName;
    this.nodeDomain = nodeDomain;
    this.proxyURL = proxyURL;
    this.insecureSkipTlsVerify = insecureSkipTlsVerify;
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
    validateServer();
    validateNodeDomain();
  }

  public String getCertificateAuthorityPath() {
    return certificateAuthorityPath;
  }

  public byte[] getCertificateAuthorityData() {
    return certificateAuthorityData;
  }

  public InetSocketAddress getServer() {
    HostAndPort parsedServer = HostAndPort.fromString(server);
    return InetSocketAddress.createUnresolved(parsedServer.getHost(), parsedServer.getPort());
  }

  public String getNodeDomain() {
    return nodeDomain;
  }

  public String getTlsServerName() {
    return tlsServerName;
  }

  public String getProxyURL() {
    return proxyURL;
  }

  public boolean isInsecureSkipTlsVerify() {
    return insecureSkipTlsVerify;
  }

  // Using parts relevant to hostnames as we're dealing with a part of hostname
  // RFC-1123 Section 2.1 and RFC-952 1.
  private void validateNodeDomain() {
    if (nodeDomain == null || nodeDomain.length() == 0) {
      throw new IllegalArgumentException(
          "nodeDomain property is required in datacenter description.");
    } else {
      if (nodeDomain.length() > NODE_DOMAIN_MAX_LENGTH) {
        // Should be shorter because it is not the whole hostname
        throw new IllegalArgumentException(
            "Subdomain name too long (max " + NODE_DOMAIN_MAX_LENGTH + "): " + nodeDomain);
      }
      if (nodeDomain.contains(" ")) {
        throw new IllegalArgumentException(
            "nodeDomain '" + nodeDomain + "' cannot contain spaces.");
      }
      if (nodeDomain.startsWith(".") || nodeDomain.endsWith(".")) {
        throw new IllegalArgumentException(
            "nodeDomain '" + nodeDomain + "' cannot start or end with a dot.");
      }
      if (nodeDomain.endsWith("-")) {
        throw new IllegalArgumentException(
            "nodeDomain '" + nodeDomain + "' cannot end with a minus sign.");
      }
    }

    List<String> components = ImmutableList.copyOf(nodeDomain.split("\\."));
    for (String component : components) {
      if (component.length() == 0) {
        throw new IllegalArgumentException(
            "nodeDomain '" + nodeDomain + "' cannot have empty components between dots.");
      }

      for (int index = 0; index < component.length(); index++) {
        if (!Character.isLetterOrDigit(component.charAt(index))) {
          if (component.charAt(index) == '-') {
            if (index == 0 || index == component.length() - 1) {
              throw new IllegalArgumentException(
                  "nodeDomain '"
                      + nodeDomain
                      + "' components can have minus sign only as interior character: "
                      + component.charAt(index));
            }
          } else {
            throw new IllegalArgumentException(
                "nodeDomain '"
                    + nodeDomain
                    + "' contains illegal character: "
                    + component.charAt(index));
          }
        }
      }
    }
  }

  private void validateServer() {
    if (server == null) {
      throw new IllegalArgumentException("server property is required in datacenter description.");
    } else {
      try {
        // Property 'server' is not a true URL because it does not contain protocol prefix
        // We're adding prefix just to satisfy that part of validation
        URL url = new URL("http://" + server);
        if (url.getPort() == -1) {
          throw new IllegalArgumentException(
              "server property '" + server + "' does not contain a port.");
        }
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(
            "server property '" + server + "' is not a valid URL", e);
      }
    }
  }
}
