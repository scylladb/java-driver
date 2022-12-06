package com.datastax.oss.driver.examples.scyllacloud;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.io.File;

public class ReadScyllaVersion {

  public static void main(String[] args) {
    String configPath = "/path/to/scylla/cloud/conf/file";
    File configFile = new File(configPath);
    DriverConfigLoader loader =
        DriverConfigLoader.programmaticBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, DefaultProtocolVersion.V4.toString())
            .build();

    try (CqlSession session =
        CqlSession.builder()
            .withConfigLoader(loader)
            .withScyllaCloudSecureConnectBundle(configFile.toPath())
            .build()) {
      ResultSet rs = session.execute("select release_version from system.local");
      Row row = rs.one();
      assert row != null;
      String releaseVersion = row.getString("release_version");
      System.out.printf("Scylla version: %s%n", releaseVersion);
    }
  }
}
