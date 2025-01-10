package com.datastax.driver.core;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverConfigurationInfo {
  private final DriverConfigurationReport report;
  private static final String DRIVER_CONFIGURATION_OPTION = "DRIVER_CONFIG";
  private static final Logger LOGGER = LoggerFactory.getLogger(DriverConfigurationInfo.class);

  private DriverConfigurationInfo(DriverConfigurationReport report) {
    this.report = report;
  }

  public void addOption(Map<String, String> options) {
    try {
      String driverConfig =
          new ObjectMapper()
              .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
              .writeValueAsString(report);
      LOGGER.info("Reporting following driver configuration: {}", driverConfig);
      options.put(DRIVER_CONFIGURATION_OPTION, driverConfig);
    } catch (JsonProcessingException e) {
      LOGGER.error("Failed to serialize driver configuration report", e);
      throw new RuntimeException(e);
    }
  }

  public static DriverConfigurationInfo build(Configuration config) {
    DriverConfigurationReport report =
        new DriverConfigurationReport(
            DriverConfigurationReport.SocketOptionReport.build(config.getSocketOptions()),
            config.getSocketOptions().getReadTimeoutMillis());

    return new DriverConfigurationInfo(report);
  }

  private static class DriverConfigurationReport {
    private final int RequestTimeoutMS;
    private final SocketOptionReport SocketOptions;

    private DriverConfigurationReport(SocketOptionReport socketReport, int requestTimeoutMS) {
      this.SocketOptions = socketReport;
      this.RequestTimeoutMS = requestTimeoutMS;
    }

    private static class SocketOptionReport implements Serializable {
      Integer ConnectionTimeoutMS;
      Boolean SO_KEEPALIVE;
      Boolean SO_REUSEADDR;
      Integer SO_LINGER;
      Boolean TCP_NODELAY;
      Integer SO_RCVBUF;
      Integer SO_SNDBUF;

      static SocketOptionReport build(com.datastax.driver.core.SocketOptions inputOptions) {
        SocketOptionReport socketOptions = new SocketOptionReport();
        socketOptions.ConnectionTimeoutMS = inputOptions.getConnectTimeoutMillis();
        socketOptions.SO_LINGER = inputOptions.getSoLinger();
        socketOptions.SO_KEEPALIVE = inputOptions.getKeepAlive();
        socketOptions.SO_REUSEADDR = inputOptions.getReuseAddress();
        socketOptions.TCP_NODELAY = inputOptions.getTcpNoDelay();
        socketOptions.SO_RCVBUF = inputOptions.getReceiveBufferSize();
        socketOptions.SO_SNDBUF = inputOptions.getSendBufferSize();
        return socketOptions;
      }
    }
  }
}
