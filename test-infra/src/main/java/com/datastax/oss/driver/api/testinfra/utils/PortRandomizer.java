package com.datastax.oss.driver.api.testinfra.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PortRandomizer {
  private static final Logger LOG = LoggerFactory.getLogger(PortRandomizer.class);

  private static final Map<Integer, Long> recentPorts = new HashMap<>();
  private static final long RECENT_PORT_TTL = 4 * 60 * (long) 1e9; // nanoseconds
  private static final int MAX_FIND_PORT_RETRIES = 20;

  /**
   * Finds an available port in the ephemeral range.
   *
   * @return A local port that is currently unused.
   */
  public static synchronized int findAvailablePort() throws RuntimeException {
    int retries = 0;
    while (retries++ < MAX_FIND_PORT_RETRIES) {
      // let the system pick an ephemeral port
      try (ServerSocket ss = new ServerSocket(0)) {
        ss.setReuseAddress(true);
        long time = System.nanoTime();
        int port = ss.getLocalPort();
        Long last = recentPorts.get(port);
        if (last == null || time - last > RECENT_PORT_TTL) {
          recentPorts.put(port, time);
          LOG.info("Found available port: {}", port);
          return port;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException(
        "Couldn't find available port. Max retries (" + MAX_FIND_PORT_RETRIES + ") exceeded.");
  }
}
