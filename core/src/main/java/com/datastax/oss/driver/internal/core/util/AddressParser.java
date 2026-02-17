/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.util;

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Utility class for parsing network addresses. This is used internally for parsing contact points
 * and client routes endpoints.
 *
 * <p>This class is part of the internal API and is not intended for public use. Classes under
 * {@code com.datastax.oss.driver.internal.*} are not user-facing and may change without notice.
 */
public class AddressParser {

  private static final int DEFAULT_PORT = 9042;

  /**
   * Parses a contact point address string into an InetSocketAddress. Supports IPv4, IPv6, and
   * hostname formats with optional port.
   *
   * <p>Accepted formats:
   *
   * <ul>
   *   <li>hostname:port (e.g., "localhost:9042")
   *   <li>hostname (defaults to port 9042)
   *   <li>ipv4:port (e.g., "192.168.1.1:9042")
   *   <li>[ipv6]:port (e.g., "[::1]:9042", "[2001:db8::1]:9042")
   *   <li>[ipv6] (defaults to port 9042)
   * </ul>
   *
   * @param address the address string to parse (must not be null)
   * @param connectionId the connection ID for error messages (can be null for generic parsing)
   * @return an InetSocketAddress
   * @throws IllegalArgumentException if the address is null, empty, or has an invalid format
   */
  public static InetSocketAddress parseContactPoint(String address, UUID connectionId) {
    if (address == null) {
      throw new IllegalArgumentException(
          formatErrorMessage(null, connectionId, "Address must not be null"));
    }
    if (address.isEmpty()) {
      throw new IllegalArgumentException(
          formatErrorMessage(address, connectionId, "Address must not be empty"));
    }

    try {
      // Add scheme to make it a valid URI for parsing
      // URI class handles IPv6 brackets, hostname, and port correctly
      String uriString = address.contains("://") ? address : "cql://" + address;
      java.net.URI uri = new java.net.URI(uriString);

      String host = uri.getHost();
      int port = uri.getPort();

      // Validate we got a host
      if (host == null || host.isEmpty()) {
        throw new IllegalArgumentException(
            formatErrorMessage(
                address,
                connectionId,
                "Invalid address format. Expected format: 'host:port' or '[ipv6]:port'"));
      }

      // Use default port if not specified
      if (port == -1) {
        port = DEFAULT_PORT;
      }

      // Validate port range
      if (port < 1 || port > 65535) {
        throw new IllegalArgumentException(
            formatErrorMessage(
                address,
                connectionId,
                String.format("Invalid port %d. Port must be between 1 and 65535.", port)));
      }

      return InetSocketAddress.createUnresolved(host, port);

    } catch (java.net.URISyntaxException e) {
      throw new IllegalArgumentException(
          formatErrorMessage(
              address,
              connectionId,
              "Invalid address format. Expected format: 'host:port' or '[ipv6]:port'. "
                  + e.getMessage()),
          e);
    }
  }

  /**
   * Formats an error message for address parsing failures, including the address and connection ID.
   *
   * @param address the address that failed to parse (can be null)
   * @param connectionId the connection ID associated with this address (can be null)
   * @param message the specific error message to include
   * @return a formatted error message string
   */
  private static String formatErrorMessage(String address, UUID connectionId, String message) {
    String addressStr = (address == null) ? "null" : "'" + address + "'";
    if (connectionId != null) {
      return String.format(
          "Failed to parse address %s (connection ID: %s). %s", addressStr, connectionId, message);
    } else {
      return String.format("Failed to parse address %s. %s", addressStr, message);
    }
  }

  private AddressParser() {
    // Utility class, no instances
  }
}
