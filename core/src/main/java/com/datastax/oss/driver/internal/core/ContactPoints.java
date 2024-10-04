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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.UnresolvedEndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to handle the initial contact points passed to the driver.
 */
public class ContactPoints {
  private static final Logger LOG = LoggerFactory.getLogger(ContactPoints.class);

  public static Set<EndPoint> merge(
      Set<EndPoint> programmaticContactPoints, List<String> configContactPoints, boolean resolve) {

    Set<EndPoint> result = Sets.newHashSet(programmaticContactPoints);
    for (String spec : configContactPoints) {
      for (EndPoint endPoint : fromString(spec, resolve)) {
        boolean wasNew = result.add(endPoint);
        if (!wasNew) {
          LOG.warn("Duplicate contact point {}", spec);
        }
      }
    }
    return ImmutableSet.copyOf(result);
  }

  public static List<EndPoint> fromString(String spec, Boolean resolve) {
    int separator = spec.lastIndexOf(':');
    if (separator < 0) {
      LOG.warn("Ignoring invalid contact point {} (expecting host:port)", spec);
      return Collections.emptyList();
    }

    String host = spec.substring(0, separator);
    String portSpec = spec.substring(separator + 1);
    int port;
    try {
      port = Integer.parseInt(portSpec);
    } catch (NumberFormatException e) {
      LOG.warn("Ignoring invalid contact point {} (expecting a number, got {})", spec, portSpec);
      return Collections.emptyList();
    }
    if (!resolve) {
      return ImmutableList.of(new UnresolvedEndPoint(host, port));
    }
    try {
      InetAddress[] inetAddresses = InetAddress.getAllByName(host);
      if (inetAddresses.length > 1) {
        LOG.info(
            "Contact point {} resolves to multiple addresses, will use them all ({})",
            spec,
            Arrays.deepToString(inetAddresses));
      }
      Set<EndPoint> result = new HashSet<>();
      for (InetAddress inetAddress : inetAddresses) {
        result.add(new DefaultEndPoint(new InetSocketAddress(inetAddress, port)));
      }
      return new ArrayList<>(result);
    } catch (UnknownHostException e) {
      LOG.warn("Ignoring invalid contact point {} (unknown host {})", spec, host);
      return Collections.emptyList();
    }
  }
}
