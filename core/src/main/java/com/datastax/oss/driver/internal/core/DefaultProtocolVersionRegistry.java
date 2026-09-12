/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Built-in implementation of the protocol version registry. */
@ThreadSafe
public class DefaultProtocolVersionRegistry implements ProtocolVersionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProtocolVersionRegistry.class);
  private static final List<ProtocolVersion> ALL_VERSIONS =
      ImmutableList.copyOf(DefaultProtocolVersion.values());

  private final String logPrefix;

  public DefaultProtocolVersionRegistry(String logPrefix) {
    this.logPrefix = logPrefix;
  }

  @Override
  public ProtocolVersion fromName(String name) {
    try {
      return DefaultProtocolVersion.valueOf(name);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown protocol version name: " + name);
    }
  }

  @Override
  public ProtocolVersion highestNonBeta() {
    ProtocolVersion highest = ALL_VERSIONS.get(ALL_VERSIONS.size() - 1);
    return highest.isBeta()
        ? downgrade(highest)
            .orElseThrow(() -> new AssertionError("There should be at least one non-beta version"))
        : highest;
  }

  @Override
  public Optional<ProtocolVersion> downgrade(ProtocolVersion version) {
    int index = ALL_VERSIONS.indexOf(version);
    if (index < 0) {
      throw new AssertionError(version + " is not a known version");
    } else if (index == 0) {
      return Optional.empty();
    }
    ProtocolVersion previousVersion = ALL_VERSIONS.get(index - 1);
    return previousVersion.isBeta() ? downgrade(previousVersion) : Optional.of(previousVersion);
  }

  @Override
  public ProtocolVersion highestCommon(Collection<Node> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("Expected at least one node");
    }

    Set<ProtocolVersion> candidates = new LinkedHashSet<>();
    for (ProtocolVersion version : ALL_VERSIONS) {
      if (!version.isBeta()) {
        candidates.add(version);
      }
    }
    ImmutableList<ProtocolVersion> initialCandidates = ImmutableList.copyOf(candidates);

    for (Node node : nodes) {
      Version cassandraVersion = node.getCassandraVersion();
      if (cassandraVersion == null) {
        LOG.warn(
            "[{}] Node {} does not report a Cassandra version, "
                + "ignoring it from optimal protocol version computation",
            logPrefix,
            node.getEndPoint());
        continue;
      }
      cassandraVersion = cassandraVersion.nextStable();
      LOG.debug(
          "[{}] Node {} reports Cassandra version {}",
          logPrefix,
          node.getEndPoint(),
          cassandraVersion);
      if (cassandraVersion.compareTo(Version.V2_1_0) < 0) {
        throw new UnsupportedProtocolVersionException(
            node.getEndPoint(),
            String.format(
                "Node %s reports Cassandra version %s, "
                    + "but the driver only supports 2.1.0 and above",
                node.getEndPoint(), cassandraVersion),
            ImmutableList.of(DefaultProtocolVersion.V3, DefaultProtocolVersion.V4));
      } else if (cassandraVersion.compareTo(Version.V2_2_0) < 0) {
        removeHigherThan(DefaultProtocolVersion.V3, candidates);
      } else if (cassandraVersion.compareTo(Version.V4_0_0) < 0) {
        removeHigherThan(DefaultProtocolVersion.V4, candidates);
      } else {
        removeHigherThan(DefaultProtocolVersion.V5, candidates);
      }
    }

    ProtocolVersion max = null;
    for (ProtocolVersion candidate : candidates) {
      if (max == null || max.getCode() < candidate.getCode()) {
        max = candidate;
      }
    }
    if (max == null) {
      throw new UnsupportedProtocolVersionException(
          null,
          String.format(
              "Could not determine a common protocol version, "
                  + "enable DEBUG logs for '%s' for more details",
              LOG.getName()),
          initialCandidates);
    }
    return max;
  }

  private void removeHigherThan(
      DefaultProtocolVersion maxVersion, Set<ProtocolVersion> candidates) {
    for (DefaultProtocolVersion version : DefaultProtocolVersion.values()) {
      if (version.compareTo(maxVersion) > 0 && candidates.remove(version)) {
        LOG.debug("[{}] Excluding protocol {}", logPrefix, version);
      }
    }
  }

  @Override
  public boolean supports(ProtocolVersion version, ProtocolFeature feature) {
    int code = version.getCode();
    if (DefaultProtocolFeature.SMALLINT_AND_TINYINT_TYPES.equals(feature)
        || DefaultProtocolFeature.DATE_TYPE.equals(feature)
        || DefaultProtocolFeature.UNSET_BOUND_VALUES.equals(feature)) {
      return DefaultProtocolVersion.V4.getCode() <= code;
    } else if (DefaultProtocolFeature.PER_REQUEST_KEYSPACE.equals(feature)
        || DefaultProtocolFeature.NOW_IN_SECONDS.equals(feature)
        || DefaultProtocolFeature.MODERN_FRAMING.equals(feature)) {
      return DefaultProtocolVersion.V5.getCode() <= code;
    }
    throw new IllegalArgumentException("Unhandled protocol feature: " + feature);
  }
}
