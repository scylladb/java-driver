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
package com.datastax.oss.driver.internal.core.loadbalancing.helper;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LocalDcHelper} that fetches the local datacenter from the
 * programmatic configuration API, or else, from the driver configuration. If no user-supplied
 * datacenter can be retrieved, it returns {@link Optional#empty empty}.
 */
@ThreadSafe
public class OptionalLocalDcHelper implements LocalDcHelper {

  private static final Logger LOG = LoggerFactory.getLogger(OptionalLocalDcHelper.class);

  @NonNull protected final InternalDriverContext context;
  @NonNull protected final DriverExecutionProfile profile;
  @NonNull protected final String logPrefix;

  public OptionalLocalDcHelper(
      @NonNull InternalDriverContext context,
      @NonNull DriverExecutionProfile profile,
      @NonNull String logPrefix) {
    this.context = context;
    this.profile = profile;
    this.logPrefix = logPrefix;
  }

  /**
   * @return The local datacenter from the programmatic configuration API, or from the driver
   *     configuration; {@link Optional#empty empty} if none found.
   */
  @Override
  @NonNull
  public Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    String localDcStr = context.getLocalDatacenter(profile.getName());
    Optional<String> localDc;
    if (localDcStr != null) {
      LOG.debug("[{}] Local DC set programmatically: {}", logPrefix, localDcStr);
      localDc = Optional.of(localDcStr);
    } else if (profile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
      localDcStr = profile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER);
      LOG.debug("[{}] Local DC set from configuration: {}", logPrefix, localDcStr);
      localDc = Optional.of(localDcStr);
    } else {
      localDc = Optional.empty();
    }
    if (localDc.isPresent()) {
      checkLocalDatacenterCompatibility(
          localDc.get(), context.getMetadataManager().getContactPoints());
      // Also warn if the configured DC doesn't match any node in the cluster
      if (!nodes.isEmpty()) {
        boolean found = false;
        for (Node node : nodes.values()) {
          if (localDc.get().equals(node.getDatacenter())) {
            found = true;
            break;
          }
        }
        if (!found) {
          LOG.warn(
              "[{}] Configured local DC '{}' does not match any node's datacenter"
                  + " (available DCs: {}); please verify your configuration",
              logPrefix,
              localDc.get(),
              formatDcs(nodes.values()));
        }
      }
    } else {
      LOG.debug("[{}] Local DC not set, DC awareness will be disabled", logPrefix);
    }
    return localDc;
  }

  /**
   * Checks if the contact points are compatible with the local datacenter specified either through
   * configuration, or programmatically.
   *
   * <p>The default implementation logs a warning when a contact point reports a datacenter
   * different from the local one, and only for the default profile.
   *
   * @param localDc The local datacenter, as specified in the config, or programmatically.
   * @param contactPoints The contact points provided when creating the session.
   */
  protected void checkLocalDatacenterCompatibility(
      @NonNull String localDc, Set<? extends Node> contactPoints) {
    if (profile.getName().equals(DriverExecutionProfile.DEFAULT_NAME)) {
      Set<Node> badContactPoints = new LinkedHashSet<>();
      for (Node node : contactPoints) {
        if (!Objects.equals(localDc, node.getDatacenter())) {
          badContactPoints.add(node);
        }
      }
      if (!badContactPoints.isEmpty()) {
        LOG.warn(
            "[{}] You specified {} as the local DC, but some contact points are from a different DC: {}; "
                + "please provide the correct local DC, or check your contact points",
            logPrefix,
            localDc,
            formatNodesAndDcs(badContactPoints));
      }
    }
  }

  /**
   * Infers the local datacenter from the control connection endpoint by matching it against nodes
   * in metadata.
   *
   * <p>If multiple nodes share the same endpoint (e.g., behind an NLB or proxy) and they belong to
   * different datacenters, this method logs a warning and returns empty rather than picking an
   * arbitrary datacenter.
   *
   * @return the datacenter of the node matching the control connection endpoint, or empty if no
   *     match was found or if the match is ambiguous across multiple DCs.
   */
  @NonNull
  protected Optional<String> inferDcFromControlConnection(@NonNull Map<UUID, Node> nodes) {
    Node controlNode = context.getControlConnection().controlNode();
    if (controlNode != null && controlNode.getHostId() != null) {
      Node metadataNode = nodes.get(controlNode.getHostId());
      if (metadataNode != null && metadataNode.getDatacenter() != null) {
        return Optional.of(metadataNode.getDatacenter());
      }
    }
    DriverChannel channel = context.getControlConnection().channel();
    if (channel != null) {
      EndPoint controlEndpoint = channel.getEndPoint();
      Set<String> candidateDcs = new HashSet<>();
      for (Node node : nodes.values()) {
        if (node.getDatacenter() != null && Objects.equals(controlEndpoint, node.getEndPoint())) {
          candidateDcs.add(node.getDatacenter());
        }
      }
      if (candidateDcs.size() == 1) {
        return Optional.of(candidateDcs.iterator().next());
      } else if (candidateDcs.size() > 1) {
        LOG.warn(
            "[{}] Control endpoint {} matches nodes in multiple DCs: {}, skipping inference",
            logPrefix,
            controlEndpoint,
            candidateDcs);
      }
    }
    return Optional.empty();
  }

  /**
   * Formats the given nodes as a string detailing each node and its datacenter, for informational
   * purposes.
   */
  @NonNull
  protected String formatNodesAndDcs(Iterable<? extends Node> nodes) {
    List<String> l = new ArrayList<>();
    for (Node node : nodes) {
      l.add(node + "=" + node.getDatacenter());
    }
    return String.join(", ", l);
  }

  /**
   * Formats the given nodes as a string detailing each distinct datacenter, for informational
   * purposes.
   */
  @NonNull
  protected String formatDcs(Iterable<? extends Node> nodes) {
    List<String> l = new ArrayList<>();
    for (Node node : nodes) {
      if (node.getDatacenter() != null) {
        l.add(node.getDatacenter());
      }
    }
    return String.join(", ", new TreeSet<>(l));
  }
}
