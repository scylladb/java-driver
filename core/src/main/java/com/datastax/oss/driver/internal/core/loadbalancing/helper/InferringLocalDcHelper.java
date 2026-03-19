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

import static com.datastax.oss.driver.internal.core.time.Clock.LOG;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

/**
 * An implementation of {@link LocalDcHelper} that fetches the user-supplied datacenter, if any,
 * from the programmatic configuration API, or else, from the driver configuration. If no local
 * datacenter is explicitly defined, this implementation infers the local datacenter from the
 * control node (the node the control connection is connected to). If the control node's datacenter
 * is not available, an {@link IllegalStateException} is thrown.
 */
@ThreadSafe
public class InferringLocalDcHelper extends OptionalLocalDcHelper {

  public InferringLocalDcHelper(
      @NonNull InternalDriverContext context,
      @NonNull DriverExecutionProfile profile,
      @NonNull String logPrefix) {
    super(context, profile, logPrefix);
  }

  /** @return The local datacenter; always present. */
  @NonNull
  @Override
  public Optional<String> discoverLocalDc(@NonNull Map<UUID, Node> nodes) {
    Optional<String> optionalLocalDc = super.discoverLocalDc(nodes);
    if (optionalLocalDc.isPresent()) {
      return optionalLocalDc;
    }
    // Infer from the control node — its datacenter is the local DC.
    DriverChannel controlChannel =
        context.getControlConnection() != null ? context.getControlConnection().channel() : null;
    if (controlChannel != null) {
      EndPoint controlEndPoint = controlChannel.getEndPoint();
      for (Node node : nodes.values()) {
        if (node.getEndPoint().equals(controlEndPoint)) {
          String datacenter = node.getDatacenter();
          if (datacenter != null) {
            LOG.info("[{}] Inferred local DC from control node: {}", logPrefix, datacenter);
            return Optional.of(datacenter);
          }
          break;
        }
      }
    }
    // Fallback: if all nodes share the same DC, use it.
    Set<String> datacenters = new HashSet<>();
    for (Node node : nodes.values()) {
      String datacenter = node.getDatacenter();
      if (datacenter != null) {
        datacenters.add(datacenter);
      }
    }
    if (datacenters.size() == 1) {
      return Optional.of(datacenters.iterator().next());
    }
    if (datacenters.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "The local DC could not be inferred (nodes are in different DCs: %s), "
                  + "please set it explicitly (see "
                  + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                  + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)",
              formatNodesAndDcs(nodes.values())));
    }
    throw new IllegalStateException(
        "The local DC could not be inferred, please set it explicitly (see "
            + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
            + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)");
  }
}
