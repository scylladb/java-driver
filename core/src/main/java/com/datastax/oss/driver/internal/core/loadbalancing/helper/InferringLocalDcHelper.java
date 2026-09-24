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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link LocalDcHelper} that fetches the user-supplied datacenter, if any,
 * from the programmatic configuration API, or else, from the driver configuration. If no local
 * datacenter is explicitly defined, this implementation tries to infer it from the control
 * connection endpoint. If that fails, an {@link IllegalStateException} is thrown.
 */
@ThreadSafe
public class InferringLocalDcHelper extends OptionalLocalDcHelper {

  private static final Logger LOG = LoggerFactory.getLogger(InferringLocalDcHelper.class);

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
    // Infer the local DC from the control connection endpoint
    Optional<String> dcFromControl = inferDcFromControlConnection(nodes);
    if (dcFromControl.isPresent()) {
      LOG.info(
          "[{}] Inferred local DC from control connection: {}", logPrefix, dcFromControl.get());
      return dcFromControl;
    }
    // Fallback: try to infer from all cluster nodes
    Set<String> datacenters = new HashSet<>();
    for (Node node : nodes.values()) {
      String datacenter = node.getDatacenter();
      if (datacenter != null) {
        datacenters.add(datacenter);
      }
    }
    if (datacenters.size() == 1) {
      String localDc = datacenters.iterator().next();
      LOG.info("[{}] Inferred local DC from cluster nodes: {}", logPrefix, localDc);
      return Optional.of(localDc);
    }
    if (datacenters.isEmpty()) {
      throw new IllegalStateException(
          "The local DC could not be inferred from cluster nodes, please set it explicitly (see "
              + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
              + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)");
    }
    throw new IllegalStateException(
        String.format(
            "No local DC was provided, but the cluster nodes resolve to nodes in different DCs: %s; "
                + "please set the local DC explicitly (see "
                + DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER.getPath()
                + " in the config, or set it programmatically with SessionBuilder.withLocalDatacenter)",
            formatNodesAndDcs(nodes.values())));
  }
}
