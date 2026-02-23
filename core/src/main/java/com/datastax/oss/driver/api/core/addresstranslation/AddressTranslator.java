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
package com.datastax.oss.driver.api.core.addresstranslation;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * Translates IP addresses received from Cassandra nodes into locally queriable addresses.
 *
 * <p>The driver auto-detects new Cassandra nodes added to the cluster through server side pushed
 * notifications and system table queries. For each node, the address the driver will receive will
 * correspond to the address set as {@code broadcast_rpc_address} in the node's YAML file. In most
 * cases, this is the correct address to use by the driver, and that is what is used by default.
 * However, sometimes the addresses received through this mechanism will either not be reachable
 * directly by the driver, or should not be the preferred address to use to reach the node (for
 * instance, the {@code broadcast_rpc_address} set on Cassandra nodes might be a private IP, but
 * some clients may have to use a public IP, or go through a router to reach that node). This
 * interface addresses such cases, by allowing to translate an address as sent by a Cassandra node
 * into another address to be used by the driver for connection.
 *
 * <p>The contact point addresses provided at driver initialization are considered translated
 * already; in other words, they will be used as-is, without being processed by this component.
 */
public interface AddressTranslator extends AutoCloseable {

  /**
   * Translates an address reported by a Cassandra node into the address that the driver will use to
   * connect.
   */
  @NonNull
  InetSocketAddress translate(@NonNull InetSocketAddress address);

  /**
   * Translates an address reported by a Cassandra node into the address that the driver will use to
   * connect, with additional node metadata for context.
   *
   * <p>This method is called during node discovery and allows implementations to use the node's
   * host ID, datacenter, and rack to make translation decisions. For example, the client routes
   * handler uses the host ID to look up the appropriate endpoint mapping.
   *
   * <p>The default implementation delegates to {@link #translate(InetSocketAddress)}, ignoring the
   * additional parameters. This ensures backward compatibility with existing implementations.
   *
   * @param address the broadcast RPC address of the node
   * @param hostId the unique identifier of the node (may be null for contact points)
   * @param datacenter the datacenter of the node (may be null if not yet known)
   * @param rack the rack of the node (may be null if not yet known)
   * @return the translated address
   */
  @NonNull
  default InetSocketAddress translate(
      @NonNull InetSocketAddress address,
      @Nullable UUID hostId,
      @Nullable String datacenter,
      @Nullable String rack) {
    return translate(address);
  }

  /** Called when the cluster that this translator is associated with closes. */
  @Override
  void close();
}
