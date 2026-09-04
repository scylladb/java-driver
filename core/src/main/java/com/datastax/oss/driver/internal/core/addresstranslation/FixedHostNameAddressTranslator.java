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
package com.datastax.oss.driver.internal.core.addresstranslation;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This translator always returns same hostname, no matter what IP address a node has but still
 * using its native transport port.
 *
 * <p>The translator can be used for scenarios when all nodes are behind some kind of proxy, and it
 * is not tailored for one concrete use case. One can use this, for example, for cloud private
 * endpoint services (such as AWS PrivateLink, Azure Private Link, or GCP Private Service Connect)
 * where all nodes are exposed to the consumer behind one hostname pointing to a single load
 * balancer endpoint.
 *
 * <p>For cloud private-endpoint deployments with per-node routing, see the client routes feature
 * ({@link com.datastax.oss.driver.api.core.config.ClientRoutesConfig}) instead. This translator is
 * suitable when all nodes share a single hostname.
 */
public class FixedHostNameAddressTranslator implements AddressTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FixedHostNameAddressTranslator.class);

  private final String advertisedHostname;
  private final String logPrefix;

  public FixedHostNameAddressTranslator(@NonNull DriverContext context) {
    logPrefix = context.getSessionName();
    advertisedHostname =
        context.getConfig().getDefaultProfile().getString(ADDRESS_TRANSLATOR_ADVERTISED_HOSTNAME);
  }

  /**
   * {@inheritDoc}
   *
   * <p>The advertised host name is returned {@linkplain InetSocketAddress#isUnresolved()
   * unresolved}, so that {@link com.datastax.oss.driver.internal.core.channel.ChannelFactory}
   * expands it per connect and can try every address it maps to. Resolving it here -- which {@code
   * new InetSocketAddress(String, int)} does eagerly -- would freeze the whole cluster on whichever
   * address the JDK happened to return first, and no other would ever be tried: the resolver
   * reports an already-resolved address as nothing to do, so the expansion is skipped entirely.
   * That matters precisely for the deployment this translator is for, where one name fronts a proxy
   * or load balancer that is itself typically several addresses.
   */
  @NonNull
  @Override
  public InetSocketAddress translate(@NonNull InetSocketAddress address) {
    final int port = address.getPort();
    LOG.debug("[{}] Resolved {}:{} to {}:{}", logPrefix, address, port, advertisedHostname, port);
    return InetSocketAddress.createUnresolved(advertisedHostname, port);
  }

  @Override
  public void close() {}
}
