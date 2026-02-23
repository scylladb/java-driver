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
package com.datastax.oss.driver.internal.core.clientroutes;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class ClientRoutesAddressTranslator implements AddressTranslator {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRoutesAddressTranslator.class);
  private final InternalDriverContext context;
  private final String logPrefix;

  public ClientRoutesAddressTranslator(@NonNull InternalDriverContext context) {
    this.context = context;
    this.logPrefix = context.getSessionName();
  }

  @NonNull
  @Override
  public InetSocketAddress translate(@NonNull InetSocketAddress address) {
    // Contact points bypass translation
    return address;
  }

  @NonNull
  @Override
  public InetSocketAddress translate(
      @NonNull InetSocketAddress address,
      @Nullable UUID hostId,
      @Nullable String datacenter,
      @Nullable String rack) {
    // Contact points (hostId == null) bypass translation
    if (hostId == null) {
      return address;
    }

    // Lazily get the handler from context to avoid initialization order issues
    ClientRoutesHandler handler = context.getClientRoutesHandler();
    if (handler == null) {
      // This shouldn't happen if ClientRoutesAddressTranslator is only used when configured,
      // but handle gracefully just in case
      LOG.warn("[{}] ClientRoutesHandler is null, using original address", logPrefix);
      return address;
    }

    boolean useSsl = context.getSslEngineFactory().isPresent();
    InetSocketAddress translated = handler.translate(hostId, useSsl);
    if (translated != null) {
      LOG.debug(
          "[{}] Translated {}:{} (host_id={}) to {}:{}",
          logPrefix,
          address.getHostString(),
          address.getPort(),
          hostId,
          translated.getHostString(),
          translated.getPort());
      return translated;
    }
    // Fallback to original address if no route found
    LOG.debug(
        "[{}] No client route found for host_id={}, using original address {}:{}",
        logPrefix,
        hostId,
        address.getHostString(),
        address.getPort());
    return address;
  }

  @Override
  public void close() {
    // Handler lifecycle is managed by DefaultDriverContext, not by this translator.
    // Multiple components may reference the handler, so we don't close it here.
    // The handler will be closed when the session closes through the context's cleanup.
  }
}
