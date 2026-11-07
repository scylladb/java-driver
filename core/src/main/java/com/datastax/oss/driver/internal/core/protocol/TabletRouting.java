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
package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Tablet;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.metadata.token.TokenLong64;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/** Shared tablet lookup and sampled version encoding for routing and prepared requests. */
public final class TabletRouting {
  private TabletRouting() {}

  @Nullable
  public static CqlIdentifier getKeyspace(
      Request request, @Nullable CqlIdentifier defaultKeyspace) {
    CqlIdentifier keyspace = request.getKeyspace();
    if (keyspace == null) {
      keyspace = request.getRoutingKeyspace();
    }
    return keyspace == null ? defaultKeyspace : keyspace;
  }

  @Nullable
  public static Tablet getTablet(
      Request request, @Nullable CqlIdentifier defaultKeyspace, Metadata metadata) {
    TabletMap tabletMap = metadata.getTabletMap().orElse(null);
    CqlIdentifier keyspace = getKeyspace(request, defaultKeyspace);
    CqlIdentifier table = request.getRoutingTable();
    if (tabletMap == null || keyspace == null || table == null) {
      return null;
    }
    Token token = request.getRoutingToken();
    if (token == null) {
      ByteBuffer key = request.getRoutingKey();
      Partitioner partitioner = request.getPartitioner();
      if (partitioner == null && metadata.getTokenMap().isPresent()) {
        partitioner = metadata.getTokenMap().get().getPartitioner();
      }
      if (key != null && partitioner != null) {
        token = partitioner.hash(key);
      }
    }
    return token instanceof TokenLong64
        ? tabletMap.getTablet(keyspace, table, ((TokenLong64) token).getValue())
        : null;
  }

  public static byte versionBlock(@Nullable Tablet tablet) {
    Long version = tablet == null ? null : tablet.getTabletVersion();
    if (version == null) {
      return (byte) ThreadLocalRandom.current().nextInt(256);
    }
    int block = ThreadLocalRandom.current().nextInt(16);
    return (byte) ((block << 4) | ((version >>> (block * 4)) & 15));
  }
}
