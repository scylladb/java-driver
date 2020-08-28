/*
 * Copyright (C) 2018 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import java.util.List;
import java.util.Map;

/** Keeps the information the driver maintains on data layout of a given node. */
public class ShardingInfo {
  private static final String SCYLLA_SHARD_PARAM_KEY = "SCYLLA_SHARD";
  private static final String SCYLLA_NR_SHARDS_PARAM_KEY = "SCYLLA_NR_SHARDS";
  private static final String SCYLLA_PARTITIONER = "SCYLLA_PARTITIONER";
  private static final String SCYLLA_SHARDING_ALGORITHM = "SCYLLA_SHARDING_ALGORITHM";
  private static final String SCYLLA_SHARDING_IGNORE_MSB = "SCYLLA_SHARDING_IGNORE_MSB";
  private static final String SCYLLA_SHARD_AWARE_PORT = "SCYLLA_SHARD_AWARE_PORT";

  private final int shardsCount;
  private final String partitioner;
  private final String shardingAlgorithm;
  private final int shardingIgnoreMSB;
  private final int shardAwarePort;

  private ShardingInfo(
      int shardsCount,
      String partitioner,
      String shardingAlgorithm,
      int shardingIgnoreMSB,
      int shardAwarePort) {
    this.shardsCount = shardsCount;
    this.partitioner = partitioner;
    this.shardingAlgorithm = shardingAlgorithm;
    this.shardingIgnoreMSB = shardingIgnoreMSB;
    this.shardAwarePort = shardAwarePort;
  }

  public int getShardsCount() {
    return shardsCount;
  }

  public int shardId(Token t) {
    long token = Long.parseLong(t.toString());
    token += Long.MIN_VALUE;
    token <<= shardingIgnoreMSB;
    long tokLo = token & 0xffffffffL;
    long tokHi = (token >>> 32) & 0xffffffffL;
    long mul1 = tokLo * shardsCount;
    long mul2 = tokHi * shardsCount; // logically shifted 32 bits
    long sum = (mul1 >>> 32) + mul2;
    return (int) (sum >>> 32);
  }

  public int getShardAwarePort() {
    return shardAwarePort;
  }

  public static class ConnectionShardingInfo {
    public final int shardId;
    public final ShardingInfo shardingInfo;

    private ConnectionShardingInfo(int shardId, ShardingInfo shardingInfo) {
      this.shardId = shardId;
      this.shardingInfo = shardingInfo;
    }
  }

  public static ConnectionShardingInfo parseShardingInfo(Map<String, List<String>> params) {
    Integer shardId = parseInt(params, SCYLLA_SHARD_PARAM_KEY);
    Integer shardsCount = parseInt(params, SCYLLA_NR_SHARDS_PARAM_KEY);
    String partitioner = parseString(params, SCYLLA_PARTITIONER);
    String shardingAlgorithm = parseString(params, SCYLLA_SHARDING_ALGORITHM);
    Integer shardingIgnoreMSB = parseInt(params, SCYLLA_SHARDING_IGNORE_MSB);
    Integer shardAwarePort = parseInt(params, SCYLLA_SHARD_AWARE_PORT);
    if (shardId == null
        || shardsCount == null
        || partitioner == null
        || shardingAlgorithm == null
        || shardingIgnoreMSB == null
        || !partitioner.equals("org.apache.cassandra.dht.Murmur3Partitioner")
        || !shardingAlgorithm.equals("biased-token-round-robin")) {
      return null;
    }
    if (shardAwarePort == null) {
      shardAwarePort = 0;
    }
    return new ConnectionShardingInfo(
        shardId,
        new ShardingInfo(
            shardsCount, partitioner, shardingAlgorithm, shardingIgnoreMSB, shardAwarePort));
  }

  private static String parseString(Map<String, List<String>> params, String key) {
    List<String> val = params.get(key);
    if (val == null || val.size() != 1) {
      return null;
    }
    return val.get(0);
  }

  private static Integer parseInt(Map<String, List<String>> params, String key) {
    String val = parseString(params, key);
    if (val == null) {
      return null;
    }
    try {
      return Integer.valueOf(val);
    } catch (Exception e) {
      return null;
    }
  }
}
