/*
 * Copyright ScyllaDB, Inc.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ShardingInfoTest {

  @Test(groups = "unit")
  public void should_parsing_and_calculating_shard_id() {
    /** verify that our murmur hash takes us to correct shard for specific keys */
    Token.Factory factory = Token.M3PToken.FACTORY;

    Map<String, List<String>> params =
        new HashMap<String, List<String>>() {
          {
            put(
                "SCYLLA_SHARD",
                new ArrayList<String>() {
                  {
                    add("1");
                  }
                });
            put(
                "SCYLLA_NR_SHARDS",
                new ArrayList<String>() {
                  {
                    add("12");
                  }
                });
            put(
                "SCYLLA_PARTITIONER",
                new ArrayList<String>() {
                  {
                    add("org.apache.cassandra.dht.Murmur3Partitioner");
                  }
                });
            put(
                "SCYLLA_SHARDING_ALGORITHM",
                new ArrayList<String>() {
                  {
                    add("biased-token-round-robin");
                  }
                });
            put(
                "SCYLLA_SHARDING_IGNORE_MSB",
                new ArrayList<String>() {
                  {
                    add("12");
                  }
                });
          }
        };
    ShardingInfo.ConnectionShardingInfo sharding = ShardingInfo.parseShardingInfo(params);
    assertThat(sharding.shardId).isEqualTo(1);

    byte[] byte_array1 = {
      'a',
    };
    assertThat(sharding.shardingInfo.shardId(factory.hash(ByteBuffer.wrap(byte_array1))))
        .isEqualTo(4);

    byte[] byte_array2 = {
      'b',
    };
    assertThat(sharding.shardingInfo.shardId(factory.hash(ByteBuffer.wrap(byte_array2))))
        .isEqualTo(6);

    byte[] byte_array3 = {
      'c',
    };
    assertThat(sharding.shardingInfo.shardId(factory.hash(ByteBuffer.wrap(byte_array3))))
        .isEqualTo(6);

    byte[] byte_array4 = {
      'e',
    };
    assertThat(sharding.shardingInfo.shardId(factory.hash(ByteBuffer.wrap(byte_array4))))
        .isEqualTo(4);

    byte[] byte_array5 = {'1', '0', '0', '0', '0', '0'};
    assertThat(sharding.shardingInfo.shardId(factory.hash(ByteBuffer.wrap(byte_array5))))
        .isEqualTo(2);
  }
}
