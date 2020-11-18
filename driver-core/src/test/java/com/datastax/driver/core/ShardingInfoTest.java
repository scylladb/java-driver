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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ShardingInfoTest {

  @Test(groups = "unit")
  public void shardIdCalculationTest() {
    // Verify that our Murmur hash calculates the correct shard for specific keys.
    Token.Factory factory = Token.M3PToken.FACTORY;

    Map<String, List<String>> params =
        new HashMap<String, List<String>>() {
          {
            put("SCYLLA_SHARD", Collections.singletonList("1"));
            put("SCYLLA_NR_SHARDS", Collections.singletonList("12"));
            put(
                "SCYLLA_PARTITIONER",
                Collections.singletonList("org.apache.cassandra.dht.Murmur3Partitioner"));
            put("SCYLLA_SHARDING_ALGORITHM", Collections.singletonList("biased-token-round-robin"));
            put("SCYLLA_SHARDING_IGNORE_MSB", Collections.singletonList("12"));
          }
        };
    ShardingInfo.ConnectionShardingInfo sharding = ShardingInfo.parseShardingInfo(params);
    assertThat(sharding).isNotNull();
    assertThat(sharding.shardId).isEqualTo(1);

    Token token1 =
        factory.hash(
            ByteBuffer.wrap(
                new byte[] {
                  'a',
                }));
    assertThat(sharding.shardingInfo.shardId(token1)).isEqualTo(4);

    Token token2 =
        factory.hash(
            ByteBuffer.wrap(
                new byte[] {
                  'b',
                }));
    assertThat(sharding.shardingInfo.shardId(token2)).isEqualTo(6);

    Token token3 =
        factory.hash(
            ByteBuffer.wrap(
                new byte[] {
                  'c',
                }));
    assertThat(sharding.shardingInfo.shardId(token3)).isEqualTo(6);

    Token token4 =
        factory.hash(
            ByteBuffer.wrap(
                new byte[] {
                  'e',
                }));
    assertThat(sharding.shardingInfo.shardId(token4)).isEqualTo(4);

    Token token5 =
        factory.hash(
            ByteBuffer.wrap(
                new byte[] {
                  '1', '0', '0', '0', '0', '0',
                }));
    assertThat(sharding.shardingInfo.shardId(token5)).isEqualTo(2);
  }
}
