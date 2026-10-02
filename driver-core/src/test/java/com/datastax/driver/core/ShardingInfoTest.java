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
import java.util.Arrays;
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

  private static final List<String> REQUIRED_KEYS =
      Arrays.asList(
          "SCYLLA_SHARD",
          "SCYLLA_NR_SHARDS",
          "SCYLLA_PARTITIONER",
          "SCYLLA_SHARDING_ALGORITHM",
          "SCYLLA_SHARDING_IGNORE_MSB");

  private static Map<String, List<String>> validParams() {
    Map<String, List<String>> params = new HashMap<String, List<String>>();
    params.put("SCYLLA_SHARD", Collections.singletonList("3"));
    params.put("SCYLLA_NR_SHARDS", Collections.singletonList("12"));
    params.put(
        "SCYLLA_PARTITIONER",
        Collections.singletonList("org.apache.cassandra.dht.Murmur3Partitioner"));
    params.put("SCYLLA_SHARDING_ALGORITHM", Collections.singletonList("biased-token-round-robin"));
    params.put("SCYLLA_SHARDING_IGNORE_MSB", Collections.singletonList("12"));
    return params;
  }

  @Test(groups = "unit")
  public void should_parse_valid_params_without_shard_aware_ports() {
    ShardingInfo.ConnectionShardingInfo sharding = ShardingInfo.parseShardingInfo(validParams());

    assertThat(sharding).isNotNull();
    assertThat(sharding.shardId).isEqualTo(3);
    assertThat(sharding.shardingInfo.getShardsCount()).isEqualTo(12);
    assertThat(sharding.shardingInfo.getShardAwarePort(false)).isEqualTo(0);
    assertThat(sharding.shardingInfo.getShardAwarePort(true)).isEqualTo(0);
  }

  @Test(groups = "unit")
  public void should_parse_shard_aware_ports() {
    Map<String, List<String>> params = validParams();
    params.put("SCYLLA_SHARD_AWARE_PORT", Collections.singletonList("19042"));
    params.put("SCYLLA_SHARD_AWARE_PORT_SSL", Collections.singletonList("19142"));

    ShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(params).shardingInfo;

    assertThat(shardingInfo.getShardAwarePort(false)).isEqualTo(19042);
    assertThat(shardingInfo.getShardAwarePort(true)).isEqualTo(19142);
  }

  @Test(groups = "unit")
  public void should_default_only_missing_plain_shard_aware_port() {
    Map<String, List<String>> params = validParams();
    params.put("SCYLLA_SHARD_AWARE_PORT_SSL", Collections.singletonList("19142"));

    ShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(params).shardingInfo;

    assertThat(shardingInfo.getShardAwarePort(false)).isEqualTo(0);
    assertThat(shardingInfo.getShardAwarePort(true)).isEqualTo(19142);
  }

  @Test(groups = "unit")
  public void should_default_only_missing_ssl_shard_aware_port() {
    Map<String, List<String>> params = validParams();
    params.put("SCYLLA_SHARD_AWARE_PORT", Collections.singletonList("19042"));

    ShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(params).shardingInfo;

    assertThat(shardingInfo.getShardAwarePort(false)).isEqualTo(19042);
    assertThat(shardingInfo.getShardAwarePort(true)).isEqualTo(0);
  }

  @Test(groups = "unit")
  public void should_default_shard_aware_port_when_not_a_number() {
    Map<String, List<String>> params = validParams();
    params.put("SCYLLA_SHARD_AWARE_PORT", Collections.singletonList("abc"));
    params.put("SCYLLA_SHARD_AWARE_PORT_SSL", Collections.singletonList("19142a"));

    ShardingInfo shardingInfo = ShardingInfo.parseShardingInfo(params).shardingInfo;

    assertThat(shardingInfo.getShardAwarePort(false)).isEqualTo(0);
    assertThat(shardingInfo.getShardAwarePort(true)).isEqualTo(0);
  }

  @Test(groups = "unit")
  public void should_return_null_when_required_param_missing() {
    for (String key : REQUIRED_KEYS) {
      Map<String, List<String>> params = validParams();
      params.remove(key);
      assertThat(ShardingInfo.parseShardingInfo(params)).as(key).isNull();
    }
  }

  @Test(groups = "unit")
  public void should_return_null_when_required_param_not_single_valued() {
    for (String key : REQUIRED_KEYS) {
      Map<String, List<String>> params = validParams();
      params.put(key, Collections.<String>emptyList());
      assertThat(ShardingInfo.parseShardingInfo(params)).as(key + " empty").isNull();
      String value = validParams().get(key).get(0);
      params.put(key, Arrays.asList(value, value));
      assertThat(ShardingInfo.parseShardingInfo(params)).as(key + " multi").isNull();
    }
  }

  @Test(groups = "unit")
  public void should_return_null_when_int_param_not_a_number() {
    for (String key :
        Arrays.asList("SCYLLA_SHARD", "SCYLLA_NR_SHARDS", "SCYLLA_SHARDING_IGNORE_MSB")) {
      Map<String, List<String>> params = validParams();
      params.put(key, Collections.singletonList("twelve"));
      assertThat(ShardingInfo.parseShardingInfo(params)).as(key).isNull();
    }
  }

  @Test(groups = "unit")
  public void should_return_null_for_unsupported_partitioner() {
    Map<String, List<String>> params = validParams();
    params.put(
        "SCYLLA_PARTITIONER",
        Collections.singletonList("org.apache.cassandra.dht.RandomPartitioner"));

    assertThat(ShardingInfo.parseShardingInfo(params)).isNull();
  }

  @Test(groups = "unit")
  public void should_return_null_for_unsupported_sharding_algorithm() {
    Map<String, List<String>> params = validParams();
    params.put("SCYLLA_SHARDING_ALGORITHM", Collections.singletonList("token-round-robin"));

    assertThat(ShardingInfo.parseShardingInfo(params)).isNull();
  }

  @Test(groups = "unit")
  public void should_accept_out_of_range_shard_values() {
    // Pins current behaviour (#1192): shard count and shard id are not range-checked
    Map<String, List<String>> params = validParams();
    params.put("SCYLLA_NR_SHARDS", Collections.singletonList("0"));
    assertThat(ShardingInfo.parseShardingInfo(params).shardingInfo.getShardsCount()).isEqualTo(0);

    params = validParams();
    params.put("SCYLLA_SHARD", Collections.singletonList("12"));
    assertThat(ShardingInfo.parseShardingInfo(params).shardId).isEqualTo(12);
  }
}
