package com.datastax.oss.driver.internal.core.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.SerializationHelper;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ShardingInfoTest {

  @Test
  public void should_create_ShardingInfo_from_valid_params() {
    // Mostly just a sanity check. Parses example data returned by docker instance.
    ShardingInfo.ConnectionShardingInfo info = ShardingInfo.parseShardingInfo(validParams());

    assertThat(info).isNotNull();
    assertThat(info.shardId).isEqualTo(0);
    assertThat(info.shardingInfo.getShardsCount()).isEqualTo(12);
    assertThat(info.shardingInfo.getPartitioner())
        .isEqualTo("org.apache.cassandra.dht.Murmur3Partitioner");
    assertThat(info.shardingInfo.getShardingAlgorithm()).isEqualTo("biased-token-round-robin");
    assertThat(info.shardingInfo.getShardAwarePort()).isEqualTo(19042);
    assertThat(info.shardingInfo.getShardAwarePortSsl()).isNull();
  }

  @Test
  public void should_survive_java_serialization() {
    // Node metadata (DefaultNode) is Serializable and holds a ShardingInfo, so ShardingInfo must be
    // serializable too, e.g. when an AllNodesFailedException crosses a JVM boundary.
    ShardingInfo original = ShardingInfo.parseShardingInfo(validParams()).shardingInfo;

    ShardingInfo copy = SerializationHelper.serializeAndDeserialize(original);

    assertThat(copy.getShardsCount()).isEqualTo(original.getShardsCount());
    assertThat(copy.getPartitioner()).isEqualTo(original.getPartitioner());
    assertThat(copy.getShardingAlgorithm()).isEqualTo(original.getShardingAlgorithm());
    assertThat(copy.getShardAwarePort()).isEqualTo(original.getShardAwarePort());
    assertThat(copy.getShardAwarePortSsl()).isEqualTo(original.getShardAwarePortSsl());
    Murmur3Token token = new Murmur3Token(-4069959284402364209L);
    assertThat(copy.shardId(token)).isEqualTo(original.shardId(token));
  }

  private static Map<String, List<String>> validParams() {
    Map<String, List<String>> params = new HashMap<>();
    params.put("COMPRESSION", Arrays.asList("lz4", "snappy"));
    params.put("CQL_VERSION", Collections.singletonList("3.3.1"));
    params.put(
        "SCYLLA_LWT_ADD_METADATA_MARK",
        Collections.singletonList("LWT_OPTIMIZATION_META_BIT_MASK=2147483648"));
    params.put("SCYLLA_NR_SHARDS", Collections.singletonList("12"));
    params.put(
        "SCYLLA_PARTITIONER",
        Collections.singletonList("org.apache.cassandra.dht.Murmur3Partitioner"));
    params.put("SCYLLA_RATE_LIMIT_ERROR", Collections.singletonList("ERROR_CODE=61440"));
    params.put("SCYLLA_SHARD", Collections.singletonList("0"));
    params.put("SCYLLA_SHARDING_ALGORITHM", Collections.singletonList("biased-token-round-robin"));
    params.put("SCYLLA_SHARDING_IGNORE_MSB", Collections.singletonList("12"));
    params.put("SCYLLA_SHARD_AWARE_PORT", Collections.singletonList("19042"));
    params.put("TABLETS_ROUTING_V1", Collections.emptyList());
    return params;
  }
}
