package com.datastax.driver.core.policies;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.datastax.driver.core.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.assertj.core.util.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RandomTwoChoicePolicyTest {
  private final ByteBuffer routingKey = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});
  private final RegularStatement statement =
      new SimpleStatement("irrelevant").setRoutingKey(routingKey).setKeyspace("keyspace");
  private final Host host1 = mock(Host.class);
  private final Host host2 = mock(Host.class);
  private final Host host3 = mock(Host.class);
  private Cluster cluster;

  @SuppressWarnings("unchecked")
  private final Gauge<Map<Host, Map<Integer, Integer>>> gauge =
      mock((Class<Gauge<Map<Host, Map<Integer, Integer>>>>) (Object) Gauge.class);

  @BeforeMethod(groups = "unit")
  public void initMocks() {
    CodecRegistry codecRegistry = new CodecRegistry();
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    Metadata metadata = mock(Metadata.class);
    Metrics metrics = mock(Metrics.class);
    Token t = mock(Token.class);
    ShardingInfo shardingInfo = mock(ShardingInfo.class);

    when(metrics.getPerShardInflightRequestInfo()).thenReturn(gauge);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getCodecRegistry()).thenReturn(codecRegistry);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(cluster.getMetadata()).thenReturn(metadata);
    when(cluster.getMetrics()).thenReturn(metrics);
    when(metadata.getReplicas(Metadata.quote("keyspace"), null, routingKey))
        .thenReturn(Sets.newLinkedHashSet(host1, host2, host3));
    when(metadata.newToken(null, routingKey)).thenReturn(t);
    when(host1.getShardingInfo()).thenReturn(shardingInfo);
    when(host2.getShardingInfo()).thenReturn(shardingInfo);
    when(host3.getShardingInfo()).thenReturn(shardingInfo);
    when(shardingInfo.shardId(t)).thenReturn(0);
    when(host1.isUp()).thenReturn(true);
    when(host2.isUp()).thenReturn(true);
    when(host3.isUp()).thenReturn(true);
  }

  @Test(groups = "unit")
  public void should_prefer_host_with_less_inflight_requests() {
    // given
    Map<Host, Map<Integer, Integer>> perHostInflightRequests =
        new HashMap<Host, Map<Integer, Integer>>() {
          {
            put(
                host1,
                new HashMap<Integer, Integer>() {
                  {
                    put(0, 6);
                  }
                });
            put(
                host2,
                new HashMap<Integer, Integer>() {
                  {
                    put(0, 2);
                  }
                });
            put(
                host3,
                new HashMap<Integer, Integer>() {
                  {
                    put(0, 4);
                  }
                });
          }
        };
    RandomTwoChoicePolicy policy =
        new RandomTwoChoicePolicy(
            new TokenAwarePolicy(
                new RoundRobinPolicy(), TokenAwarePolicy.ReplicaOrdering.TOPOLOGICAL));
    policy.init(
        cluster,
        new ArrayList<Host>() {

          {
            add(host1);
            add(host2);
            add(host3);
          }
        });
    when(gauge.getValue()).thenReturn(perHostInflightRequests);

    Iterator<Host> queryPlan = policy.newQueryPlan("keyspace", statement);
    // host2 should appear first in the query plan with fewer inflight requests than host1

    assertThat(queryPlan.next()).isEqualTo(host2);
    assertThat(queryPlan.next()).isEqualTo(host1);
    assertThat(queryPlan.next()).isEqualTo(host3);
  }
}
