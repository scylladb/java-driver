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

public class InflightRequestPolicyTest {
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
    when(shardingInfo.shardId(t)).thenReturn(0);
    when(shardingInfo.shardId(t)).thenReturn(0);
    when(host1.isUp()).thenReturn(true);
    when(host2.isUp()).thenReturn(true);
    when(host3.isUp()).thenReturn(true);
  }

  @Test(groups = "unit")
  public void should_round_robin_over_all_hosts() {
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
    InFlightRequestPolicy policy = new InFlightRequestPolicy();
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

    Map<Host, Integer> hostReachedCount = new HashMap<Host, Integer>();
    hostReachedCount.put(host1, 0);
    hostReachedCount.put(host2, 0);
    hostReachedCount.put(host3, 0);

    // when
    for (int i = 0; i < 10; i++) {
      Iterator<Host> queryPlan = policy.newQueryPlan("keyspace", statement);
      while (queryPlan.hasNext()) {
        Host host = queryPlan.next();
        hostReachedCount.put(host, hostReachedCount.get(host) + 1);
      }
    }
    // then
    assertThat(hostReachedCount.get(host1)).isEqualTo(10);
    assertThat(hostReachedCount.get(host2)).isEqualTo(10);
    assertThat(hostReachedCount.get(host3)).isEqualTo(10);
  }

  @Test(groups = "unit")
  public void should_exclude_replicas_out_of_min_inflight_request_range() {
    // given
    Map<Host, Map<Integer, Integer>> perHostInflightRequests =
        new HashMap<Host, Map<Integer, Integer>>() {
          {
            put(
                host1,
                new HashMap<Integer, Integer>() {
                  {
                    put(0, 35);
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
    InFlightRequestPolicy policy = new InFlightRequestPolicy();
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

    Set<Host> hostSet = new HashSet<Host>();
    hostSet.add(host1);
    hostSet.add(host2);
    hostSet.add(host3);

    // when
    Iterator<Host> queryPlan = policy.newQueryPlan("keyspace", statement);
    while (queryPlan.hasNext()) {
      hostSet.remove(queryPlan.next());
    }
    // then
    assertThat(!hostSet.contains(host1));
    assertThat(!hostSet.contains(host2));
    assertThat(hostSet.contains(host3));
  }
}
