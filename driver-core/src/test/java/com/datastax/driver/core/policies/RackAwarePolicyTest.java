package com.datastax.driver.core.policies;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.assertj.core.api.Condition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RackAwarePolicyTest {
  private RegularStatement statement = new SimpleStatement("irrelevant");
  private Host host1 = mock(Host.class);
  private Host host2 = mock(Host.class);
  private Host host3 = mock(Host.class);
  private Host host4 = mock(Host.class);
  private Host host5 = mock(Host.class);
  private Host host6 = mock(Host.class);

  private Cluster cluster;

  @BeforeMethod(groups = "short")
  public void setUp() {
    cluster = mock(Cluster.class);
    when(host1.isUp()).thenReturn(true);
    when(host1.getDatacenter()).thenReturn("dc1");
    when(host1.getRack()).thenReturn("rack1");

    when(host2.isUp()).thenReturn(true);
    when(host2.getDatacenter()).thenReturn("dc1");
    when(host2.getRack()).thenReturn("rack1");

    when(host3.isUp()).thenReturn(true);
    when(host3.getDatacenter()).thenReturn("dc1");
    when(host3.getRack()).thenReturn("rack1");

    when(host4.isUp()).thenReturn(true);
    when(host4.getDatacenter()).thenReturn("dc1");
    when(host4.getRack()).thenReturn("rack2");

    when(host5.isUp()).thenReturn(true);
    when(host5.getDatacenter()).thenReturn("dc1");
    when(host5.getRack()).thenReturn("rack2");

    when(host6.isUp()).thenReturn(true);
    when(host6.getDatacenter()).thenReturn("dc2");
    when(host6.getRack()).thenReturn("rack1");
  }

  @Test(groups = "short")
  public void should_first_round_robin_within_local_dc_local_rack() {
    // given
    RackAwarePolicy policy = new RackAwarePolicy("dc1", "rack1", 3, 3, false);
    policy.init(cluster, Arrays.asList(host1, host2, host3, host4, host5, host6));

    // when
    Iterator<Host> queryPlan = policy.newQueryPlan("keyspace", statement);

    // then
    List<Host> result = new ArrayList<Host>();
    result.add(queryPlan.next());
    result.add(queryPlan.next());
    result.add(queryPlan.next());
    assertThat(result).containsOnly(host1, host2, host3);
  }

  @Test(groups = "short")
  public void should_use_limited_remote_rack_hosts() {
    // given
    RackAwarePolicy policy = new RackAwarePolicy("dc1", "rack1", 3, 1, false);
    policy.init(cluster, Arrays.asList(host1, host2, host3, host4, host5, host6));

    // when
    Iterator<Host> queryPlan = policy.newQueryPlan("keyspace", statement);

    // then
    // Omit local dc and local rack hosts
    queryPlan.next();
    queryPlan.next();
    queryPlan.next();
    // One of the next two hosts should not be in local dc and local rack
    final Host remoteHost1 = queryPlan.next();
    final Host remoteHost2 = queryPlan.next();

    assertThat(remoteHost1)
        .is(
            new Condition<Host>() {
              @Override
              public boolean matches(Host host) {
                return host.equals(host4) || host.equals(host5);
              }
            });
    assertThat(remoteHost2).isEqualTo(host6);
  }

  @Test(groups = "short")
  public void should_not_include_remote_hosts_or_foreign_dc_hosts() {
    // given
    RackAwarePolicy policy = new RackAwarePolicy("dc1", "rack1", 0, 0, false);
    policy.init(cluster, Arrays.asList(host1, host2, host3, host4, host5, host6));

    // when
    Iterator<Host> queryPlan = policy.newQueryPlan("keyspace", statement);

    // then
    List<Host> result = new ArrayList<Host>();
    while (queryPlan.hasNext()) {
      result.add(queryPlan.next());
    }

    assertThat(result).doesNotContain(host4, host5, host6);
  }
}
