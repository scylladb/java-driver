// Copyright (C) 2018 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

public class PagingOptimizingLoadBalancingPolicy implements LoadBalancingPolicy {
  private final LoadBalancingPolicy wrapped;
  private volatile CopyOnWriteArrayList<Host> hosts;

  public PagingOptimizingLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
    wrapped = loadBalancingPolicy;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    this.hosts = new CopyOnWriteArrayList<Host>(hosts);
    wrapped.init(cluster, hosts);
  }

  @Override
  public HostDistance distance(Host host) {
    return wrapped.distance(host);
  }

  private class WithFirstIterator implements Iterator<Host> {

    private Host firstToReturn;
    private final Iterator<Host> wrapped;

    public WithFirstIterator(Host host, Iterator<Host> iterator) {
      firstToReturn = host;
      wrapped = iterator;
    }

    @Override
    public boolean hasNext() {
      return wrapped.hasNext() || hasValidFirstToReturn();
    }

    @Override
    public Host next() {
      if (hasValidFirstToReturn()) {
        final Host result = firstToReturn;
        firstToReturn = null;
        return result;
      }
      return wrapped.next();
    }

    private boolean hasValidFirstToReturn() {
      if (firstToReturn == null) {
        return false;
      }
      return hosts.contains(firstToReturn);
    }
  }

  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    final Iterator<Host> inner = wrapped.newQueryPlan(loggedKeyspace, statement);
    final Host lastHost = statement.getLastHost();
    return lastHost == null ? inner : new WithFirstIterator(lastHost, inner);
  }

  @Override
  public void onAdd(Host host) {
    hosts.addIfAbsent(host);
    wrapped.onAdd(host);
  }

  @Override
  public void onUp(Host host) {
    hosts.addIfAbsent(host);
    wrapped.onUp(host);
  }

  @Override
  public void onDown(Host host) {
    hosts.remove(host);
    wrapped.onDown(host);
  }

  @Override
  public void onRemove(Host host) {
    hosts.remove(host);
    wrapped.onRemove(host);
  }

  @Override
  public void close() {
    wrapped.close();
  }
}
