/*
 * Copyright DataStax, Inc.
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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A load balancing policy wrapper that ensures that only hosts matching the predicate will ever be
 * returned.
 *
 * <p>This policy wraps another load balancing policy and will delegate the choice of hosts to the
 * wrapped policy with the exception that only hosts matching the predicate provided when
 * constructing this policy will ever be returned. Any host not matching the predicate will be
 * considered {@code IGNORED} and thus will not be connected to.
 */
public class HostFilterPolicy implements ChainableLoadBalancingPolicy {
  private final LoadBalancingPolicy childPolicy;
  private final Predicate<Host> predicate;
  private final ImmutableSet<String> whiteListedDcs;

  /**
   * Create a new policy that wraps the provided child policy but only "allows" hosts matching the
   * predicate.
   *
   * @param childPolicy the wrapped policy.
   * @param predicate the host predicate. Only hosts matching this predicate may get connected to
   *     (whether they will get connected to or not depends on the child policy).
   */
  public HostFilterPolicy(LoadBalancingPolicy childPolicy, Predicate<Host> predicate) {
    this(childPolicy, predicate, ImmutableSet.<String>of());
  }

  private HostFilterPolicy(
      LoadBalancingPolicy childPolicy,
      Predicate<Host> predicate,
      ImmutableSet<String> whiteListedDcs) {
    this.childPolicy = childPolicy;
    this.predicate = predicate;
    this.whiteListedDcs = whiteListedDcs;
  }

  @Override
  public LoadBalancingPolicy getChildPolicy() {
    return childPolicy;
  }

  /**
   * The datacenters this policy restricts the session to, or an empty set if that is not something
   * it can say.
   *
   * <p>A {@link Predicate} is opaque once built, so only {@link
   * #fromDCWhiteList(LoadBalancingPolicy, Iterable)} populates this: it is the one factory whose
   * argument <em>is</em> the set of allowed datacenters. An instance built from {@link
   * #fromDCBlackList(LoadBalancingPolicy, Iterable)} or from a caller-supplied predicate returns an
   * empty set — a denied datacenter names no preferred one, and an arbitrary predicate cannot be
   * read back at all.
   *
   * @return an immutable set of allowed datacenter names, empty when unknown.
   */
  @Beta
  public Set<String> getWhiteListedDatacenters() {
    return whiteListedDcs;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if none of the host in {@code hosts} (which will correspond to
   *     the contact points) matches the predicate.
   */
  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    List<Host> whiteHosts = new ArrayList<Host>(hosts.size());
    for (Host host : hosts) if (predicate.apply(host)) whiteHosts.add(host);

    if (whiteHosts.isEmpty())
      throw new IllegalArgumentException(
          String.format(
              "Cannot use HostFilterPolicy where the filter allows none of the contacts points (%s)",
              hosts));

    childPolicy.init(cluster, whiteHosts);
  }

  /**
   * {@inheritDoc}
   *
   * @return {@link HostDistance#IGNORED} if {@code host} is not matching the predicate, the
   *     HostDistance as returned by the wrapped policy otherwise.
   */
  @Override
  public HostDistance distance(Host host) {
    return predicate.apply(host) ? childPolicy.distance(host) : HostDistance.IGNORED;
  }

  /**
   * {@inheritDoc}
   *
   * <p>It is guaranteed that only hosts matching the predicate will be returned.
   */
  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    // Just delegate to the child policy, since we filter the hosts not white
    // listed upfront, the child policy will never see a host that is not white
    // listed and thus can't return one.
    return childPolicy.newQueryPlan(loggedKeyspace, statement);
  }

  @Override
  public void onUp(Host host) {
    if (predicate.apply(host)) childPolicy.onUp(host);
  }

  @Override
  public void onDown(Host host) {
    if (predicate.apply(host)) childPolicy.onDown(host);
  }

  @Override
  public void onAdd(Host host) {
    if (predicate.apply(host)) childPolicy.onAdd(host);
  }

  @Override
  public void onRemove(Host host) {
    if (predicate.apply(host)) childPolicy.onRemove(host);
  }

  @Override
  public void close() {
    childPolicy.close();
  }

  /**
   * Create a new policy that wraps the provided child policy but only "allows" hosts whose DC
   * belongs to the provided list.
   *
   * @param childPolicy the wrapped policy.
   * @param dcs the DCs.
   * @return the policy.
   */
  public static HostFilterPolicy fromDCWhiteList(
      LoadBalancingPolicy childPolicy, Iterable<String> dcs) {
    ImmutableSet<String> whiteListedDcs = ImmutableSet.copyOf(dcs);
    return new HostFilterPolicy(childPolicy, hostDCPredicate(whiteListedDcs, true), whiteListedDcs);
  }

  /**
   * Create a new policy that wraps the provided child policy but only "forbids" hosts whose DC
   * belongs to the provided list.
   *
   * @param childPolicy the wrapped policy.
   * @param dcs the DCs.
   * @return the policy.
   */
  public static HostFilterPolicy fromDCBlackList(
      LoadBalancingPolicy childPolicy, Iterable<String> dcs) {
    return new HostFilterPolicy(childPolicy, Predicates.not(hostDCPredicate(dcs, false)));
  }

  private static Predicate<Host> hostDCPredicate(
      Iterable<String> dcs, final boolean includeNullDC) {
    final ImmutableSet<String> _dcs = ImmutableSet.copyOf(dcs);
    return new Predicate<Host>() {
      @Override
      public boolean apply(Host host) {
        String hdc = host.getDatacenter();
        return (hdc == null) ? includeNullDC : _dcs.contains(hdc);
      }
    };
  }
}
