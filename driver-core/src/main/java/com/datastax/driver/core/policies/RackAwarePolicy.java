package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Statement;
import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rack aware round-robin load balancing policy.
 *
 * <p>This policy provides round-robin queries over the node of the local data center and local
 * rack. It also includes in the query plans returned a configurable number of hosts in the remote
 * racks and data centers, but those are always tried after the local nodes. In other words, this
 * policy guarantees that no host in a remote rack or data center will be queried unless no host in
 * the local data center and local rack can be reached.
 *
 * <p>If used with a single data center and a single rack, this policy is equivalent to the {@link
 * RoundRobinPolicy}, but its rack awareness incurs a slight overhead so the latter should be
 * preferred to this policy in that case.
 */
public class RackAwarePolicy implements LoadBalancingPolicy {
  private static final Logger logger = LoggerFactory.getLogger(RackAwarePolicy.class);
  private static final String UNSET = "";

  private volatile String localDc;
  private volatile String localRack;

  private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perDcLiveHosts =
      new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
  // Note: Key is of form <dc + "-" + rack> to avoid collision in case two dc have racks with the
  // same name
  private final ConcurrentMap<String, CopyOnWriteArrayList<Host>> perRackLiveHosts =
      new ConcurrentHashMap<String, CopyOnWriteArrayList<Host>>();
  private final int usedHostsPerRemoteDc;
  private final int usedHostsPerRemoteRack;
  private final AtomicInteger index = new AtomicInteger();

  public RackAwarePolicy(
      final String localDc,
      final String localRack,
      final int usedHostsPerRemoteDc,
      final int usedHostsPerRemoteRack,
      boolean allowEmptyLocalDcOrRack) {
    if (!allowEmptyLocalDcOrRack
        && (Strings.isNullOrEmpty(localDc) || Strings.isNullOrEmpty(localRack))) {
      throw new IllegalArgumentException(
          "Null or empty DC or Rack specified for rack-aware policy");
    }

    this.localDc = localDc == null ? UNSET : localDc;
    this.localRack = localRack != null && localDc != null ? localDc + "-" + localRack : UNSET;
    this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
    this.usedHostsPerRemoteRack = usedHostsPerRemoteRack;
  }

  @Override
  public void init(Cluster cluster, Collection<Host> hosts) {
    if (!localDc.equals(UNSET) && !localRack.equals(UNSET)) {
      logger.info(
          "Using data-center name '{}' and rack name '{}' for RackAwarePolicy", localDc, localRack);
    }

    for (Host host : hosts) {
      String dc = dc(host);
      String rack = dc + "-" + rack(host); // Add DC as prefix to rack name to make it unique

      if (localDc.equals(UNSET) && !dc.equals(UNSET)) {
        logger.info(
            "Using data-center name '{}' for RackAwarePolicy (if this is incorrect, please provide the correct datacenter name with RackAwarePolicy constructor)",
            dc);
        localDc = dc;
      }

      if (localRack.equals(UNSET) && !rack(host).equals(UNSET)) {
        logger.info(
            "Using rack name '{}' for RackAwarePolicy (if this is incorrect, please provide the correct rack name with RackAwarePolicy constructor)",
            rack(host));
        localRack = rack;
      }

      CopyOnWriteArrayList<Host> currentDcLiveHosts = perDcLiveHosts.get(dc);
      if (currentDcLiveHosts == null) {
        perDcLiveHosts.put(dc, new CopyOnWriteArrayList<Host>(Collections.singletonList(host)));
      } else {
        currentDcLiveHosts.addIfAbsent(host);
      }

      CopyOnWriteArrayList<Host> currentRackLiveHosts = perRackLiveHosts.get(rack);
      if (currentRackLiveHosts == null) {
        perRackLiveHosts.put(rack, new CopyOnWriteArrayList<Host>(Collections.singletonList(host)));
      } else {
        currentRackLiveHosts.addIfAbsent(host);
      }
    }

    this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
  }

  private String dc(Host host) {
    String dc = host.getDatacenter();
    return dc == null ? localDc : dc;
  }

  private String rack(Host host) {
    String rack = host.getRack();
    return rack == null ? localRack : rack;
  }

  /**
   * Return the HostDistance for the provided host.
   *
   * <p>This policy considers nodes in the local datacenter and local rack as {@code LOCAL}. For
   * each remote rack in the local datacenter, it considers a configurable number of hosts as {@code
   * REMOTE} and the rest is {@code IGNORED}. For each remote datacenter it considers a configurable
   * number of hosts as {@code FOREIGN} and the rest is {@code IGNORED}.
   *
   * @param host the host of which to return the distance of.
   * @return the HostDistance to {@code host}.
   */
  @Override
  public HostDistance distance(Host host) {
    String dc = dc(host);
    String rack = dc + "-" + rack(host);

    if (localDc.equals(UNSET)
        || localRack.equals(UNSET)
        || (dc.equals(localDc) && rack(host).equals(localRack))) {
      return HostDistance.LOCAL;
    }

    if (dc.equals(localDc)) {
      CopyOnWriteArrayList<Host> rackHosts = perRackLiveHosts.get(rack);
      if (rackHosts == null || usedHostsPerRemoteRack == 0) {
        return HostDistance.IGNORED;
      }

      // The below subList is not thread safe, so copy is necessary
      rackHosts = cloneList(rackHosts);
      if (rackHosts.subList(0, Math.min(rackHosts.size(), usedHostsPerRemoteRack)).contains(host)) {
        return HostDistance.REMOTE;
      }
    }

    CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc);
    if (dcHosts == null || usedHostsPerRemoteDc == 0) {
      return HostDistance.IGNORED;
    }

    dcHosts = cloneList(dcHosts);
    if (dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc)).contains(host)) {
      return HostDistance.FOREIGN;
    }

    return HostDistance.IGNORED;
  }

  @SuppressWarnings("unchecked")
  private static CopyOnWriteArrayList<Host> cloneList(CopyOnWriteArrayList<Host> list) {
    return (CopyOnWriteArrayList<Host>) list.clone();
  }

  private List<Host> getLocalDCRemoteRackHosts(final List<Host> localRackHosts) {
    CopyOnWriteArrayList<Host> localDCLiveHosts = perDcLiveHosts.get(localDc);
    List<Host> localDcAllHosts =
        localDCLiveHosts == null ? Collections.<Host>emptyList() : localDCLiveHosts;
    final List<Host> localDcRemoteHosts = new ArrayList<Host>();

    for (Host localDcHost : localDcAllHosts) {
      if (!localRackHosts.contains(localDcHost)) {
        if (localDcRemoteHosts.size() < usedHostsPerRemoteRack) {
          localDcRemoteHosts.add(localDcHost);
        }
      }
    }

    return localDcRemoteHosts;
  }

  /**
   * {@inheritDoc}
   *
   * <p>The returned plan will always try each known host in the local datacenter and local rack
   * first, and then, if none of the local host is reachable, will try up to a configurable number
   * of other hosts per remote rack, if none of the remote rack hosts is reachable, it will try up
   * to a configurable number of other hosts per foreign datacenter. The order of the local node in
   * the returned query plan will follow a Round-robin algorithm.
   *
   * @param loggedKeyspace the keyspace currently logged in on for this query.
   * @param statement the query for which to build the plan.
   * @return a new query plan, i.e. an iterator indicating which host to try first for querying,
   *     which one to use as failover, etc...
   */
  @Override
  public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
    CopyOnWriteArrayList<Host> localRackLiveHosts = perRackLiveHosts.get(localRack);
    final List<Host> localRackHosts =
        localRackLiveHosts == null ? Collections.<Host>emptyList() : cloneList(localRackLiveHosts);
    final List<Host> localDcRemoteHosts = getLocalDCRemoteRackHosts(localRackHosts);

    final int startIdx = index.getAndIncrement();

    return new AbstractIterator<Host>() {
      private int idx = startIdx;
      private int remainingLocal = localRackHosts.size();
      private int remainingRemote = localDcRemoteHosts.size();
      private Iterator<String> foreignDcs;
      private List<Host> currentDcHosts;
      private int currentDcRemaining;

      @Override
      protected Host computeNext() {
        while (true) {
          // Returns local dc and local rack hosts
          if (remainingLocal > 0) {
            remainingLocal--;
            int c = idx++ % localRackHosts.size();
            if (c < 0) {
              c += localRackHosts.size();
            }
            return localRackHosts.get(c);
          }

          // Returns local dc and non-local rack hosts
          if (remainingRemote > 0) {
            remainingRemote--;
            int c = idx++ % localDcRemoteHosts.size();
            if (c < 0) {
              c += localDcRemoteHosts.size();
            }
            return localDcRemoteHosts.get(c);
          }

          // Returns remaining hosts in remote datacenters
          if (currentDcHosts != null && currentDcRemaining > 0) {
            currentDcRemaining--;
            int c = idx++ % currentDcHosts.size();
            if (c < 0) {
              c += currentDcHosts.size();
            }
            return currentDcHosts.get(c);
          }

          if (foreignDcs == null) {
            Set<String> copy = new HashSet<String>(perDcLiveHosts.keySet());
            copy.remove(localDc);
            foreignDcs = copy.iterator();
          }

          if (!foreignDcs.hasNext()) {
            break;
          }

          String nextRemoteDc = foreignDcs.next();
          CopyOnWriteArrayList<Host> nextDcHosts = perDcLiveHosts.get(nextRemoteDc);
          if (nextDcHosts != null) {
            // Clone for thread safety
            List<Host> dcHosts = cloneList(nextDcHosts);
            currentDcHosts = dcHosts.subList(0, Math.min(dcHosts.size(), usedHostsPerRemoteDc));
            currentDcRemaining = currentDcHosts.size();
          }
        }
        return endOfData();
      }
    };
  }

  @Override
  public void onAdd(Host host) {
    onUp(host);
  }

  @Override
  public void onUp(Host host) {
    String dc = dc(host);
    String rack = dc + "-" + rack(host);

    if (localDc.equals(UNSET) && !dc.equals(UNSET)) {
      localDc = dc;
    }

    if (localRack.equals(UNSET) && !rack(host).equals(UNSET)) {
      logger.info(
          "Using datacenter name '{}' and rack name '{}' for RackAwarePolicy (if this is incorrect, please provide the correct names with RackAwarePolicy constructor)",
          dc,
          rack(host));
      localRack = rack;
    }

    addToConcurrentMapValues(perDcLiveHosts, dc, host);
    addToConcurrentMapValues(perRackLiveHosts, rack, host);
  }

  private void addToConcurrentMapValues(
      ConcurrentMap<String, CopyOnWriteArrayList<Host>> map, String key, Host host) {
    CopyOnWriteArrayList<Host> hosts = map.get(key);
    if (hosts == null) {
      CopyOnWriteArrayList<Host> newList =
          new CopyOnWriteArrayList<Host>(Collections.singletonList(host));
      map.put(key, newList);
    } else {
      hosts.add(host);
    }
  }

  @Override
  public void onDown(Host host) {
    CopyOnWriteArrayList<Host> dcHosts = perDcLiveHosts.get(dc(host));
    if (dcHosts != null) {
      dcHosts.remove(host);
    }

    String rack = dc(host) + "-" + rack(host);
    CopyOnWriteArrayList<Host> rackHosts = perRackLiveHosts.get(rack);
    if (rackHosts != null) {
      rackHosts.remove(host);
    }
  }

  @Override
  public void onRemove(Host host) {
    onDown(host);
  }

  @Override
  public void close() {
    // nothing to do
  }
}
