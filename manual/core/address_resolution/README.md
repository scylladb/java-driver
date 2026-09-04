<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Address resolution

### Quick overview

The driver uses `system.peers.rpc-address` to connect to newly discovered nodes. For special network
topologies, an address translation component can be plugged in.

* `advanced.address-translator` in the configuration.
* none by default. Also available: EC2-specific (for deployments that span multiple regions), or
  write your own.

-----

Each node in the Cassandra cluster is uniquely identified by an address that the driver will use to
establish connections.

* for contact points, these are provided as part of configuring the `CqlSession` object;
* for other nodes, addresses will be discovered dynamically, either by inspecting `system.peers` on
  already connected nodes, or via push notifications received on the control connection when new
  nodes are discovered by gossip.

That address does not have to be an IP: see [multi-address resolution](#multi-address-resolution)
below.


### Multi-address resolution

An address the driver holds as a **hostname** is resolved at connection time, and expanded to *every*
address the name maps to; each one is tried in turn until a connection succeeds. So a single
unreachable IP behind a multi-record name no longer fails the connection, and a DNS record change is
picked up on the next connection attempt rather than requiring a restart.

This applies wherever the driver holds a name rather than an IP:

* **contact points** — kept as you wrote them, and re-resolved per connection attempt. (The
  `advanced.resolve-contact-points` option, which used to resolve them once at startup and turn each
  A-record into a separate `Node`, is deprecated and has no effect.) The exception is a contact point
  you pass programmatically as an *already-resolved* `InetSocketAddress`: there is no name left in it
  to re-resolve, so it stays bound to that one address. Use
  `InetSocketAddress.createUnresolved(host, port)`, or configure the contact points as strings under
  `basic.contact-points`, if you want the name expanded — see `SessionBuilder.addContactPoints`.
* the **Cloud SNI proxy** address, and a **client route** hostname (see below);
* whatever a custom [`AddressTranslator`](#driver-side-address-translation) hands back —
  `SubnetAddressTranslator` returns a name by default, under `resolve-addresses = false`.

Two configuration options matter:

* `advanced.connection.max-candidate-addresses` (default `5`) caps how many addresses **one**
  connection attempt tries. Each one costs a TCP connect plus the protocol handshake, and the
  attempts are serial, so this bounds what a single attempt can cost in time. Set it to `1` to
  restore the pre-multi-address behaviour of one address per attempt.
* `advanced.control-connection.reconnection.fallback-to-original-contact-points` (default `true`)
  appends the original contact points to the control connection's reconnection plan, after the live
  nodes the load balancing policy offers. This is the driver's DNS re-resolution path: a node
  discovered from `system.peers` holds an already-resolved address that is never re-resolved, and so
  does the node the control connection is on — it is registered under the address it reached, not
  under the contact point it was reached through — so falling back to the contact-point hostnames is
  what lets a reconnect pick up new IPs. Turning it off therefore disables DNS re-resolution
  altogether, rather than narrowing it.

  Two exceptions, where the option is close to a no-op. Under an `AddressTranslator` that returns a
  hostname, every node's endpoint stays unresolved and re-expands per attempt regardless. And in a
  Cloud (SNI) session every endpoint is an `SniEndPoint`, which does the same — and there the append
  is skipped altogether unless the live-node plan came back empty, because the topology monitor
  re-resolves node addresses itself. Turning the option off in those deployments removes only that
  empty-plan fallback.

  A client-routes session is not a third exception. Its endpoints re-expand a route hostname per
  attempt only while that node has a route; a node without one falls back to a static,
  already-resolved address that is never re-resolved. The driver therefore reports such a session as
  re-resolving its own addresses only while *every* known node has a live route. In a partially
  routed cluster the contact points are appended as usual, and this option is the only DNS
  re-resolution the route-less nodes get.

Resolution goes through Netty's configured `AddressResolverGroup`, so a resolver installed via
`NettyOptions.afterBootstrapInitialized()` is honoured — including `DnsAddressResolverGroup` for
non-blocking lookups. With Netty's default resolver the lookup blocks the Netty I/O event loop it
runs on (never the admin loop), which is the same behaviour an unresolved address had before, so the
JVM DNS cache settings (`networkaddress.cache.ttl`) matter more than they used to.

Custom `EndPoint` implementations take part in this: return an
[unresolved](https://docs.oracle.com/javase/8/docs/api/java/net/InetSocketAddress.html#isUnresolved--)
`InetSocketAddress` from `resolve()` and the driver expands it. Implementations must **not** resolve
names themselves and must not block — see the `EndPoint.resolve()` javadoc.


### Cassandra-side configuration

The address that each Cassandra node shares with clients is the **broadcast RPC address**; it is
controlled by various properties in [cassandra.yaml]:

* [rpc_address] or [rpc_interface] is the address that the Cassandra process *binds to*. You must
  set one or the other, not both (for more details, see the inline comments in the default
  `cassandra.yaml` that came with your installation);
* [broadcast_rpc_address] \(introduced in Cassandra 2.1) is the address to share with clients, if it
  is different than the previous one (the reason for having a separate property is if the bind
  address is not public to clients, because there is a router in between).

If `broadcast_rpc_address` is not set, it defaults to `rpc_address`/`rpc_interface`. If
`rpc_address`/`rpc_interface` is 0.0.0.0 (all interfaces), then `broadcast_rpc_address` *must* be
set.

If you're not sure which address a Cassandra node is broadcasting, launch cqlsh locally on the node,
execute the following query and take node of the result:

```
cqlsh> select broadcast_address from system.local;

 broadcast_address
-------------------
         172.1.2.3
```

Then connect to *another* node in the cluster and run the following query, injecting the previous
result:

```
cqlsh> select rpc_address from system.peers where peer = '172.1.2.3';

 rpc_address
-------------
     1.2.3.4
```

That last result is the broadcast RPC address. Ensure that it is accessible from the client machine
where the driver will run.


### Driver-side address translation

Sometimes it's not possible for Cassandra nodes to broadcast addresses that will work for each and
every client; for instance, they might broadcast private IPs because most clients are in the same
network, but a particular client could be on another network and go through a router.

For such cases, you can register a driver-side component that will perform additional address
translation. Write a class that implements [AddressTranslator] with the following constructor:

```java
public class MyAddressTranslator implements AddressTranslator {

  public PassThroughAddressTranslator(DriverContext context, DriverOption configRoot) {
    // retrieve any required dependency or extra configuration option, otherwise can stay empty
  }

  @Override
  public InetSocketAddress translate(InetSocketAddress address) {
    // your custom translation logic
  }

  @Override
  public void close() {
    // free any resources if needed, otherwise can stay empty
  }
}
```

Then reference this class from the [configuration](../configuration/):

```
datastax-java-driver.advanced.address-translator.class = com.mycompany.MyAddressTranslator
```

Note: the contact points provided while creating the `CqlSession` are not translated, only addresses
retrieved from or sent by Cassandra nodes are.

### Client Routes (cloud private endpoint deployments)

For cloud deployments using private endpoint services (such as AWS PrivateLink, Azure Private Link,
or GCP Private Service Connect) or similar technologies (e.g., ScyllaDB Cloud), nodes are accessed
through private DNS endpoints rather than direct IP addresses. The driver
provides a built-in client routes feature that handles address translation automatically.

Client routes can be configured either **programmatically** or via **HOCON configuration files**.
Note that `OptionsMap`-based configuration does not support client routes — use the programmatic
API (`SessionBuilder.withClientRoutesConfig()`) instead, which can be combined with `OptionsMap`
for all other driver options.

Client routes are **mutually exclusive** with:
- A custom `AddressTranslator` (if both are provided, an `IllegalStateException` is thrown)
- Cloud secure connect bundles (if both are provided, an `IllegalStateException` is thrown)

#### Quick start (programmatic)

```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import java.net.InetSocketAddress;

ClientRoutesConfig config = ClientRoutesConfig.builder()
    .addEndpoint(new ClientRouteProxy(
        "12345678-1234-1234-1234-123456789012",
        "my-cluster-endpoint.example.com"))
    .build();

CqlSession session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("my-cluster-endpoint.example.com", 9042))
    .withClientRoutesConfig(config)
    .withLocalDatacenter("datacenter1")
    .build();
```

#### Quick start (HOCON configuration file)

```
datastax-java-driver {
  advanced.client-routes {
    endpoints = [
      { connection-id = "12345678-1234-1234-1234-123456789012",
        connection-addr = "my-cluster-endpoint.example.com" }
    ]
  }
}
```

#### How it works

1. **Startup** — after the control connection is established, the driver queries
   `system.client_routes` (filtered to the configured `connection_id` values) and builds an
   in-memory map of `host_id → (hostname, port, tls_port)`.
2. **Translation** — every time the driver opens a connection to a peer node, it looks up the
   node's `host_id` in the route map and resolves the associated DNS hostname. Contact points bypass
   translation so the initial seed addresses are used as-is.
3. **Event-driven updates** — the driver registers for `CLIENT_ROUTES_CHANGE` server events. When
   one arrives, it re-queries the table and atomically swaps the route map.
4. **Reconnect** — if the control connection is recreated the driver performs a full re-read of the
   route table before refreshing node metadata.

#### DNS resolution

DNS is resolved at connection time (not at route discovery time), and through the same mechanism as
every other address the driver connects to: the route's hostname is handed to the connection layer
unresolved, and Netty's configured `AddressResolverGroup` expands it. A custom resolver installed via
`NettyOptions.afterBootstrapInitialized()` therefore applies to client routes as well, and a hostname
that maps to several addresses has all of them tried in turn.

With Netty's default (JDK) resolver the lookup is a blocking `InetAddress` call that uses the JVM's
built-in DNS cache (30 s default TTL in the JDK). It runs on a Netty I/O event loop — never on the
admin event loop that drives control-connection reconnects — so it delays the connection attempt
itself. It is therefore worth configuring the JVM DNS cache TTL via the `networkaddress.cache.ttl`
security property (e.g. in `$JAVA_HOME/conf/security/java.security` or programmatically with
`java.security.Security.setProperty("networkaddress.cache.ttl", "60")`), or installing
`DnsAddressResolverGroup` for non-blocking resolution.

- **Route-map refresh** — the driver re-queries `system.client_routes` and atomically swaps the
  in-memory route map in two situations:
  - a `CLIENT_ROUTES_CHANGE` server event is received, or
  - the control connection reconnects after a failure.

  A route-map refresh does **not** flush the DNS cache. New hostnames are resolved on first use.

#### Limitations

- Requires ScyllaDB Enterprise ≥ 2026.1 with `system.client_routes` support
  (scylladb/scylladb#27323). Not yet available on ScyllaDB OSS.
- Not supported on Apache Cassandra.
- Mutually exclusive with custom `AddressTranslator` and with cloud secure connect bundles.
### Fixed proxy hostname

If your client applications access Cassandra through some kind of proxy (eg. with AWS PrivateLink when all Cassandra
nodes are exposed via one hostname pointing to AWS Endpoint), you can configure driver with
`FixedHostNameAddressTranslator` to always translate all node addresses to that same proxy hostname, no matter what IP
address a node has but still using its native transport port.

To use it, specify the following in the [configuration](../configuration):

```
datastax-java-driver.advanced.address-translator.class = FixedHostNameAddressTranslator
advertised-hostname = proxyhostname
```

The advertised hostname is handed on unresolved, so it is expanded on every connection attempt like
any other name (see [Multi-address resolution](#multi-address-resolution)): if the proxy is fronted by several A-records,
all of them are tried, and a change to the records is picked up without restarting the session. The
addresses are tried in the order the resolver returned them rather than shuffled, because a fixed
proxy hostname says nothing about whether its addresses lead to the same place.

### Fixed proxy hostname per subnet

When running Cassandra in a private network and accessing it from outside of that private network via some kind of
proxy, we have an option to use `FixedHostNameAddressTranslator`. But for multi-datacenter Cassandra deployments, we
want to have more control over routing queries to a specific datacenter (eg. for optimizing latencies), which requires
setting up a separate proxy per datacenter.

Normally, each Cassandra datacenter nodes are deployed to a different subnet to support internode communications in the
cluster and avoid IP address collisions. So when Cassandra broadcasts its nodes IP addresses, we can determine which
datacenter that node belongs to by checking its IP address against the given datacenter subnet.

For such scenarios you can use `SubnetAddressTranslator` to translate node IPs to the datacenter proxy address
associated with it. 

To use it, specify the following in the [configuration](../configuration):
```
datastax-java-driver.advanced.address-translator {
  class = SubnetAddressTranslator
  subnet-addresses {
    "100.64.0.0/15" = "cassandra.datacenter1.com:9042"
    "100.66.0.0/15" = "cassandra.datacenter2.com:9042"
    # IPv6 example:
    # "::ffff:6440:0/111" = "cassandra.datacenter1.com:9042"
    # "::ffff:6442:0/111" = "cassandra.datacenter2.com:9042"
  }
  # Optional. When configured, addresses not matching the configured subnets are translated to this address.
  default-address = "cassandra.datacenter1.com:9042"
  # Whether to resolve the addresses once on initialization (if true) or on each node (re-)connection (if false).
  # If not configured, defaults to false.
  resolve-addresses = false
}
```

Such setup is common for running Cassandra on Kubernetes with [k8ssandra](https://docs.k8ssandra.io/).

### EC2 multi-region

If you deploy both Cassandra and client applications on Amazon EC2, and your cluster spans multiple regions, you'll have
to configure your Cassandra nodes to broadcast public RPC addresses.

However, this is not always the most cost-effective: if a client and a node are in the same region, it would be cheaper
to connect over the private IP. Ideally, you'd want to pick the best address in each case.

The driver provides `Ec2MultiRegionAddressTranslator` which does exactly that.  To use it, specify the following in
the [configuration](../configuration/):

```
datastax-java-driver.advanced.address-translator.class = Ec2MultiRegionAddressTranslator
```

With this configuration, you keep broadcasting public RPC addresses. But each time the driver connects to a new
Cassandra node:

* if the node is *in the same EC2 region*, the public IP will be translated to the intra-region private IP;
* otherwise, it will not be translated.

(To achieve this, `Ec2MultiRegionAddressTranslator` performs a reverse DNS lookup of the origin address, to find the
domain name of the target instance. Then it performs a forward DNS lookup of the domain name; the EC2 DNS does the
private/public switch automatically based on location).

[AddressTranslator]: https://docs.datastax.com/en/drivers/java/4.17/com/datastax/oss/driver/api/core/addresstranslation/AddressTranslator.html

[cassandra.yaml]:        https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html
[rpc_address]:           https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__rpc_address
[rpc_interface]:         https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__rpc_interface
[broadcast_rpc_address]: https://docs.datastax.com/en/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html?scroll=configCassandra_yaml__broadcast_rpc_address
