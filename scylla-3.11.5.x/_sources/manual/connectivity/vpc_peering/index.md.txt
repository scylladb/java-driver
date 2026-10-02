## VPC peering, Transit Gateway and direct connections

### Quick overview

The common case: every node is reachable at the address it broadcasts, so the driver needs no
address translation at all.

* contact points and a local datacenter are the whole of the connectivity configuration.
* covers VPC peering, AWS Transit Gateway and direct connections over the public internet.
* leave the address translator unset -- see
  [when the broadcast addresses are not reachable](#when-the-broadcast-addresses-are-not-reachable)
  if the nodes are *not* reachable at their broadcast addresses.

-----

ScyllaDB Cloud offers three ways to put your application on a network that can route to the cluster.
They differ in how the packets travel, not in what the driver does: in all three the node addresses
in `system.peers` are addresses your application can open a socket to, which is what the driver
assumes by default.

Setting any of them up is a cluster-side operation, documented under
[Network Access Options](https://cloud.docs.scylladb.com/stable/cluster-connections/connectivity-options.html)
in the ScyllaDB Cloud manual. This page covers only what it means for the driver.

### VPC peering

A private link between your VPC and the cluster's VPC, within one cloud provider. Traffic never
leaves the provider's network. Available on
[AWS](https://cloud.docs.scylladb.com/stable/cluster-connections/vpc-peering/aws-vpc-peering.html)
and
[GCP](https://cloud.docs.scylladb.com/stable/cluster-connections/vpc-peering/gcp-vpc-peering.html).

Two constraints are worth knowing before you plan an application around it: peering has to be
enabled when the cluster is created and cannot be added afterwards, and your VPC's CIDR must not
overlap the cluster's.

### AWS Transit Gateway

A [TGW VPC attachment](https://cloud.docs.scylladb.com/stable/cluster-connections/aws-tgw-vpc-attachment.html)
connects a cluster datacenter to a Transit Gateway in your AWS account, so several VPCs or accounts
reach one cluster through a single hub. Choose it over peering when more than one VPC needs the
cluster, or when your topology already has a Transit Gateway.

### Direct connection over the public internet

Clusters are reachable on the public internet by default, encrypted with TLS, with access limited to
the addresses on the cluster's allowlist. It needs no network setup, which makes it the right choice
for local development and for evaluating a trial cluster, and the wrong one for production traffic.

Two things matter more here than on a private network: configure [SSL](../../ssl/), and remember
that ScyllaDB Cloud hands you node *hostnames* rather than addresses. `addContactPoint(String)`
resolves a hostname once, when it is called, and adds every address it resolves to; a hostname that
later points somewhere else is not followed.

### Connecting

Give the cluster the contact points from your cluster's connect page, name the local datacenter, and
add credentials:

```java
Cluster cluster = Cluster.builder()
    .addContactPoints(
        "node-0.aws-eu-west-1.example.clusters.scylla.cloud",
        "node-1.aws-eu-west-1.example.clusters.scylla.cloud",
        "node-2.aws-eu-west-1.example.clusters.scylla.cloud")
    .withPort(9142)
    .withLoadBalancingPolicy(new TokenAwarePolicy(
        DCAwareRoundRobinPolicy.builder().withLocalDc("AWS_EU_WEST_1").build()))
    .withCredentials("scylla", "...")
    .withSSL()
    .build();
```

Port 9142 is the TLS port; 9042 stays open for unencrypted traffic unless the cluster enforces
encryption. The cluster certificate is signed by a per-cluster CA, not a public one: download it
from the cluster's details page ("Download CA public key"), import it into a truststore as in
[client truststore](../../ssl/#client-truststore), and start the JVM with
`-Djavax.net.ssl.trustStore=/path/to/client.truststore` and
`-Djavax.net.ssl.trustStorePassword=...`, which `withSSL()` picks up. `withSSL()` does *not* verify
that the certificate matches the host it connects to. [SSL](../../ssl/) shows how to turn hostname
verification on by overriding `newSSLEngine`, and covers client certificates with
`RemoteEndpointAwareJdkSSLOptions`.

The datacenter name has to match the cluster's exactly; it is shown on the cluster page. `build()`
does not contact the cluster, so it cannot catch a wrong name, and neither does `connect()`: it
logs `Some contact points don't match local data center`, treats every node as remote, returns a
`Session`, and every request on it then fails with `NoHostAvailableException`. The fix is the right
datacenter name. See [load balancing](../../load_balancing/) for what the local datacenter controls.

When the nodes advertise a shard-aware port -- 19042 by default, 19142 with SSL -- the driver opens
its connections there and picks each connection's local port to reach a given shard. If only the
regular port is open between the application and the cluster, or a NAT in between rewrites client
ports, call `withoutAdvancedShardAwareness()`.

### When the broadcast addresses are not reachable

If connections to the contact points succeed but every other node is unreachable, the cluster is
broadcasting addresses your network cannot route to. That is a different deployment shape:

* the cluster is behind a cloud private endpoint and publishes a per-node endpoint mapping -- AWS
  PrivateLink, Azure Private Link or GCP Private Service Connect: that is
  [client routes](../client_routes/), which 3.x does not support;
* everything else -- one proxy hostname for the whole cluster, or EC2 multi-region: use an
  [address translator](../../address_resolution/#driver-side-address-translation) that gives each
  node its own address and port.

Two queries separate the two. The first shows the addresses the driver is being handed, and needs
both halves because `system.peers` never lists the node you are connected to:

```
cqlsh> select peer, rpc_address from system.peers;
cqlsh> select broadcast_address, rpc_address from system.local;
```

`rpc_address` is the one the driver connects to; `peer` and `broadcast_address` are the addresses the
nodes use among themselves.

The second decides which answer applies -- a row per node means the cluster publishes the mapping
client routes reads, and a missing table means it does not:

```
cqlsh> select * from system.client_routes;
```
