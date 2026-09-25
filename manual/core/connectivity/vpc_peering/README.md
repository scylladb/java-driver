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

## VPC peering, Transit Gateway and direct connections

### Quick overview

The common case: every node is reachable at the address it broadcasts, so the driver needs no
address translation at all.

* contact points and `withLocalDatacenter` are the whole of the connectivity configuration.
* covers VPC peering, AWS Transit Gateway and direct connections over the public internet.
* leave `advanced.address-translator` unset -- see
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

Clusters are reachable on the public internet by default, with access limited to the addresses on
the cluster's allowlist. It needs no network setup, which makes it the right choice for local
development and for evaluating a trial cluster, and the wrong one for production traffic.

Two things matter more here than on a private network: configure [SSL](../../ssl/), and remember
that ScyllaDB Cloud hands you node *hostnames* rather than addresses. A hostname contact point is
looked up when the session first connects to it, and only the first address the lookup returns is
tried, so give the session every hostname from the connect page rather than just one.

### Connecting

There is nothing connectivity-specific to configure. Give the session the contact points from your
cluster's connect page, name the local datacenter, and add credentials:

```java
CqlSession session = CqlSession.builder()
    .addContactPoint(InetSocketAddress.createUnresolved(
        "node-0.aws-eu-west-1.example.clusters.scylla.cloud", 9142))
    .addContactPoint(InetSocketAddress.createUnresolved(
        "node-1.aws-eu-west-1.example.clusters.scylla.cloud", 9142))
    .addContactPoint(InetSocketAddress.createUnresolved(
        "node-2.aws-eu-west-1.example.clusters.scylla.cloud", 9142))
    .withLocalDatacenter("AWS_EU_WEST_1")
    .withAuthCredentials("scylla", "...")
    .build();
```

`createUnresolved` defers that lookup until the session connects;
`new InetSocketAddress(host, port)` does it on construction, so the contact point is fixed to
whatever that one lookup returned. Either way the name matters only at startup: the driver soon
reaches each node at the address the cluster reports for it, so a DNS change after startup is not
followed. Contact points given in the configuration are resolved once, when the session is built,
and every address a name returns becomes a contact point, unless `advanced.resolve-contact-points`
is `false` -- see the [reference configuration](../../configuration/reference/).

Port 9142 is the TLS port; 9042 stays open for unencrypted traffic unless the cluster enforces
encryption. The cluster certificate is signed by a per-cluster CA, not a public one: download it
from the cluster's details page ("Download CA public key") and import it into a truststore:

```
keytool -import -v -trustcacerts -alias CARoot -file scylladb_cluster_ca.pem -keystore client.truststore
```

Then point the engine factory at that truststore:

```
datastax-java-driver.advanced.ssl-engine-factory {
  class = DefaultSslEngineFactory
  truststore-path = /path/to/client.truststore
  truststore-password = password123
}
```

[SSL](../../ssl/) covers truststores, hostname validation and client certificates.

The datacenter name has to match the cluster's exactly; it is shown on the cluster page. Getting it
wrong does *not* fail the build: the driver logs `You specified ... as the local DC, but some contact
points are from a different DC`, carries on, and then has no node it is willing to route to, so
every request fails with `NoNodeAvailableException`. See [load balancing](../../load_balancing/) for
what the local datacenter controls.

### When the broadcast addresses are not reachable

If connections to the contact points succeed but every other node is unreachable, the cluster is
broadcasting addresses your network cannot route to. That is a different deployment shape:

* the cluster is behind a cloud private endpoint and publishes a per-node endpoint mapping -- AWS
  PrivateLink, Azure Private Link or GCP Private Service Connect: that needs
  [client routes](../client_routes/), which this version does not have;
* everything else -- one proxy hostname for the whole cluster, or EC2 multi-region: use an
  [address translator](../../address_resolution/#driver-side-address-translation) that gives each
  node its own address and port.

Two queries separate the two. The first shows the addresses the driver is being handed, and needs
both halves because `system.peers` never lists the node you are connected to:

```
cqlsh> select peer, rpc_address from system.peers;
cqlsh> select broadcast_address, rpc_address from system.local;
```

`rpc_address` is the one clients connect to; `peer` and `broadcast_address` are the addresses the
nodes use among themselves.

The second decides which answer applies -- a row per node means the cluster publishes the mapping
client routes reads, and a missing table means it does not:

```
cqlsh> select * from system.client_routes;
```
