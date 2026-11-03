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

# Connectivity

## Quick overview

How the driver reaches the nodes of a cluster, and what each kind of network needs configured.

* contact points and a local datacenter, and nothing else, when every node is reachable at the
  address it broadcasts – VPC peering, AWS Transit Gateway, or a direct connection over the
  public internet.
* `advanced.client-routes` when the cluster sits behind a cloud private endpoint that publishes a
  per-node endpoint mapping.
* `advanced.address-translator` for anything else, see
  [address resolution](../address_resolution/#driver-side-address-translation).

---

A `CqlSession` starts from the contact points you give it, discovers the rest of the cluster from
`system.peers`, and then opens connections to each node at the address that node broadcasts. Whether
that last step works is a property of your network, not of the driver, and it is what the pages in
this section are about.

<table border="1" class="docutils">
<thead>
<tr>
<th>How your application reaches the cluster</th>
<th>What the driver needs</th>
</tr>
</thead>
<tbody>
<tr>
<td><a href="vpc_peering/">VPC peering, Transit Gateway or a direct connection</a> -- every node is reachable at the address it broadcasts</td>
<td>contact points and a local datacenter, nothing else</td>
</tr>
<tr>
<td><a href="client_routes/">A cloud private endpoint that publishes a per-node entry in <code>system.client_routes</code></a> -- AWS PrivateLink, Azure Private Link, GCP Private Service Connect</td>
<td><code>advanced.client-routes</code></td>
</tr>
<tr>
<td>A proxy in front of the cluster -- one hostname for every node (a single-endpoint PrivateLink included), or EC2 multi-region</td>
<td>an <a href="../address_resolution/#driver-side-address-translation">address translator</a></td>
</tr>
</tbody>
</table>

A cloud private endpoint appears in two of those rows, and the difference is what the cluster
publishes: a per-node endpoint mapping the driver can read means client routes, while one hostname
standing in front of every node means an address translator. The translator has to give each node
its own address and port: nodes translated to the same one share a single endpoint, so the proxy
rather than the driver picks the node, and token-aware and shard-aware routing stop working.
`FixedHostNameAddressTranslator` keeps each node’s port, which on ScyllaDB is the same for every
node, so it does not meet this on its own.

Two things are the same in every case and are covered elsewhere:
[SSL](../ssl/) for encrypting the traffic, and [authentication](../authentication/) for proving who
you are. Contact points themselves are covered in the [core manual](../#contact-points).
