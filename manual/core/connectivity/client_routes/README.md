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

## Client routes (private networking)

### Quick overview

Client routes are **not available in driver 4.18.x**. They were first released in 4.19.0.7.

* upgrade to 4.19.0.7 or later to connect through a cloud private endpoint that publishes per-node
  routes; the feature also needs ScyllaDB Enterprise 2026.1 or later.
* see the
  [4.x client routes documentation](https://github.com/scylladb/java-driver/blob/scylla-4.x/manual/core/connectivity/client_routes/README.md).

-----

Client routes are how driver 4.19.0.7 and later connects through a **private service connection**:
AWS **PrivateLink** (**PL**), Azure **Private Link**, or GCP **Private Service Connect** (**PSC**).
Those are the provider services in front of the cluster, not names of a driver feature. After the
table it reads, the feature itself is also written as `client_routes` or `clientroutes`.

With client routes, the cluster publishes a per-node endpoint mapping in `system.client_routes`,
and the driver connects to each node through the hostname it finds there instead of through the
address the node broadcasts. 4.18.x neither reads that table nor handles the `CLIENT_ROUTES_CHANGE`
events that keep the mapping current.

A custom [address translator](../../address_resolution/#driver-side-address-translation) is not a
substitute: it maps one address to another, while client routes key each node on its `host_id` and
follow the cluster as the mapping changes. An address translator remains the right tool for a proxy
in front of the cluster -- one hostname for every node, or EC2 multi-region -- provided each node
translates to its own address and port.
