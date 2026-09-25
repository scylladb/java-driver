## Client routes (private networking)

### Quick overview

Reach a cluster that is only exposed through a cloud private endpoint, where the cluster publishes a
per-node endpoint mapping in `system.client_routes`.

* **not supported in Java Driver 3.x, at any version, and it will not be added.**
* use Java Driver 4.x, 4.19.0.7 or later, with ScyllaDB Enterprise 2026.1 or later.
* a custom `AddressTranslator` is not an equivalent.

-----

Client routes are how the driver connects through a **private service connection**: AWS
**PrivateLink** (**PL**), Azure **Private Link**, or GCP **Private Service Connect** (**PSC**). Those
are the provider services in front of the cluster, not names of a driver feature. After the table it
reads, the feature itself is also written as **client_routes** or **clientroutes**.

When a ScyllaDB Cloud cluster is reached through a private endpoint service, its nodes broadcast
addresses that the client cannot route to. Java Driver 4.x solves this with client routes: it reads
per-node endpoint mappings from the `system.client_routes` table and opens each connection through
the private endpoint that serves that node. This works regardless of which provider service fronts
the private endpoint.

**Java Driver 3.x does not support client routes.** They were added in 4.x and first released in
4.19.0.7, and they need ScyllaDB Enterprise 2026.1 or later, which is where `system.client_routes`
appears. The 4.x manual covers them under [Client routes][4.x-client-routes].

### Why an address translator is not a substitute

Java Driver 3.x never reads `system.client_routes` and knows nothing of the `CLIENT_ROUTES_CHANGE`
event, so a custom [AddressTranslator] written here does not automatically receive route-change
events or refresh `system.client_routes`; keeping it in step with the cluster would be up to you.
If you need client routes, use [Java Driver 4.x][4.x-driver]; the 4.x
[upgrade guide from 3.x][4.x-from-3x] covers the move.

A deployment that is *not* fronted by a per-node endpoint mapping -- one proxy hostname for the whole
cluster, or EC2 multi-region -- does work with 3.x, through an address translator, provided each node
translates to its own address and port; see
[address resolution](../../address_resolution/#driver-side-address-translation).

[4.x-client-routes]: https://github.com/scylladb/java-driver/blob/scylla-4.x/manual/core/connectivity/client_routes/README.md
[4.x-driver]:        https://github.com/scylladb/java-driver/tree/scylla-4.x
[4.x-from-3x]:       https://github.com/scylladb/java-driver/tree/scylla-4.x/upgrade_guide/from_3x
[AddressTranslator]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/AddressTranslator.html
