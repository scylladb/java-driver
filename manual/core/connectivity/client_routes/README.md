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

Reach a cluster that is only exposed through a cloud private endpoint: the cluster publishes a
per-node endpoint mapping, and the driver connects through the hostname it finds there instead of
through the address the node broadcasts.

* `advanced.client-routes` in the configuration, or `SessionBuilder.withClientRoutesConfig()`.
* disabled by default. Requires ScyllaDB Enterprise 2026.1 or later.
* mutually exclusive with `advanced.address-translator` and with cloud secure connect bundles.

-----

For cloud deployments using private endpoint services, nodes are accessed through private DNS
endpoints rather than direct IP addresses, and the driver's built-in client routes feature handles
the address translation automatically. It works regardless of which provider service fronts the
private endpoint — AWS PrivateLink (PL), Azure Private Link, or GCP Private Service Connect (PSC),
collectively a private service connection — and equally for ScyllaDB Cloud and similar technologies.
The feature is also written as `client_routes` or `clientroutes`.

Client routes can be configured either **programmatically** or via **HOCON configuration files**.
Note that `OptionsMap`-based configuration does not support client routes — use the programmatic
API (`SessionBuilder.withClientRoutesConfig()`) instead, which can be combined with `OptionsMap`
for all other driver options.

Neither example below configures TLS. A private endpoint that requires it needs an engine
factory as well -- see [SSL](../../ssl/); the driver does not enable TLS on its own.

### Quick start (programmatic)

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
    .addContactPoint(InetSocketAddress.createUnresolved("my-cluster-endpoint.example.com", 9042))
    .withClientRoutesConfig(config)
    .withLocalDatacenter("datacenter1")
    .build();
```

### Quick start (HOCON configuration file)

```
datastax-java-driver {
  basic.contact-points = [ "my-cluster-endpoint.example.com:9042" ]
  basic.load-balancing-policy.local-datacenter = "datacenter1"

  advanced.client-routes {
    endpoints = [
      { connection-id = "12345678-1234-1234-1234-123456789012",
        connection-addr = "my-cluster-endpoint.example.com" }
    ]
  }
}
```

The contact points are not optional here. Client routes do not translate them, and a session that
is given none falls back to `127.0.0.1:9042`.

The [reference configuration](../../configuration/reference/) documents every option under
`advanced.client-routes`.

### How it works

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

### DNS resolution

DNS is resolved at connection time (not at route discovery time). The driver delegates to
`InetAddress.getByName()`, which is a blocking call that uses the JVM's built-in DNS cache. How
long a successful lookup is cached is JVM-dependent -- commonly 30 s, but indefinitely when a
security manager is installed. The lookup blocks the thread opening the connection, usually one of
the driver's two admin threads, which also run pool management, the control connection and metadata
refreshes, so slow or unresponsive DNS stalls all of them. To mitigate this, configure the JVM DNS
cache TTL via the `networkaddress.cache.ttl` security property (e.g. in
`$JAVA_HOME/conf/security/java.security` or programmatically with
`java.security.Security.setProperty("networkaddress.cache.ttl", "60")`). The JDK reads this
property once, at its first DNS lookup, so set it in `java.security` or at the very start of `main`,
before anything resolves a name.

Refreshing the route map does **not** flush the DNS cache; new hostnames are resolved on first use.

### Limitations

- Requires ScyllaDB Enterprise ≥ 2026.1 with `system.client_routes` support
  (scylladb/scylladb#27323). Not yet available on ScyllaDB OSS.
- Not supported on Apache Cassandra.
- Mutually exclusive with a custom `AddressTranslator` and with cloud secure connect bundles:
  configuring either one alongside client routes throws an `IllegalStateException`.

A deployment that is *not* fronted by a per-node endpoint mapping -- one proxy hostname for the
whole cluster, or EC2 multi-region -- needs an address translator instead, and those are described
on the
[address resolution](../../address_resolution/#driver-side-address-translation) page.
