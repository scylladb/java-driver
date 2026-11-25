# Session and Configuration

How to rewrite 3.x `Cluster` and `Session` setup for 4.x. In 4.x one `CqlSession` replaces both, and
most options move from builder calls to `application.conf`. See
[Session](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#session) and
[Configuration](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#configuration)
in the migration guide.

## Basic Setup

### 3.x

```java
Cluster cluster = Cluster.builder()
    .addContactPoints("10.0.0.1", "10.0.0.2")
    .withPort(9042)
    .withCredentials("app_user", System.getenv("DB_PASSWORD"))
    .withLoadBalancingPolicy(new TokenAwarePolicy(
        DCAwareRoundRobinPolicy.builder().withLocalDc("dc1").build()))
    .build();
Session session = cluster.connect("my_keyspace");
// ...
cluster.close();
```

### 4.x

```java
try (CqlSession session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("10.0.0.1", 9042))
    .addContactPoint(new InetSocketAddress("10.0.0.2", 9042))
    .withLocalDatacenter("dc1")
    .withAuthCredentials("app_user", System.getenv("DB_PASSWORD"))
    .withKeyspace("my_keyspace")
    .build()) {
  // ...
}
```

- Always set the local datacenter. Port the value the 3.x code set; if it set none, ask the user
  which datacenter is local rather than leaving it to inference. Only 4.19.0.9 and 4.19.2.0 or later
  infer it; other 4.x versions throw from `build()` without it.
- The default policy is already token-aware, and on ScyllaDB also shard-aware and tablet-aware.
  Delete the `TokenAwarePolicy` and `DCAwareRoundRobinPolicy` wrapping; do not port it as a custom
  policy.
- `build()` connects right away. There is no separate `init()` or `connect()`.
- Create one `CqlSession` per cluster and share it, as with `Cluster` in 3.x. An application that
  talks to two clusters keeps two sessions. 3.x code that called `cluster.connect("ks")` for several
  keyspaces either keeps one session per keyspace or qualifies the table names (`ks.table`) and
  shares one session. Close each session on shutdown. During the migration it runs next to the 3.x
  `Cluster` until step 8 in SKILL.md removes 3.x.

## Builder Calls Mapped to 4.x

The table configures the new `CqlSession`; leave the 3.x `Cluster` builder as it is until step 8.
Options without a builder method go in `application.conf` on the classpath, under
`datastax-java-driver { ... }`. Put them there rather than in code, so they can be changed without a
rebuild.

| 3.x | 4.x |
| --- | --- |
| `addContactPoint(s)("host")` + `withPort(p)` | `basic.contact-points = ["host:p"]` or `addContactPoint(InetSocketAddress.createUnresolved("host", p))`. 3.x tried every address of a hostname; since 4.19.2.2 these forms try up to `advanced.connection.max-candidate-addresses` (5), while a resolved `new InetSocketAddress("host", p)` is one address. IP literals work in either form. |
| `cluster.connect("ks")` | `withKeyspace("ks")` |
| `DCAwareRoundRobinPolicy...withLocalDc("dc1")` | `withLocalDatacenter("dc1")` or `basic.load-balancing-policy.local-datacenter` |
| `RackAwareRoundRobinPolicy...withLocalRack("r1")` | `basic.load-balancing-policy.local-rack = "r1"` (no builder method) |
| `withUsedHostsPerRemoteDc(n)` | `advanced.load-balancing-policy.dc-failover.max-nodes-per-remote-dc = n` |
| `allowRemoteDCsForLocalConsistencyLevel()` | `advanced.load-balancing-policy.dc-failover.allow-for-local-consistency-levels = true` |
| `LatencyAwarePolicy` | Delete it. `basic.load-balancing-policy.slow-replica-avoidance` (default `true`) replaces it. |
| `withCredentials(u, p)` | `withAuthCredentials(u, p)`, or `advanced.auth-provider` with `class = PlainTextAuthProvider`, `username = u` and `password = p`. Take the password from the secret the 3.x code read it from, such as `password = ${DB_PASSWORD}` for an environment variable; never hard-code it in code or `application.conf`. A missing password now fails: `withAuthCredentials` throws `NullPointerException: password cannot be null` at once (`IllegalArgumentException` for an empty one), and `application.conf` fails `build()` with `UnresolvedSubstitution` when the variable is unset, where 3.x still connected to a node without authentication; if the secret is unset in some environments, configure credentials only where it is set. |
| `withSSL()` | `advanced.ssl-engine-factory.class = DefaultSslEngineFactory`. It validates hostnames, which 3.x `withSSL()` did not, so a node whose certificate does not match the address the driver connects to now fails the handshake with `No subject alternative DNS name matching ...`. If that happens, tell the user and recommend fixing the node certificates. Set `hostname-validation = false`, or use `withSslContext(...)`, which also skips the check, only for clearly labeled local testing and when the user explicitly asks; never in production. |
| `withQueryOptions(...setConsistencyLevel(cl))` | `basic.request.consistency` |
| `...setSerialConsistencyLevel(cl)` | `basic.request.serial-consistency` |
| `...setFetchSize(n)` | `basic.request.page-size` |
| `...setDefaultIdempotence(b)` | `basic.request.default-idempotence` |
| `...setLoadBalancingLwtRequestRoutingMethod(m)` | `advanced.load-balancing-policy.default-lwt-request-routing-method`, same values: `PRESERVE_REPLICA_ORDER` (the default in both) or `REGULAR` |
| `withSocketOptions(...setConnectTimeoutMillis(ms))` | `advanced.connection.connect-timeout` |
| `withSocketOptions(...setReadTimeoutMillis(ms))` | `basic.request.timeout`. It now covers the whole request, retries included; see SKILL.md. |
| `withPoolingOptions(...)` | `advanced.connection.pool.local.size`, `advanced.connection.max-requests-per-connection` (see below) |
| `withRetryPolicy(DefaultRetryPolicy.INSTANCE)` | Delete it; it is the default. |
| `withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)` | `advanced.retry-policy.class = ConsistencyDowngradingRetryPolicy` |
| `withReconnectionPolicy(new ExponentialReconnectionPolicy(b, m))` | `advanced.reconnection-policy.base-delay`, `max-delay` |
| `withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(d, n))` | `advanced.speculative-execution-policy` with `class = ConstantSpeculativeExecutionPolicy`, `delay = d`, `max-executions = n + 1` |
| `withCompression(LZ4)` | `advanced.protocol.compression = lz4`, and add `at.yawk.lz4:lz4-java` at the version in the driver's parent POM (1.12.1 for 4.19.4.0) |
| `withAddressTranslator(t)` | `withAddressTranslator(t)` is gone; use `advanced.address-translator.class` |
| `withoutAdvancedShardAwareness()` | `advanced.connection.advanced-shard-awareness.enabled = false` |
| `withLocalPortRange(low, high)` | `advanced.connection.advanced-shard-awareness.port-low`, `port-high` |
| `withClusterName(name)` | `basic.session-name` |
| `withProtocolVersion(v)` | Delete it. 4.x negotiates the version, also in mixed clusters. |
| `withoutJMXReporting()`, `withoutMetrics()` | Nothing; 4.x metrics are off by default. Keep them on the 3.x `Cluster` until step 8 deletes it; see [Both Drivers Side by Side](coordinates-and-build.md#both-drivers-side-by-side). |
| `cluster.register(...)` with a `Host.StateListener` or `SchemaChangeListener` | `addNodeStateListener(...)`, `addSchemaChangeListener(...)` on the builder, once per listener. `withNodeStateListener` and `withSchemaChangeListener` replace any listener set before, so with several listeners only the last one would fire. |

Speculative executions: in 3.x, `n` counts the extra executions. In 4.x, `max-executions` includes
the first one, so add 1.

## Example `application.conf`

```hocon
datastax-java-driver {
  basic.request {
    timeout = 12 seconds        # matches the 3.x read timeout; lower it for DML-only profiles
    consistency = LOCAL_QUORUM  # copy the value the 3.x code set; do not change it during migration
  }
  basic.load-balancing-policy {
    local-datacenter = dc1
    local-rack = rack1          # only if the 3.x code was rack-aware
  }
}
```

To keep options in code instead, pass a loader built with `DriverConfigLoader.programmaticBuilder()`
to `CqlSession.builder().withConfigLoader(...)`. Each `DefaultDriverOption` constant names one of
the keys above, for example `DefaultDriverOption.LOAD_BALANCING_LOCAL_RACK`.

## Connection Pool

On ScyllaDB the 4.x driver opens connections per shard. `advanced.connection.pool.local.size` is a
total per node, and the driver rounds it up so that every shard gets at least one connection. The
default, `1`, therefore means one connection per shard. Do not port 3.x `PoolingOptions` values.
Keep the defaults unless the user has measured a need, and see `scylladb-cloud-setup` for pool
sizing. 3.x queued a request when every connection was busy (`setMaxQueueSize`, default 256, for up
to `setPoolTimeoutMillis`, default 5 s); 4.x has no queue, tries the next node, then fails with
`AllNodesFailedException`. If the application relied on that queue, tell the user. The closest 4.x
option is `advanced.throttler.class = ConcurrencyLimitingRequestThrottler` with
`max-concurrent-requests` and `max-queue-size`, but it limits the whole session rather than each
node, counts queued time against `basic.request.timeout`, and fails with
`RequestThrottlingException` when its queue is full.

## Metadata

`session.getMetadata()` returns an immutable snapshot. Code that kept a 3.x `Metadata` reference and
expected it to update must call `getMetadata()` again. See
[Metadata](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#metadata).
