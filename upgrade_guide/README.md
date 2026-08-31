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

## Upgrade guide

### 4.19.2.1

#### The driver reports a session identifier, and its configuration, at connection time

Two CQL `STARTUP` options are new. The server stores them in its client-connection system table
(`system.clients` on ScyllaDB, `system_views.clients` on Cassandra 4.1+), so that operators can group
a client's connections and inspect its driver settings while investigating an incident.

* `SESSION_ID` — a driver-generated identifier, shared by all of a session's connections. It is sent
  on **every** connection, unconditionally: it is an innate behavior with no configuration option to
  turn it off. It is not derived from `CLIENT_ID`, which remains user-settable and unchanged.
* `DRIVER_CONFIG` — a compact JSON description of the effective configuration of the session's
  default execution profile (connection/socket settings, timeouts,
  retry/reconnection/speculative-execution/load-balancing policies, connection pooling, query
  defaults, and TLS). Only the control connection sends it, since it describes the whole session.
  It reports settings only — never credentials, statements or data — and identifies non-built-in
  policies by class name: the simple name, or the fully-qualified name when the policy is an
  anonymous class (which has no simple name). Reporting it is best-effort: if the report cannot be
  built, or would exceed 32 KiB, it is skipped (with a warning) rather than allowed to interfere
  with connecting. It is serialized with Jackson, which the driver
  [allows you to exclude](../manual/core/integration/#jackson); on such a classpath the report is
  skipped and an informational message is logged at startup. `SESSION_ID` is unaffected.

Reporting the configuration is **enabled by default**. To turn it off:

```properties
datastax-java-driver.advanced.driver-config-reporting.enabled = false
```

Note that this option does not affect `SESSION_ID`.

#### Two `BasicLoadBalancingPolicy` accessors widened from `protected` to `public`

`BasicLoadBalancingPolicy.getLocalDatacenter()` and `getLocalRack()` are now `public`, so that the
configuration report above can describe the datacenter and rack the policy actually resolved rather
than whatever the profile currently says.

Binary compatibility is unaffected — an already-compiled subclass keeps working. But if you
[extend `BasicLoadBalancingPolicy`](../manual/core/load_balancing/#custom-implementation) and
override either method, you have to widen your override to `public` in order to recompile: Java does
not allow an override to reduce visibility.

### 4.19.0.7

#### Cloud private-endpoint support via client routes

The driver now supports automatic address translation for cloud private-endpoint deployments
(e.g. AWS PrivateLink, Azure Private Link, GCP Private Service Connect)
through the new client routes feature. When enabled, the driver reads endpoint mappings from the
`system.client_routes` system table and translates peer addresses transparently at connection time,
with automatic refresh on `CLIENT_ROUTES_CHANGE` events.

Configure it programmatically on the session builder:

```java
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.ClientRoutesConfig;
import com.datastax.oss.driver.api.core.config.ClientRouteProxy;
import java.net.InetSocketAddress;

ClientRoutesConfig config = ClientRoutesConfig.builder()
    .addEndpoint(new ClientRouteProxy(
        "<connection-id>",
        "my-cluster.region.provider.scylladb.com"))
    .build();

CqlSession session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("my-cluster.region.provider.scylladb.com", 9042))
    .withClientRoutesConfig(config)
    .withLocalDatacenter("datacenter1")
    .build();
```

Or via HOCON configuration file:

```
datastax-java-driver {
  advanced.client-routes {
    endpoints = [
      { connection-id = "<connection-id>",
        connection-addr = "my-cluster.region.provider.scylladb.com" }
    ]
  }
}
```

Key points:

- **Mutually exclusive** with a custom `AddressTranslator` and with cloud secure connect bundles —
  providing both throws `IllegalStateException` at session build time.
- **Requires ScyllaDB Enterprise ≥ 2026.1** (scylladb/scylladb#27323). The feature is not
  available on ScyllaDB OSS or Apache Cassandra.

See [Address resolution — Client Routes](../manual/core/address_resolution/) for full details.

### 4.18.1

#### Keystore reloading in DefaultSslEngineFactory

`DefaultSslEngineFactory` now includes an optional keystore reloading interval, for detecting changes in the local
client keystore file. This is relevant in environments with mTLS enabled and short-lived client certificates, especially
when an application restart might not always happen between a new keystore becoming available and the previous
keystore certificate expiring.

This feature is disabled by default for compatibility. To enable, see `keystore-reload-interval` in `reference.conf`.

### 4.17.0

#### Support for Java17

With the completion of [JAVA-3042](https://datastax-oss.atlassian.net/browse/JAVA-3042) the driver now passes our automated test matrix for Java Driver releases.
If you discover an issue with the Java Driver running on Java 17, please let us know. We will triage and address Java 17 issues.

#### Updated API for vector search

The 4.16.0 release introduced support for the CQL `vector` datatype. This release modifies the `CqlVector`
value type used to represent a CQL vector to make it easier to use.  `CqlVector` now implements the Iterable interface
as well as several methods modelled on the JDK's List interface. For more, see
[JAVA-3060](https://datastax-oss.atlassian.net/browse/JAVA-3060). 

The builder interface was replaced with factory methods that resemble similar methods on `CqlDuration`.
For example, the following code will create a keyspace and table, populate that table with some data, and then execute
a query that will return a `vector` type.  This data is retrieved directly via `Row.getVector()` and the resulting
`CqlVector` value object can be interrogated directly.

```java
try (CqlSession session = new CqlSessionBuilder().withLocalDatacenter("datacenter1").build()) {

    session.execute("DROP KEYSPACE IF EXISTS test");
    session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute("CREATE TABLE test.foo(i int primary key, j vector<float, 3>)");
    session.execute("CREATE CUSTOM INDEX ann_index ON test.foo(j) USING 'StorageAttachedIndex'");
    session.execute("INSERT INTO test.foo (i, j) VALUES (1, [8, 2.3, 58])");
    session.execute("INSERT INTO test.foo (i, j) VALUES (2, [1.2, 3.4, 5.6])");
    session.execute("INSERT INTO test.foo (i, j) VALUES (5, [23, 18, 3.9])");
    ResultSet rs=session.execute("SELECT j FROM test.foo WHERE j ann of [3.4, 7.8, 9.1] limit 1");
    for (Row row : rs){
        CqlVector<Float> v = row.getVector(0, Float.class);
        System.out.println(v);
        if (Iterables.size(v) != 3) {
            throw new RuntimeException("Expected vector with three dimensions");
        }
    }
}
```

You can also use the `CqlVector` type with prepared statements:

```java
PreparedStatement preparedInsert = session.prepare("INSERT INTO test.foo (i, j) VALUES (?,?)");
CqlVector<Float> vector = CqlVector.newInstance(1.4f, 2.5f, 3.6f);
session.execute(preparedInsert.bind(3, vector));
```

In some cases, it makes sense to access the vector directly as an array of some numerical type. This version
supports such use cases by providing a codec which translates a CQL vector to and from a primitive array. Only float arrays are supported. 
You can find more information about this codec in the manual documentation on [custom codecs](../manual/core/custom_codecs/)

### 4.15.0

#### CodecNotFoundException now extends DriverException

Before [JAVA-2995](https://datastax-oss.atlassian.net/browse/JAVA-2995), `CodecNotFoundException`
was extending `RuntimeException`. This is a discrepancy as all other exceptions extend
`DriverException`, which in turn extends `RuntimeException`.

This was causing integrators to do workarounds in order to react on all exceptions correctly.

The change introduced by JAVA-2995 shouldn't be a problem for most users. But if your code was using
a logic such as below, it won't compile anymore:

```java
try {
    doSomethingWithDriver();
} catch(DriverException e) {
} catch(CodecNotFoundException e) { 
}
```

You need to either reverse the catch order and catch `CodecNotFoundException` first:

```java
try {
    doSomethingWithDriver();
} catch(CodecNotFoundException e) { 
} catch(DriverException e) {
}
```

Or catch only `DriverException`:

```java
try {
    doSomethingWithDriver();
} catch(DriverException e) { 
}
```

### 4.14.0

#### AllNodesFailedException instead of NoNodeAvailableException in certain cases

[JAVA-2959](https://datastax-oss.atlassian.net/browse/JAVA-2959) changed the behavior for when a
request cannot be executed because all nodes tried were busy. Previously you would get back a
`NoNodeAvailableException` but you will now get back an `AllNodesFailedException` where the
`getAllErrors` map contains a `NodeUnavailableException` for that node.

### 4.13.0

#### Enhanced support for GraalVM native images 

[JAVA-2940](https://datastax-oss.atlassian.net/browse/JAVA-2940) introduced an enhanced support for
building GraalVM native images. 

If you were building a native image for your application, please verify your native image builder
configuration. Most of the extra configuration required until now is likely to not be necessary
anymore.

Refer to this [manual page](../manual/core/graalvm) for details.

#### Registration of multiple listeners and trackers

[JAVA-2951](https://datastax-oss.atlassian.net/browse/JAVA-2951) introduced the ability to register
more than one instance of the following interfaces:

* [RequestTracker](https://docs.datastax.com/en/drivers/java/4.12/com/datastax/oss/driver/api/core/tracker/RequestTracker.html)
* [NodeStateListener](https://docs.datastax.com/en/drivers/java/4.12/com/datastax/oss/driver/api/core/metadata/NodeStateListener.html)
* [SchemaChangeListener](https://docs.datastax.com/en/drivers/java/4.12/com/datastax/oss/driver/api/core/metadata/schema/SchemaChangeListener.html)

Multiple components can now be registered both programmatically and through the configuration. _If
both approaches are used, components will add up and will all be registered_ (whereas previously,
the programmatic approach would take precedence over the configuration one).

When using the programmatic approach to register multiple components, you should use the new
`SessionBuilder` methods `addRequestTracker`, `addNodeStateListener` and  `addSchemaChangeListener`:

```java
CqlSessionBuilder builder = CqlSession.builder();
builder
    .addRequestTracker(tracker1)
    .addRequestTracker(tracker2);
builder
    .addNodeStateListener(nodeStateListener1)
    .addNodeStateListener(nodeStateListener2);
builder
    .addSchemaChangeListener(schemaChangeListener1)
    .addSchemaChangeListener(schemaChangeListener2);
```

To support registration of multiple components through the configuration, the following
configuration options were deprecated because they only allow one component to be declared:

* `advanced.request-tracker.class`
* `advanced.node-state-listener.class`
* `advanced.schema-change-listener.class`

They are still honored, but the driver will log a warning if they are used. They should now be
replaced with the following ones, that accept a list of classes to instantiate, instead of just
one:

* `advanced.request-tracker.classes`
* `advanced.node-state-listener.classes`
* `advanced.schema-change-listener.classes`

Example:

```
datastax-java-driver {
  advanced {
    # RequestLogger is a driver built-in tracker
    request-tracker.classes = [RequestLogger,com.example.app.MyRequestTracker]
    node-state-listener.classes = [com.example.app.MyNodeStateListener1,com.example.app.MyNodeStateListener2]
    schema-change-listener.classes = [com.example.app.MySchemaChangeListener]
  }
}
```

When more than one component of the same type is registered, the driver will distribute received
signals to all components in sequence, by order of their registration, starting with the
programmatically-provided ones. If a component throws an error, the error is intercepted and logged.

### 4.12.0

#### MicroProfile Metrics upgraded to 3.0

The MicroProfile Metrics library has been upgraded from version 2.4 to 3.0. Since this upgrade
involves backwards-incompatible binary changes, users of this library and of the
`java-driver-metrics-microprofile` module are required to take the appropriate action:

* If your application is still using MicroProfile Metrics < 3.0, you can still upgrade the core
  driver to 4.12, but you now must keep `java-driver-metrics-microprofile` in version 4.11 or lower,
  as newer versions will not work.
    
* If your application is using MicroProfile Metrics >= 3.0, then you must upgrade to driver 4.12 or
  higher, as previous versions of `java-driver-metrics-microprofile` will not work.

#### Mapper `@GetEntity` and `@SetEntity` methods can now be lenient

Thanks to [JAVA-2935](https://datastax-oss.atlassian.net/browse/JAVA-2935), `@GetEntity` and
`@SetEntity` methods now have a new `lenient` attribute.

If the attribute is `false` (the default value), then the source row or the target statement must
contain a matching column for every property in the entity definition. If such a column is not
found, an error will be thrown. This corresponds to the mapper's current behavior prior to the
introduction of the new attribute.

If the new attribute is explicitly set to `true` however, the mapper will operate on a best-effort
basis and attempt to read or write all entity properties that have a matching column in the source
row or in the target statement, *leaving unmatched properties untouched*.

This new, lenient behavior allows to achieve the equivalent of driver 3.x 
[lenient mapping](https://docs.datastax.com/en/developer/java-driver/3.10/manual/object_mapper/using/#manual-mapping).

Read the manual pages on [@GetEntity](../manual/mapper/daos/getentity) methods and
[@SetEntity](../manual/mapper/daos/setentity) methods for more details and examples of lenient mode.

### 4.11.0

#### Native protocol V5 is now production-ready

Thanks to [JAVA-2704](https://datastax-oss.atlassian.net/browse/JAVA-2704), 4.11.0 is the first
version in the driver 4.x series to fully support Cassandra's native protocol version 5, which has
been promoted from beta to production-ready in the upcoming Cassandra 4.0 release.

Users should not experience any disruption. When connecting to Cassandra 4.0, V5 will be
transparently selected as the protocol version to use.

#### Customizable metric names, support for metric tags

[JAVA-2872](https://datastax-oss.atlassian.net/browse/JAVA-2872) introduced the ability to configure
how metric identifiers are generated. Metric names can now be configured, but most importantly,
metric tags are now supported. See the [metrics](../manual/core/metrics/) section of the online
manual, or the `advanced.metrics.id-generator` section in the
[reference.conf](../manual/core/configuration/reference/) file for details.

Users should not experience any disruption. However, those using metrics libraries that support tags
are encouraged to try out the new `TaggingMetricIdGenerator`, as it generates metric names and tags
that will look more familiar to users of libraries such as Micrometer or MicroProfile Metrics (and
look nicer when exported to Prometheus or Graphite).

#### New `NodeDistanceEvaluator` API

All driver built-in load-balancing policies now accept a new optional component called
[NodeDistanceEvaluator]. This component gets invoked each time a node is added to the cluster or
comes back up. If the evaluator returns a non-null distance for the node, that distance will be
used, otherwise the driver will use its built-in logic to assign a default distance to it.

[NodeDistanceEvaluator]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/loadbalancing/NodeDistanceEvaluator.html

This component replaces the old "node filter" component. As a consequence, all `withNodeFilter`
methods in `SessionBuilder` are now deprecated and should be replaced by the equivalent
`withNodeDistanceEvaluator` methods.

If you have an existing node filter implementation, it can be converted to a `NodeDistanceEvaluator`
very easily:

```java
Predicate<Node> nodeFilter = ...
NodeDistanceEvaluator nodeEvaluator = 
    (node, dc) -> nodeFilter.test(node) ? null : NodeDistance.IGNORED;
```

The above can also be achieved by an adapter class as shown below:

```java
public class NodeFilterToDistanceEvaluatorAdapter implements NodeDistanceEvaluator {

    private final Predicate<Node> nodeFilter;

    public NodeFilterToDistanceEvaluatorAdapter(@NonNull Predicate<Node> nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    @Nullable @Override
    public NodeDistance evaluateDistance(@NonNull Node node, @Nullable String localDc) {
        return nodeFilter.test(node) ? null : NodeDistance.IGNORED;
    }
}
```

Finally, the `datastax-java-driver.basic.load-balancing-policy.filter.class` configuration option
has been deprecated; it should be replaced with a node distance evaluator class defined by the
`datastax-java-driver.basic.load-balancing-policy.evaluator.class` option instead.

### 4.10.0

#### Cross-datacenter failover

[JAVA-2899](https://datastax-oss.atlassian.net/browse/JAVA-2899) re-introduced the ability to
perform cross-datacenter failover using the driver's built-in load balancing policies. See [Load
balancing](../manual/core/load_balancing/) in the manual for details.

Cross-datacenter failover is disabled by default, therefore existing applications should not
experience any disruption.

#### New `RetryVerdict` API

[JAVA-2900](https://datastax-oss.atlassian.net/browse/JAVA-2900) introduced [`RetryVerdict`], a new 
interface that allows custom retry policies to customize the request before it is retried.

For this reason, the following methods in the `RetryPolicy` interface were added; they all return
a `RetryVerdict` instance:

1. [`onReadTimeoutVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onReadTimeoutVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-boolean-int-)
2. [`onWriteTimeoutVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onWriteTimeoutVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-com.datastax.oss.driver.api.core.servererrors.WriteType-int-int-int-)
3. [`onUnavailableVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onUnavailableVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-int-)
4. [`onRequestAbortedVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onRequestAbortedVerdict-com.datastax.oss.driver.api.core.session.Request-java.lang.Throwable-int-)
5. [`onErrorResponseVerdict`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onErrorResponseVerdict-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.servererrors.CoordinatorException-int-)

The following methods were deprecated and will be removed in the next major version:

1. [`onReadTimeout`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onReadTimeout-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-boolean-int-)
2. [`onWriteTimeout`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onWriteTimeout-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-com.datastax.oss.driver.api.core.servererrors.WriteType-int-int-int-)
3. [`onUnavailable`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onUnavailable-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.ConsistencyLevel-int-int-int-)
4. [`onRequestAborted`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onRequestAborted-com.datastax.oss.driver.api.core.session.Request-java.lang.Throwable-int-)
5. [`onErrorResponse`](https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryPolicy.html#onErrorResponse-com.datastax.oss.driver.api.core.session.Request-com.datastax.oss.driver.api.core.servererrors.CoordinatorException-int-)

Driver 4.10.0 also re-introduced a retry policy whose behavior is equivalent to the
`DowngradingConsistencyRetryPolicy` from driver 3.x. See this
[FAQ entry](https://docs.datastax.com/en/developer/java-driver/4.11/faq/#where-is-downgrading-consistency-retry-policy)
for more information.

[`RetryVerdict`]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/retry/RetryVerdict.html

#### Enhancements to the `Uuids` utility class

[JAVA-2449](https://datastax-oss.atlassian.net/browse/JAVA-2449) modified the implementation of
[Uuids.random()]: this method does not delegate anymore to the JDK's `java.util.UUID.randomUUID()`
implementation, but instead re-implements random UUID generation using the non-cryptographic
random number generator `java.util.Random`.

For most users, non-cryptographic strength is enough and this change should translate into better 
performance when generating UUIDs for database insertion. However, in the unlikely case where your
application requires cryptographic strength for UUID generation, you should update your code to
use `java.util.UUID.randomUUID()` instead of `com.datastax.oss.driver.api.core.uuid.Uuids.random()` 
from now on.

This release also introduces two new methods for random UUID generation:

1. [Uuids.random(Random)]: similar to `Uuids.random()` but allows to pass a custom instance of 
   `java.util.Random` and/or re-use the same instance across calls.
2. [Uuids.random(SplittableRandom)]: similar to `Uuids.random()` but uses a 
   `java.util.SplittableRandom` instead.

[Uuids.random()]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/uuid/Uuids.html#random--
[Uuids.random(Random)]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/uuid/Uuids.html#random-java.util.Random-
[Uuids.random(SplittableRandom)]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/uuid/Uuids.html#random-java.util.SplittableRandom-

#### System and DSE keyspaces automatically excluded from metadata and token map computation

[JAVA-2871](https://datastax-oss.atlassian.net/browse/JAVA-2871) now allows for a more fine-grained
control over which keyspaces should qualify for metadata and token map computation, including the 
ability to *exclude* keyspaces based on their names.

From now on, the following keyspaces are automatically excluded:

1. The `system` keyspace;
2. All keyspaces starting with `system_`;
3. DSE-specific keyspaces: 
   1. All keyspaces starting with `dse_`;
   2. The `solr_admin` keyspace;
   3. The `OpsCenter` keyspace.
   
This means that they won't show up anymore in [Metadata.getKeyspaces()], and [TokenMap] will return
empty replicas and token ranges for them. If you need the driver to keep computing metadata and
token map for these keyspaces, you now must modify the following configuration option:
`datastax-java-driver.advanced.metadata.schema.refreshed-keyspaces`.

[Metadata.getKeyspaces()]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/metadata/Metadata.html#getKeyspaces--
[TokenMap]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/metadata/TokenMap.html

### 4.5.x - 4.6.0

These versions are subject to [JAVA-2676](https://datastax-oss.atlassian.net/browse/JAVA-2676), a
bug that causes performance degradations in certain scenarios. We strongly recommend upgrading to at
least 4.6.1.

### 4.4.0

DataStax Enterprise support is now available directly in the main driver. There is no longer a
separate DSE driver.

#### For Apache Cassandra® users

The great news is that [reactive execution](../manual/core/reactive/) is now available for everyone.
See the `CqlSession.executeReactive` methods.

Apart from that, the only visible change is that DSE-specific features are now exposed in the API: 

* new execution methods: `CqlSession.executeContinuously*`. They have default implementations so
  this doesn't break binary compatibility. You can just ignore them.
* new dependency: Reactive Streams. If you want to keep your classpath lean, you can exclude it when
  you don't use reactive execution; see the
  [Integration>Driver dependencies](../manual/core/integration/#driver-dependencies) section.

#### For DataStax Enterprise users

Adjust your Maven coordinates to use the unified artifact:

```xml
<!-- Replace: -->
<dependency>
  <groupId>com.datastax.dse</groupId>
  <artifactId>dse-java-driver-core</artifactId>
  <version>2.3.0</version>
</dependency>

<!-- By: -->
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.4.0</version>
</dependency>

<!-- Do the same for the other modules: query builder, mapper... -->
```

The new driver is a drop-in replacement for the DSE driver. Note however that we've deprecated a few
DSE-specific types in favor of their OSS equivalents. They still work, so you don't need to make the
changes right away; but you will get deprecation warnings:

* `DseSession`: use `CqlSession` instead, it can now do everything that a DSE session does. This
  also applies to the builder:
  
    ```java
    // Replace:
    DseSession session = DseSession.builder().build()  
  
    // By:
    CqlSession session = CqlSession.builder().build()
    ```
* `DseDriverConfigLoader`: the driver no longer needs DSE-specific config loaders. All the factory
  methods in this class now redirect to `DriverConfigLoader`. On that note, `dse-reference.conf`
  does not exist anymore, all the driver defaults are now in
  [reference.conf](../manual/core/configuration/reference/).
* plain-text authentication: there is now a single implementation that works with both Cassandra and
  DSE. If you used `DseProgrammaticPlainTextAuthProvider`, replace it by
  `PlainTextProgrammaticAuthProvider`. Similarly, if you wrote a custom implementation by
  subclassing `DsePlainTextAuthProviderBase`, extend `PlainTextAuthProviderBase` instead.
* `DseLoadBalancingPolicy`: DSE-specific features (the slow replica avoidance mechanism) have been
  merged into `DefaultLoadBalancingPolicy`. `DseLoadBalancingPolicy` still exists for backward
  compatibility, but it is now identical to the default policy.

#### Class Loader

The default class loader used by the driver when instantiating classes by reflection changed. 
Unless specified by the user, the driver will now use the same class loader that was used to load
the driver classes themselves, in order to ensure that implemented interfaces and implementing 
classes are fully compatible.

This should ensure a more streamlined experience for OSGi users, who do not need anymore to define
a specific class loader to use.

However if you are developing a web application and your setup corresponds to the following 
scenario, then you will now be required to explicitly define another class loader to use: if in your
application the driver jar is loaded by the web server's system class loader (for example, 
because the driver jar was placed in the "/lib" folder of the web server), then the default class
loader will be the server's system class loader. Then if the application tries to load, say, a 
custom load balancing policy declared in the web app's "WEB-INF/lib" folder, then the default class 
loader will not be able to locate that class. Instead, you must use the web app's class loader, that 
you can obtain in most web environments by calling `Thread.getContextClassLoader()`:
 
    CqlSession.builder()
        .addContactEndPoint(...)
        .withClassLoader(Thread.currentThread().getContextClassLoader())
        .build();
 
See the javadocs of [SessionBuilder.withClassLoader] for more information.

[SessionBuilder.withClassLoader]: https://docs.datastax.com/en/drivers/java/4.11/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withClassLoader-java.lang.ClassLoader-

### 4.1.0

#### Object mapper

4.1.0 marks the introduction of the new object mapper in the 4.x series.

Like driver 3, it relies on annotations to configure mapped entities and queries. However, there are
a few notable differences:

* it uses compile-time annotation processing instead of runtime reflection;
* the "mapper" and "accessor" concepts have been unified into a single "DAO" component, that handles
  both pre-defined CRUD patterns, and user-provided queries.

Refer to the [mapper manual](../manual/mapper/) for all the details.

#### Internal API

`NettyOptions#afterBootstrapInitialized` is now responsible for setting socket options on driver
connections (see `advanced.socket` in the configuration). If you had written a custom `NettyOptions`
for 4.0, you'll have to copy over -- and possibly adapt -- the contents of
`DefaultNettyOptions#afterBootstrapInitialized` (if you didn't override `NettyOptions`, you don't
have to change anything).

### 4.0.0

Version 4 is a major redesign of the internal architecture, and is **not binary compatible**
with previous versions. That upgrade has its own page: see
[Migrating from Java Driver 3.x](from_3x/).
