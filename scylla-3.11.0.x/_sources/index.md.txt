# Scylla Java Driver for Scylla and Apache Cassandra®

*If you're reading this on github.com, please note that this is the readme
for the development version and that some features described here might
not yet have been released. You can find the documentation for the latest
version through the [Java driver
docs](https://docs.scylladb.com/using-scylla/scylla-java-driver/) or via the release tags,
[e.g.
3.11.0.0](https://github.com/scylladb/java-driver/releases/tag/3.11.0.0).*

A modern, [feature-rich](manual/) and highly tunable Java client
library for Apache Cassandra (2.1+) and using exclusively Cassandra's binary protocol 
and Cassandra Query Language v3.

The Scylla Java Driver is a fork from [DataStax Java Driver](https://github.com/datastax/java-driver), including some non-breaking changes for Scylla optimization, with more updates planned.

**Features:**

* Like all Scylla Drivers, the Scylla Java Driver is **Shard Aware** and contains extensions for a `tokenAwareHostPolicy`. 
  Using this policy, the driver can select a connection to a particular shard based on the shard’s token. 
  As a result, latency is significantly reduced because there is no need to pass data between the shards.
* [Sync](manual/) and [Async](manual/async/) API
* [Simple](manual/statements/simple/), [Prepared](manual/statements/prepared/), and [Batch](manual/statements/batch/)
  statements
* Asynchronous IO, parallel execution, request pipelining
* [Connection pooling](manual/pooling/)
* Auto node discovery
* Automatic reconnection
* Configurable [load balancing](manual/load_balancing/) and [retry policies](manual/retries/)
* Works with any cluster size
* [Query builder](manual/statements/built/)
* [Object mapper](manual/object_mapper/)

The driver architecture is based on layers. At the bottom lies the driver core.
This core handles everything related to the connections to a Cassandra
cluster (for example, connection pool, discovering new nodes, etc.) and exposes a simple,
relatively low-level API on top of which higher level layers can be built.

The driver contains the following modules:

- driver-core: the core layer.
- driver-mapping: the object mapper.
- driver-extras: optional features for the Java driver.
- driver-examples: example applications using the other modules which are
  only meant for demonstration purposes.
- driver-tests: tests for the java-driver.

**Useful links:**

- SCYLLA UNIVERSITY JAVA [CLASS](https://university.scylladb.com/courses/using-scylla-drivers/lessons/coding-with-java-part-1/) 
- DOCS: the [manual](manual/) has quick
  start material and technical details about the driver and its features.
- GITHUB REPOSITORY: https://github.com/scylladb/java-driver
- [changelog](changelog/)

## Getting the driver

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency 

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>scylla-driver-core</artifactId>
  <version>3.11.0.0</version>
</dependency>
```

Note that the object mapper is published as a separate artifact:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>scylla-driver-mapping</artifactId>
  <version>3.11.0.0</version>
</dependency>
```

The 'extras' module is also published as a separate artifact:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>scylla-driver-extras</artifactId>
  <version>3.11.0.0</version>
</dependency>
```


We also provide a [shaded JAR](manual/shaded_jar/)
to avoid the explicit dependency to Netty.

## Compatibility

The Java client driver 3.11.0.0 ([branch 3.x](https://github.com/scylladb/java-driver/tree/3.x)) is compatible with Apache
Cassandra 2.1, 2.2 and 3.0+.

UDT and tuple support is available only when using Apache Cassandra 2.1 or higher.

Other features are available only when using Apache Cassandra 2.0 or higher (e.g. result set paging,
[lightweight transactions](https://docs.scylladb.com/using-scylla/lwt/))

The java driver supports Java JDK versions 6 and above.



__Disclaimer__: Some _Scylla_ products might partially work on 
big-endian systems, but _Scylla_ does not officially support these systems.

## Upgrading from previous versions

If you are upgrading from a previous version of the driver, be sure to have a look at
the [upgrade guide](/upgrade_guide/).



## License
&copy; DataStax, Inc. 
&copy; ScyllaDB, all rights reserved.

© 2016, The Apache Software Foundation.

Apache®, Apache Cassandra®, Cassandra®, the Apache feather logo and the Apache Cassandra® 
Eye logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States 
and/or other countries. 
No endorsement by The Apache Software Foundation is implied by the use of these marks.

