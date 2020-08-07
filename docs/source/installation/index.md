## Installation

### Getting the driver

The last release of the driver is available on Maven Central. You can install
it in your application using the following Maven dependency (_if
using DataStax Enterprise, install the [DataStax Enterprise Java driver][dse-driver] instead_):

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>scylla-driver-core</artifactId>
  <version>3.7.1-scylla-0-SNAPSHOT</version>
</dependency>
```

Note that the object mapper is published as a separate artifact:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>scylla-driver-mapping</artifactId>
  <version>3.7.1-scylla-0-SNAPSHOT</version>
</dependency>
```

The 'extras' module is also published as a separate artifact:

```xml
<dependency>
  <groupId>com.scylladb</groupId>
  <artifactId>scylla-driver-extras</artifactId>
  <version>3.7.1-scylla-0-SNAPSHOT</version>
</dependency>
```


We also provide a [shaded JAR](../manual/shaded_jar/index)
to avoid the explicit dependency to Netty.

If you can't use a dependency management tool, a
[binary tarball](http://downloads.datastax.com/java-driver/cassandra-java-driver-3.7.1.tar.gz)
is available for download.

### Compatibility

The Java client driver 3.7.1 ([branch 3.x](https://github.com/datastax/java-driver/tree/3.x)) is compatible with Apache
Cassandra 2.1, 2.2 and 3.0+ (see [this page](http://docs.datastax.com/en/developer/java-driver/latest/manual/native_protocol/) for
the most up-to-date compatibility information).

UDT and tuple support is available only when using Apache Cassandra 2.1 or higher (see [CQL improvements in Cassandra 2.1](http://www.datastax.com/dev/blog/cql-in-2-1)).

Other features are available only when using Apache Cassandra 2.0 or higher (e.g. result set paging,
[BatchStatement](https://github.com/datastax/java-driver/blob/3.x/driver-core/src/main/java/com/datastax/driver/core/BatchStatement.java),
[lightweight transactions](http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_ltweight_transaction_t.html) 
-- see [What's new in Cassandra 2.0](http://www.datastax.com/documentation/cassandra/2.0/cassandra/features/features_key_c.html)). 
Trying to use these with a cluster running Cassandra 1.2 will result in 
an [UnsupportedFeatureException](https://github.com/datastax/java-driver/blob/3.x/driver-core/src/main/java/com/datastax/driver/core/exceptions/UnsupportedFeatureException.java) being thrown.

The java driver supports Java JDK versions 6 and above.

If using _DataStax Enterprise_, the [DataStax Enterprise Java driver][dse-driver] provides 
more features and better compatibility.

__Disclaimer__: Some _DataStax/DataStax Enterprise_ products might partially work on 
big-endian systems, but _DataStax_ does not officially support these systems.

### Upgrading from previous versions

If you are upgrading from a previous version of the driver, be sure to have a look at
the [upgrade guide](../upgrade_guide/index).

If you are upgrading to _DataStax Enterprise_, use the [DataStax Enterprise Java driver][dse-driver] for more
features and better compatibility.

[dse-driver]: https://docs.datastax.com/en/developer/java-driver-dse/
