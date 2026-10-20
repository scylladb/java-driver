# Metadata

The driver maintains global information about the Cassandra cluster it
is connected to. It is available via
[Cluster#getMetadata()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Cluster.html#getMetadata--).

## Schema metadata

Use [getKeyspace(String)](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#getKeyspace-java.lang.String-) or [getKeyspaces()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#getKeyspaces--)
to get keyspace-level metadata. From there you can access the keyspace’s
objects (tables, and UDTs and UDFs if relevant).

### Refreshes

Schema metadata gets refreshed in the following circumstances:

* **schema changes via the driver**: after successfully executing a
  schema-altering query (ex: `CREATE TABLE`), the driver waits for
  schema agreement (see below), then refreshes the schema.
* **third-party schema changes**: if another client (cqlsh, other driver
  instance…) changes the schema, the driver gets notified by Cassandra
  via a push notification. It refreshes the schema directly (there is no
  need to wait for schema agreement since Cassandra has already done it).

### Subscribing to schema changes

Users interested in being notified of schema changes can implement the
[SchemaChangeListener](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SchemaChangeListener.html) interface.

Every listener must [be registered](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Cluster.html#register-com.datastax.driver.core.SchemaChangeListener-) against a `Cluster` instance:

```java
Cluster cluster = ...
SchemaChangeListener myListener = ...
cluster.register(myListener);
```

Once registered, the listener will be notified of all schema changes detected by the driver,
regardless of where they originate from.

Note that it is preferable to register a listener only *after* the cluster is fully initialized,
otherwise the listener could be notified with a great deal of “Added” events as
the driver builds the schema metadata from scratch for the first time.

### Schema agreement

Schema changes need to be propagated to all nodes in the cluster. Once
they have settled on a common version, we say that they are in
agreement.

As explained above, the driver waits for schema agreement after
executing a schema-altering query. This is to ensure that subsequent
requests (which might get routed to different nodes) see an up-to-date
version of the schema.

```ditaa
 Application             Driver           Cassandra
------+--------------------+------------------+-----
      |                    |                  |
      |  CREATE TABLE...   |                  |
      |------------------->|                  |
      |                    |   send request   |
      |                    |----------------->|
      |                    |                  |
      |                    |     success      |
      |                    |<-----------------|
      |                    |                  |
      |          /--------------------\       |
      |          :Wait until all nodes+------>|
      |          :agree (or timeout)  :       |
      |          \--------------------/       |
      |                    |        ^         |
      |                    |        |         |
      |                    |        +---------|
      |                    |                  |
      |                    |  refresh schema  |
      |                    |----------------->|
      |                    |<-----------------|
      |   complete query   |                  |
      |<-------------------|                  |
      |                    |                  |
```

The schema agreement wait is performed synchronously, so the `execute`
call – or the completion of the `ResultSetFuture` if you use the async
API – will only return after it has completed.

The check is implemented by repeatedly querying system tables for the
schema version reported by each node, until they all converge to the
same value. If that doesn’t happen within a given timeout, the driver
will give up waiting.  The default timeout is 10 seconds, it can be
customized when building your cluster:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withMaxSchemaAgreementWaitSeconds(20)
    .build();
```

After executing a statement, you can check whether schema agreement was
successful or timed out:

```java
ResultSet rs = session.execute("CREATE TABLE...");
if (rs.getExecutionInfo().isSchemaInAgreement()) {
    // schema is in agreement
} else {
    // schema agreement timed out
}
```

You can also perform an on-demand check at any time:

```java
if (cluster.getMetadata().checkSchemaAgreement()) {
    // schema is in agreement
} else {
    // schema is not in agreement
}
```

The on-demand check does not retry, it only queries system tables once
(so `maxSchemaAgreementWaitSeconds` does not apply). If you need
retries, you’ll have to schedule them yourself (for example with a
custom executor).

Check out the API docs for the features in this section:

* [withMaxSchemaAgreementWaitSeconds(int)](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Cluster.Builder.html#withMaxSchemaAgreementWaitSeconds-int-)
* [isSchemaInAgreement()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/ExecutionInfo.html#isSchemaInAgreement--)
* [checkSchemaAgreement()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#checkSchemaAgreement--)

## Token metadata

This feature is probably of less interest to regular driver users, but
it will be useful if you’re writing an analytics client on top of the
driver.

[Metadata](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html) exposes a number of methods to manipulate tokens
and ranges: [getTokenRanges()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#getTokenRanges--), [getTokenRanges(String
keyspace, Host host)](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#getTokenRanges-java.lang.String-com.datastax.driver.core.Host-), [getReplicas(String keyspace,
TokenRange range)](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#getReplicas-java.lang.String-com.datastax.driver.core.TokenRange-), [newToken(String)](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#newToken-java.lang.String-) and
[newTokenRange(Token start, Token end)](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Metadata.html#newTokenRange-com.datastax.driver.core.Token-com.datastax.driver.core.Token-).

[TokenRange](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/TokenRange.html) provides various operations on ranges
(splitting, merging, etc.).

Each host exposes its primary tokens as [getTokens()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Host.html#getTokens--).

Finally, you can inject tokens in CQL queries with
[BoundStatement#setToken](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/BoundStatement.html#setToken-int-com.datastax.driver.core.Token-), and retrieve them from results with
[Row#getToken](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Row.html#getToken-int-) and [Row#getPartitionKeyToken](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Row.html#getPartitionKeyToken--).

As an example, here is how you could compute the splits to partition a
job (pseudocode):

```default
metadata = cluster.getMetadata()
for range : metadata.getTokenRanges() {
    hosts = metadata.getReplicas(keyspace, range)
    int n = estimateNumberOfSplits() // more on that below
    for split : range.splitEvenly(n)
        // pick a host to process split
}
```

For `estimateNumberOfSplits`, you need a way to estimate the total
number of partition keys (this is what analytics clients would
traditionally do with the Thrift operation `describe_splits_ex`).
Starting with Cassandra 2.1.5, this information is available in a system
table (see
[CASSANDRA-7688](https://issues.apache.org/jira/browse/CASSANDRA-7688)).
