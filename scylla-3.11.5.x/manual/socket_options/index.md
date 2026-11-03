# Socket options

[SocketOptions](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html) controls various low-level parameters related to TCP connections between the driver and Cassandra.

You can provide these when initializing the cluster:

```java
Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .withSocketOptions(
                new SocketOptions()
                        .setConnectTimeoutMillis(2000))
        .build();
```

If you don’t, the driver uses a default configuration (see the javadocs of the setter methods of [SocketOptions](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html) for the
default values).

You can retrieve and change the options at runtime:

```java
SocketOptions socketOptions = cluster.getConfiguration().getSocketOptions();
socketOptions.setConnectTimeoutMillis(3000);
```

* changes to the [read timeout]() will be taken into account for future request executions;
* changes to any other option will be taken into account for future connections (connections that were already opened at
  the time of the change are unaffected, they keep the old values).

## TCP options

[setConnectTimeoutMillis](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setConnectTimeoutMillis-int-) defines how long the driver waits to establish a new connection to a Cassandra node before
giving up.

Other options control the usual low-level TCP parameters (refer to their individual javadoc for details):
[setKeepAlive](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setKeepAlive-boolean-), [setReceiveBufferSize](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setReceiveBufferSize-int-), [setReuseAddress](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setReuseAddress-boolean-), [setSendBufferSize](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setSendBufferSize-int-), [setSoLinger](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setSoLinger-int-), [setTcpNoDelay](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setTcpNoDelay-boolean-). Most of
these options are not set explicitly by the driver, so they get the default value from the underlying TCP transport.
One exception is `setTcpNoDelay`, which is forced to `true` (meaning that Nagle’s algorithm is *disabled* for driver
connections).

## Driver read timeout

[setReadTimeoutMillis](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/SocketOptions.html#setReadTimeoutMillis-int-) controls how long the driver waits for a response *from a given Cassandra node* before
considering it unresponsive.

Cassandra normally provides a guaranteed response time for each type of query, as shown by these parameters in
`cassandra.yaml` (here with their default values):

```yaml
counter_write_request_timeout_in_ms: 5000
range_request_timeout_in_ms: 10000
read_request_timeout_in_ms: 5000
request_timeout_in_ms: 10000
truncate_request_timeout_in_ms: 60000
write_request_timeout_in_ms: 2000
```

The goal of `setReadTimeoutMillis` is to give up on a node if it took longer than these thresholds to reply, on the
assumption that there’s probably something wrong with it. Therefore it should be set **higher than the server-side
timeouts**.

The default value is **12 seconds**. For truncate queries (where the server timeout is 60 seconds) or aggregate queries
(where `read_request_timeout_in_ms` applies per page of processed data, not to the whole query), you can increase the
timeout on a specific statement:

```java
session.execute(
        new SimpleStatement("TRUNCATE tmp").setReadTimeoutMillis(65000));
```

Do not set the read timeout too low, or the driver might give up on requests that had a chance of succeeding.

If the timeout is reached, the driver will receive an [OperationTimedOutException](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/exceptions/OperationTimedOutException.html), and invoke [onRequestError](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/RetryPolicy.html#onRequestError-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-com.datastax.driver.core.exceptions.DriverException-int-) on the
[retry policy]() to decide what to do (the default is to retry on the next node in the
[query plan]()).

### Limiting overall query time

It should be clear by now that `setReadTimeoutMillis` is *per node*, not per query. If the driver retries on 4 different
nodes, the overall execution time could theoretically be up to 4 times the read timeout. If you want a per query timeout,
use the following pattern:

```java
import com.google.common.base.Throwables;

ResultSet execute(Statement statement, long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
    ResultSetFuture future = session.executeAsync(statement);
    try {
        return future.get(timeout, unit);
    } catch (ExecutionException e) {
        throw Throwables.propagate(e.getCause());
    } catch (TimeoutException e) {
        future.cancel(true);
        throw e;
    }
}
```

A complementary approach is to enable [speculative executions](), to have the driver query
multiple nodes in parallel. This way you won’t have to wait for the full timeout if the first node is unresponsive.

### Driver read timeout vs. server read timeout

Unfortunately, the term “read timeout” clashes with another concept that is not directly related: a Cassandra node may
reply with a [READ_TIMEOUT]() error when it didn’t hear back from enough replicas during a
read query.

To clarify:

* **driver read timeout:** the driver did not receive any response from the current coordinator within
  `SocketOptions.setReadTimeoutMillis`. It invokes [onRequestError](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/RetryPolicy.html#onRequestError-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-com.datastax.driver.core.exceptions.DriverException-int-) on the [retry policy]() with an
  [OperationTimedOutException](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/exceptions/OperationTimedOutException.html) to decide what to do.
* **server read timeout:** the driver *did* receive a response, but that response indicates that the coordinator timed
  out while waiting for other replicas. It invokes [onReadTimeout](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/RetryPolicy.html#onReadTimeout-com.datastax.driver.core.Statement-com.datastax.driver.core.ConsistencyLevel-int-int-boolean-int-) on the [retry policy]() to decide what to
  do.

We might rename `SocketOptions.setReadTimeoutMillis` in a future version to clear up any confusion.
