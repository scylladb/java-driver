# Reconnection

If the driver loses a connection to a node, it tries to re-establish it according to a configurable
policy. This is used in two places:

* [connection pools](): for each node, a session has a fixed-size pool of connections to
  execute user requests. If a node is detected as down, a reconnection is started.
* [control connection](): a session uses a single connection to an arbitrary
  node for administrative requests. If that connection goes down, a reconnection gets started; each
  attempt iterates through all active nodes until one of them accepts a connection. This goes on
  until we have a control node again.

[ReconnectionPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/ReconnectionPolicy.html) controls the interval between each attempt. The policy to use may be
provided using [Cluster.Builder.withReconnectionPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Cluster.Builder.html#withReconnectionPolicy-com.datastax.driver.core.policies.ReconnectionPolicy-).  For example, the following configures
an [ExponentialReconnectionPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/ExponentialReconnectionPolicy.html) with a base delay of 1 second, and a max delay of 10 minutes
(this is the default behavior).

```java
Cluster.builder()
  .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000))
  .build();
```

[ConstantReconnectionPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/ConstantReconnectionPolicy.html) uses the same delay every time, regardless of the
previous number of attempts.

You can also write your own policy; it must implement [ReconnectionPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/ReconnectionPolicy.html).

For best results, use reasonable values: very low values (for example a constant delay of 10
milliseconds) will quickly saturate your system.
