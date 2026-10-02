# Control connection

The control connection is a dedicated connection used for administrative tasks:

* querying system tables to learn about the cluster’s topology and
  [schema]();
* checking [schema agreement]();
* reacting to server events, which are used to notify the driver of external topology or schema
  changes.

When the driver starts, the control connection is established to the first contacted node. If that
node goes down, a [reconnection]() is started to find another node; it is governed
by the same policy as regular connections and tries the nodes according to a query plan from the
[load balancing policy]().

The control connection is managed independently from [regular pooled connections](), and
used exclusively for administrative requests. It is included in [Session.State.getOpenConnections](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Session.State.html#getOpenConnections-com.datastax.driver.core.Host-),
as well as the `open-connections` [metric](); for example, if you’ve configured a pool
size of 2, the control node will have 3 connections.
