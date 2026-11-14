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

## Statements

To execute a query, you  create a [Statement] instance and pass it to [Session#execute()][execute] or
[Session#executeAsync][executeAsync]. The driver provides various implementations:

* [SimpleStatement](simple/): a simple implementation built directly from a
  character string. Typically used for queries that are executed only
  once or a few times.
* [BoundStatement](prepared/): obtained by binding values to a prepared
  statement. Typically used for queries that are executed
  often, with different values.
* [BuiltStatement](built/): a statement built with the [QueryBuilder] DSL. It
  can be executed directly like a simple statement, or prepared.
* [BatchStatement](batch/): a statement that groups multiple statements to be
  executed as a batch.


The default consistency is `LOCAL_QUORUM`; the default serial consistency for the serial phase of
conditional updates (LWT) is `LOCAL_SERIAL`. Ordinary requests require a local replica quorum, while
LWT serial coordination is scoped to the local datacenter. These defaults apply to simple, bound and
batch statements. Explicit statement settings override the cluster's `QueryOptions`. Applications
coordinating conditional writes across datacenters for global uniqueness or distributed locks must
explicitly retain `SERIAL` for the serial phase; see the [serial consistency documentation][serial].
Applications that need the previous defaults can configure them when building the cluster:

```java
Cluster cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withQueryOptions(new QueryOptions()
        .setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
        .setSerialConsistencyLevel(ConsistencyLevel.SERIAL))
    .build();
```

`BatchStatement` uses a native-protocol `BATCH` message. With protocol v2, that message retains its
protocol-defined serial consistency of `SERIAL`; selecting `LOCAL_SERIAL` for `BatchStatement`
requires protocol v3 or newer. A CQL-text `BEGIN BATCH ... APPLY BATCH` sent as a `SimpleStatement`
uses a `QUERY` message instead and can carry `LOCAL_SERIAL` with protocol v2.

Adapters with their own consistency setting must apply it to every statement, including paged
scans.

[serial]: https://docs.scylladb.com/manual/stable/cql/consistency.html

### Customizing execution

Before executing a statement, you might want to customize certain
aspects of its execution. `Statement` provides a number of methods for
this, for example:

```java
Statement s = new SimpleStatement("select release_version from system.local");
s.enableTracing();
session.execute(s);
```

If you use custom policies ([RetryPolicy], [LoadBalancingPolicy],
[SpeculativeExecutionPolicy]...), you might also want to have custom
properties that influence statement execution. To achieve this, you can
wrap your statements in a custom [StatementWrapper] implementation.

[Statement]:                  https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Statement.html
[QueryBuilder]:               https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/querybuilder/QueryBuilder.html
[StatementWrapper]:           https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/StatementWrapper.html
[RetryPolicy]:                https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/RetryPolicy.html
[LoadBalancingPolicy]:        https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/LoadBalancingPolicy.html
[SpeculativeExecutionPolicy]: https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/policies/SpeculativeExecutionPolicy.html
[execute]:                    https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Session.html#execute-com.datastax.driver.core.Statement-
[executeAsync]:               https://docs.datastax.com/en/drivers/java/3.11/com/datastax/driver/core/Session.html#executeAsync-com.datastax.driver.core.Statement-

```{eval-rst}
.. toctree::
   :hidden:
   :glob:
   
   simple/*
   prepared/*
   built/*
   batch/*
```
