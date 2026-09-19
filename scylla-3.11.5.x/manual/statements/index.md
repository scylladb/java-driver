# Statements

To execute a query, you  create a [Statement](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Statement.html) instance and pass it to [Session#execute()](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Session.html#execute-com.datastax.driver.core.Statement-) or
[Session#executeAsync](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/Session.html#executeAsync-com.datastax.driver.core.Statement-). The driver provides various implementations:

* [SimpleStatement](): a simple implementation built directly from a
  character string. Typically used for queries that are executed only
  once or a few times.
* [BoundStatement](): obtained by binding values to a prepared
  statement. Typically used for queries that are executed
  often, with different values.
* [BuiltStatement](): a statement built with the [QueryBuilder](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/querybuilder/QueryBuilder.html) DSL. It
  can be executed directly like a simple statement, or prepared.
* [BatchStatement](): a statement that groups multiple statements to be
  executed as a batch.

## Customizing execution

Before executing a statement, you might want to customize certain
aspects of its execution. `Statement` provides a number of methods for
this, for example:

```java
Statement s = new SimpleStatement("select release_version from system.local");
s.enableTracing();
session.execute(s);
```

If you use custom policies ([RetryPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/RetryPolicy.html), [LoadBalancingPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/LoadBalancingPolicy.html),
[SpeculativeExecutionPolicy](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/policies/SpeculativeExecutionPolicy.html)…), you might also want to have custom
properties that influence statement execution. To achieve this, you can
wrap your statements in a custom [StatementWrapper](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/api/com/datastax/driver/core/StatementWrapper.html) implementation.

```eval_rst
.. toctree::
   :hidden:
   :glob:
   
   simple/*
   prepared/*
   built/*
   batch/*
```
