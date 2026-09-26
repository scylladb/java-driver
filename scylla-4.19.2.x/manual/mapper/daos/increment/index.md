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

# Increment methods

Annotate a DAO method with [@Increment](https://java-driver.docs.scylladb.com/scylla-4.19.2.x/api/com/datastax/oss/driver/api/mapper/annotations/Increment.html) to generate a query that updates a counter table that is
mapped to an entity:

```java
// CREATE TABLE votes(article_id int PRIMARY KEY, up_votes counter, down_votes counter);

@Entity
public class Votes {
  @PartitionKey private int articleId;
  private long upVotes;
  private long downVotes;
  ... // constructor(s), getters and setters, etc.
}

@Dao
public interface VotesDao {
  @Increment(entityClass = Votes.class)
  void incrementUpVotes(int articleId, long upVotes);

  @Increment(entityClass = Votes.class)
  void incrementDownVotes(int articleId, long downVotes);
  
  @Select
  Votes findById(int articleId);
}
```

## Parameters

The entity class must be specified with `entityClass` in the annotation.

The method’s parameters must start with the [full primary key](),
in the exact order (as defined by the [@PartitionKey](https://java-driver.docs.scylladb.com/scylla-4.19.2.x/api/com/datastax/oss/driver/api/mapper/annotations/PartitionKey.html) and [@ClusteringColumn](https://java-driver.docs.scylladb.com/scylla-4.19.2.x/api/com/datastax/oss/driver/api/mapper/annotations/ClusteringColumn.html) annotations in the
entity class). The parameter names don’t necessarily need to match the names of the columns, but the
types must match. Unlike other methods like [@Select]() or [@Delete](), counter
updates cannot operate on a whole partition, they need to target exactly one row; so all the
partition key and clustering columns must be specified.

Then must follow one or more parameters representing counter increments. Their type must be
`long` or `java.lang.Long`. The name of the parameter must match the name of the entity
property that maps to the counter (that is, the name of the getter without “get” and
decapitalized). Alternatively, you may annotate a parameter with [@CqlName](https://java-driver.docs.scylladb.com/scylla-4.19.2.x/api/com/datastax/oss/driver/api/mapper/annotations/CqlName.html) to specify the
raw column name directly; in that case, the name of the parameter does not matter:

```java
@Increment(entityClass = Votes.class)
void incrementUpVotes(int articleId, @CqlName("up_votes") long foobar);
```

When you invoke the method, each parameter value is interpreted as a **delta** that will be applied
to the counter. In other words, if you pass 1, the counter will be incremented by 1. Negative values
are allowed. If you are using Cassandra 2.2 or above, you can use `Long` and pass `null` for some of
the parameters, they will be ignored (following [NullSavingStrategy#DO_NOT_SET]()
semantics). If you are using Cassandra 2.1, `null` values will trigger a runtime error.

A `Function<BoundStatementBuilder, BoundStatementBuilder>` or `UnaryOperator<BoundStatementBuilder>`
can be added as the **last** parameter. It will be applied to the statement before execution. This
allows you to customize certain aspects of the request (page size, timeout, etc) at runtime. See
[statement attributes]().

## Return type

The method can return `void`, a void [CompletionStage](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/CompletionStage.html) or [CompletableFuture](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/CompletableFuture.html), or a
[ReactiveResultSet](https://java-driver.docs.scylladb.com/scylla-4.19.2.x/api/com/datastax/dse/driver/api/core/cql/reactive/ReactiveResultSet.html).

## Target keyspace and table

If a keyspace was specified [when creating the DAO](), then the
generated query targets that keyspace. Otherwise, it doesn’t specify a keyspace, and will only work
if the mapper was built from a session that has a [default keyspace](https://java-driver.docs.scylladb.com/scylla-4.19.2.x/api/com/datastax/oss/driver/api/core/session/SessionBuilder.html#withKeyspace-com.datastax.oss.driver.api.core.CqlIdentifier-) set.

If a table was specified when creating the DAO, then the generated query targets that table.
Otherwise, it uses the default table name for the entity (which is determined by the name of the
entity class and the naming convention).
