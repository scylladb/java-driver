# Statements, Results, and Async Code

How to port 3.x statement, result set, async, and exception code to 4.x. See [Statements](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#statements) and [Dual result set APIs](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#dual-result-set-apis) in the migration guide.

## Statements Are Immutable

Every 4.x statement setter returns a new statement. The most common migration bug is a setter call whose result is dropped. It compiles, and the option is silently lost.

### Wrong

```java
SimpleStatement stmt = SimpleStatement.newInstance("SELECT * FROM users WHERE id = ?", id);
stmt.setConsistencyLevel(DefaultConsistencyLevel.QUORUM);       // result ignored: the profile default applies
session.execute(stmt);

BoundStatement bound = ps.bind();
bound.setUuid("id", id);                                        // result ignored: id is unset
session.execute(bound);
```

### Right

```java
SimpleStatement stmt = SimpleStatement.newInstance("SELECT * FROM users WHERE id = ?", id)
    .setConsistencyLevel(DefaultConsistencyLevel.QUORUM);

BoundStatement bound = ps.bind().setUuid("id", id);
// or, to set many values without creating an object per call:
BoundStatement bound2 = ps.boundStatementBuilder().setUuid("id", id).setString("name", name).build();
```

After porting, search the code for setter and batch `add` calls used as statements on their own line (`^\s*\w+\.(set\w+|add|addAll)\(.*\);`) and check each one on a driver statement.

## Renamed Calls

| 3.x | 4.x |
| --- | --- |
| `new SimpleStatement(q, values...)` | `SimpleStatement.newInstance(q, values...)` |
| `new BoundStatement(ps)` | `ps.bind()` |
| `new BatchStatement()` / `new BatchStatement(BatchStatement.Type.UNLOGGED)` | `BatchStatement.builder(DefaultBatchType.LOGGED)` / `BatchStatement.builder(DefaultBatchType.UNLOGGED)`, then `.addStatement(s)` and `.build()`. A 3.x batch was idempotent when all its statements were; a 4.x batch ignores its statements and takes `basic.request.default-idempotence` (default `false`) unless you call `.setIdempotent(true)` on it. If the configuration sets `default-idempotence = true`, call `.setIdempotent(false)` on any batch with a non-idempotent statement, such as a counter update. A non-idempotent batch is not retried after a write timeout or request error, nor run speculatively. |
| `batch.add(s)` | `batch = batch.add(s)`. It returns a new batch, like every setter. |
| `stmt.setFetchSize(n)` | `stmt.setPageSize(n)` |
| `stmt.setReadTimeoutMillis(ms)` | `stmt.setTimeout(Duration.ofMillis(ms))`, which covers the whole request; see the timeout rule in SKILL.md. |
| `stmt.enableTracing()` | `stmt.setTracing(true)` |
| `stmt.setRetryPolicy(p)` | No per-statement policy. Define an execution profile with its own `advanced.retry-policy` and call `stmt.setExecutionProfileName("name")`. |
| `stmt.setPagingState(ps)` | `stmt.setPagingState(pagingState)` with a `PagingState` from `getSafePagingState()`. A paging state from another query throws `IllegalArgumentException`, not 3.x `PagingStateException`. The `ByteBuffer` overload skips that check, like 3.x `setPagingStateUnsafe`. |
| `rs.getExecutionInfo().getPagingState()` | `rs.getExecutionInfo().getSafePagingState()` (a `PagingState`), or `getPagingState()` (a raw `ByteBuffer`) |
| `PagingState.fromString(s)` / `pagingState.toString()` | `PagingState.fromString(s)` / `pagingState.toString()` on the 4.x `PagingState` type |
| `rs.getExecutionInfo().getQueryTrace()` | Same name; it blocks. Use `getQueryTraceAsync()` in async code. |
| `rs.fetchMoreResults()` | Removed. Background prefetch exists only in the async API (see below). |

`session.execute(String query, Object... values)`, `ResultSet.one()`, `all()`, iteration, `getAvailableWithoutFetching()` and `isFullyFetched()` keep their 3.x behavior. Call `wasApplied()` before consuming the rows: after `rs.one()` on a single-row LWT result it throws `IllegalStateException`.

Prepared statements are cached on the client in 4.x: calling `session.prepare(q)` twice returns the same instance. 3.x code that kept its own `ConcurrentHashMap<String, PreparedStatement>` still works; do not remove it during the migration.

## Async Execution

3.x returned Guava `ListenableFuture` (`ResultSetFuture`). 4.x returns `CompletionStage`, and the async result type is `AsyncResultSet`, which holds one page only.

### 3.x

```java
ResultSetFuture future = session.executeAsync(stmt);
Futures.addCallback(future, new FutureCallback<ResultSet>() {
  public void onSuccess(ResultSet rs) { for (Row row : rs) { process(row); } }
  public void onFailure(Throwable t) { log.error("query failed", t); }
}, executor);
```

Iterating a 3.x `ResultSet` inside a callback could block the I/O thread when it reached the end of a page.

### 4.x

```java
CompletionStage<Void> done = session.executeAsync(stmt).thenCompose(this::processPages);

private CompletionStage<Void> processPages(AsyncResultSet rs) {
  for (Row row : rs.currentPage()) {
    process(row);
  }
  return rs.hasMorePages()
      ? rs.fetchNextPage().thenCompose(this::processPages)
      : CompletableFuture.completedFuture(null);
}
```

- Do not iterate `AsyncResultSet` as if it held every row: `currentPage()` is one page.
- Do not call blocking methods (`session.execute`, `join`, `get`) inside a `thenApply` or `thenCompose` callback; they run on the driver's I/O threads.
- To block on a result at the edge of the application, use `stage.toCompletableFuture().join()`, which replaces 3.x `getUninterruptibly()`.
- `session.prepareAsync(q)` returns `CompletionStage<PreparedStatement>`.

## Exceptions

| 3.x (`com.datastax.driver.core.exceptions`) | 4.x |
| --- | --- |
| `NoHostAvailableException`, `getErrors()` | `AllNodesFailedException`, `getAllErrors()` (a `Map<Node, List<Throwable>>`). `NoNodeAvailableException` is the subclass thrown when no node could be tried. |
| `OperationTimedOutException` (client-side) | `DriverTimeoutException` |
| `ReadTimeoutException`, `WriteTimeoutException`, `UnavailableException`, `InvalidQueryException`, `SyntaxError`, `OverloadedException` | Same names in `com.datastax.oss.driver.api.core.servererrors` |
| `AuthenticationException` | `com.datastax.oss.driver.api.core.auth.AuthenticationException`, no longer a `DriverException`. At startup `build()` throws `AllNodesFailedException` with it in `getAllErrors()`, where 3.x `connect()` threw it directly. |
| `DriverException` | `com.datastax.oss.driver.api.core.DriverException` |

Failed `CompletionStage`s wrap the cause in `CompletionException` when joined. Unwrap it before matching on the driver exception type.

## Query Builder

Built queries are no longer executable; call `.build()` to get a `SimpleStatement`, or `.asCql()` for a string. See [Query builder](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#query-builder).

```java
// 3.x
Statement s3 = QueryBuilder.select().all().from("users").where(QueryBuilder.eq("id", id));

// 4.x
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
SimpleStatement s4 = selectFrom("users").all().whereColumn("id").isEqualTo(bindMarker()).build(id);
```
