# Verification

How to check a 3.x to 4.x migration before calling it done. A migration that compiles can still fail at startup, write nulls differently, drop a consistency level, or lose shard-aware routing. Run every section below, and report to the user what was checked and what was not.

## 1. Build Checks

1. The project compiles with the 4.x artifacts and with the Java version from [coordinates-and-build](coordinates-and-build.md#java-version).
2. The driver-imports grep and the build-files grep from SKILL.md return nothing. The Guava grep may still match if the build declares Guava directly; see [coordinates-and-build](coordinates-and-build.md#guava).
3. The resolved dependency tree has no 3.x driver and no upstream 4.x driver:

```bash
# Maven
mvn dependency:tree | grep -E 'scylla-driver|cassandra-driver|com\.datastax\.oss:java-driver|org\.apache\.cassandra:java-driver'
# Gradle
./gradlew dependencies --configuration runtimeClasspath | grep -E 'scylla-driver|cassandra-driver|com\.datastax\.oss:java-driver|org\.apache\.cassandra:java-driver'
```

Both must print nothing. A hit means another dependency pulls in a driver; exclude it there, or tell the user if that library itself needs 3.x.

4. The project's own tests pass. Do not delete or skip a failing test to make the build green; a failure after migration usually points to one of the behavior changes in the Rules section of SKILL.md.

## 2. Run Against ScyllaDB

Unit tests with mocked sessions do not exercise codecs, the mapper, or routing. Run the application, or its integration tests, against a real ScyllaDB node: a test environment the user names, or a local node in Docker. The checks below write and overwrite rows, so never run them against a production or shared cluster unless the user explicitly confirms. To start a local node:

```bash
docker run --name scylla -d -p 9042:9042 scylladb/scylla --smp 2 --memory 2G --overprovisioned 1
docker exec scylla nodetool status   # wait until the node shows UN
```

- The local datacenter of this container is `datacenter1`. Use it for `withLocalDatacenter` or `basic.load-balancing-policy.local-datacenter` in the test configuration only; keep the user's production value.
- `--smp 2` gives the node two shards, so the shard-awareness check below has something to see.
- When done, ask the user before you remove the container with `docker rm -f scylla`.

## 3. Behavior Checks

For each entity or table the code touches, write one row through the migrated code and read it back with `cqlsh` (`docker exec -it scylla cqlsh`). Compare with what the 3.x code would have written.

| Check | What a failure looks like |
| --- | --- |
| Every `timestamp`, `date`, and `time` column is written and read | `CodecNotFoundException` at runtime; see [types-and-codecs](types-and-codecs.md) |
| Columns with enum or JSON codecs are written and read | `CodecNotFoundException` for the custom type |
| Mapped entities write to the existing column names | DAO creation fails with `has missing columns: [first_name]`; see [object-mapper](object-mapper.md) |
| Saving an entity with a `null` field | The column keeps its old value instead of becoming null, or the reverse |
| Statements that set a consistency level, page size, or timeout | The setting is silently ignored; see [statements-and-results](statements-and-results.md) |
| Queries that page through many rows | Only the first page comes back in async code |

To confirm that a statement option survived the port, print it from the statement the driver actually ran:

```java
ResultSet rs = session.execute(stmt);
Statement<?> ran = (Statement<?>) rs.getExecutionInfo().getRequest();
System.out.println(ran.getConsistencyLevel()); // null: the profile default applies
```

## 4. Shard-Aware Routing

The 4.x `com.scylladb` driver routes each request to the right shard. Code that uses only the common API also runs on the upstream driver, without shard awareness, so check it explicitly. The check uses `getShardingInfo()`, which exists only in the ScyllaDB driver; if it does not compile, the build resolved an upstream driver.

```java
for (Node node : session.getMetadata().getNodes().values()) {
  NodeShardingInfo sharding = node.getShardingInfo();
  System.out.printf("%s shards=%s connections=%d%n", node.getEndPoint(),
      sharding == null ? "none" : sharding.getShardsCount(), node.getOpenConnections());
}
```

- Against the `--smp 2` container, expect `shards=2` and at least 2 connections, plus the control connection.
- `getShardingInfo()` is `null` before the node's pool is created, so run the check after the first query. `shards=none` after that means the driver got no sharding information from the node.

## 5. Before Reporting Done

Tell the user:

- which checks ran, and which could not run (for example, no access to their cluster);
- every behavior change you kept or decided on: request timeout, mapper naming strategy, null saving strategy, and any `ALLOW FILTERING` query you flagged;
- which settings moved from code to `application.conf`, so they are deployed with the application.
