---
name: scylladb-java-driver-migration
description: Migrate a Java application from ScyllaDB Java driver 3.x (com.scylladb:scylla-driver-core) or the DataStax 3.x driver to the ScyllaDB Java driver 4.x line, keeping shard-aware and tablet-aware routing. Use this skill when code imports com.datastax.driver.core or com.datastax.driver.mapping, builds a Cluster, uses the 3.x object mapper or scylla-driver-extras codecs, when a user asks to upgrade the Java driver to 4.x, or when Java driver 3.x deprecation comes up. Connecting a new application to ScyllaDB Cloud goes to scylladb-cloud-setup. Triggers on "migrate Java driver", "upgrade Java driver to 4.x", "Java driver 3.x deprecated", "scylla-driver-core", "scylla-driver-mapping", "cassandra-driver-core", "com.datastax.driver.core", "Cluster.builder", "MappingManager", "ResultSetFuture".
---

# ScyllaDB Java Driver 3.x to 4.x Migration

If the task goes beyond this skill, use the parent `scylladb` skill for a ScyllaDB overview and the
list of other ScyllaDB skills. If it is not installed, fetch it from
`https://raw.githubusercontent.com/scylladb/agent-skills/main/skills/scylladb/SKILL.md`. That skill
and the other skills named here, such as `scylladb-cloud-setup` and `scylladb-data-modeling`, live
in `scylladb/agent-skills`, not in this repository.

Java driver 3.x is deprecated ([driver
support](https://docs.scylladb.com/stable/versioning/driver-support.md)). Driver 4.x is a redesign:
packages, the session model, statements, temporal types, configuration and the object mapper all
change, so the migration is a code change, not a version bump. The [3.x to 4.x migration
guide](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md) is the source of
truth for the API; this skill adds the workflow and the mistakes agents make.

## When to Use This Skill

- Java code imports `com.datastax.driver.core`, `com.datastax.driver.mapping` or
  `com.datastax.driver.extras`
- The build depends on `com.scylladb:scylla-driver-core`, `scylla-driver-mapping`,
  `scylla-driver-extras`, or DataStax's `com.datastax.cassandra:cassandra-driver-core`
- The user asks to upgrade the Java driver to 4.x, or asks what Java driver 3.x deprecation means
  for their code
- The build fails after the driver version was bumped to 4.x

For adjacent topics, use the sibling skill instead:

- Connecting a new application to ScyllaDB Cloud, choosing a driver for a new project:
  `scylladb-cloud-setup`
- Schema, keys, and query design: `scylladb-data-modeling`

## Migration Workflow

Follow these steps in order. After each step, and in steps 5 to 7 after each class, build and run
the project's tests, and fix failures before you move on. If the build already fails because the 3.x
dependency was replaced with 4.x, restore the 3.x dependency first, so that the build passes before
you start. The 3.x and 4.x packages differ (`com.datastax.driver.core` and
`com.datastax.oss.driver.api.core`), so both drivers stay on the classpath from step 3 until step 8,
and each class uses one of them.

1. **Inventory.** Find every 3.x usage before you edit anything, and show the user the counts. The
   skill refers to these three commands by name:

   ```sh
   # driver-imports grep
   grep -rlE 'com\.datastax\.driver\.(core|mapping|extras)' --include='*.java' --include='*.kt' --include='*.scala' .
   # build-files grep: also covers version catalogs and buildSrc
   grep -rnE 'scylla-driver-(core|mapping|extras)|cassandra-driver-(core|mapping|extras)' --include='pom.xml' --include='*.gradle' --include='*.gradle.kts' --include='*.toml' --include='*.sbt' --include='*.kt' .
   # Guava grep
   grep -rlE 'import (static )?com\.google\.common\.' --include='*.java' --include='*.kt' --include='*.scala' .
   ```

   3.x brought Guava in transitively; 4.x shades it, so the imports the Guava grep finds stop
   compiling unless the build declares Guava itself.
2. **Check the Java version.** 4.19.2.0 and later need Java 11 or newer. If the project builds with
   Java 8, stop and ask the user: move to Java 11+ (recommended), or target the 4.19.0.x line, the
   last one that runs on Java 8.
3. **Add the 4.x dependencies** next to the 3.x ones, and add `withoutJMXReporting()` to the 3.x
   `Cluster`: without it, 3.x fails at startup when the build resolves the metrics-core version 4.x
   brings. See
   [coordinates-and-build](references/coordinates-and-build.md#both-drivers-side-by-side).
4. **Open a `CqlSession` next to the 3.x `Cluster`**, with policies and options in
   `application.conf`. No class uses it yet. See
   [session-and-configuration](references/session-and-configuration.md).
5. **Port statements, result sets, async code, and exceptions.** See
   [statements-and-results](references/statements-and-results.md).
6. **Port temporal types, getters, and codecs.** See
   [types-and-codecs](references/types-and-codecs.md).
7. **Port the object mapper**, if the code uses `com.datastax.driver.mapping`. See
   [object-mapper](references/object-mapper.md).
8. **Remove 3.x**: close and delete the `Cluster`, then remove the 3.x artifacts. See [Remove the
   Old Artifacts](references/coordinates-and-build.md#remove-the-old-artifacts).
9. **Verify** with the tests and against a running ScyllaDB node. See
   [verification](references/verification.md).

Steps 5 to 7 go one class at a time, and they are one unit for that class: move it from the 3.x
`Session` to the `CqlSession` and port its statements, types, and mapper usage together, then build
and test. Classes not yet ported keep using 3.x. If classes pass 3.x types to each other, such as a
DAO that returns a `ResultSet` or `Row` to its caller, port them together as one unit.

## Rules

- **Use `com.scylladb` artifacts.** The 4.x core is `com.scylladb:java-driver-core`,
  latest 4.19.4.0. Never switch to `com.datastax.oss` or `org.apache.cassandra`: those drivers have
  no shard-aware or tablet-aware routing. Parts of the 4.x manual come from upstream and show
  `org.apache.cassandra`; use `com.scylladb` anyway.
- **Keep the `com.datastax.oss.driver.api` imports.** The ScyllaDB 4.x driver keeps the upstream
  package names. Do not invent `com.scylladb.*` packages.
- **Set the local datacenter explicitly.** If the 3.x code named one (`withLocalDc`), set the same
  value with `withLocalDatacenter("<dc>")` or `basic.load-balancing-policy.local-datacenter`. If it
  did not, ask the user which datacenter is local; do not leave it to inference. Only 4.19.0.9
  and 4.19.2.0 or later infer it; other 4.x versions fail at startup without it.
- **Reassign statements.** 4.x statements are immutable: `stmt.setConsistencyLevel(...)` returns a
  new statement and leaves `stmt` unchanged. A 3.x-style call whose result is ignored compiles and
  silently does nothing.
- **Recheck timeouts.** 3.x `SocketOptions.setReadTimeoutMillis` (default 12 s) applied per
  attempt. 4.x `basic.request.timeout` (default 11 s since 4.19.2.3, 2 s in older 4.x releases)
  covers the whole request, retries and speculative executions included. A request to an
  unresponsive node no longer moves on to the next node after one attempt's timeout: it waits out
  the whole timeout and fails with `DriverTimeoutException`. Speculative executions are the closest
  replacement; ask the user before adding them.
- **Keep the mapper's column names.** The 4.x mapper defaults to snake case (`firstName` maps to
  `first_name`); 3.x mapped `firstName` to `firstname`. Without `@NamingStrategy`, creating the DAO
  fails at startup with `has missing columns: [first_name]`, or, if the table also has a
  `first_name` column, silently uses it. A clean startup does not prove the mapping is unchanged.
  See [object-mapper](references/object-mapper.md).
- **Do not drop ScyllaDB features in translation.** Keep rack awareness, shard-aware port settings,
  and LWT routing; each has a 4.x configuration key in
  [session-and-configuration](references/session-and-configuration.md).
- **Change one thing per step.** Do not change the schema, consistency levels, or retry behavior
  while you migrate, unless the user asks. Behavior must stay the same.

## Quick Reference

- [coordinates-and-build](references/coordinates-and-build.md) — Maven and Gradle coordinates for
  every 3.x artifact, Java version, Guava. Consult when editing `pom.xml`, `build.gradle`, or
  `build.sbt`.
- [session-and-configuration](references/session-and-configuration.md) — `Cluster.builder()` options
  mapped to `CqlSession.builder()` and `application.conf`: contact points, local DC and rack,
  credentials, TLS, policies, pooling, shard awareness. Consult when rewriting session setup or a
  `Cluster.Builder` call.
- [statements-and-results](references/statements-and-results.md) — Simple, bound and batch
  statements, paging, async execution with `CompletionStage`, exceptions. Consult when code calls
  `execute`, `executeAsync`, `prepare`, `bind`, or catches driver exceptions.
- [types-and-codecs](references/types-and-codecs.md) — Row getter renames, `java.time` types, UDTs
  and tuples, `scylla-driver-extras` codecs mapped to `ExtraTypeCodecs`. Consult when code reads or
  writes rows, or registers a `TypeCodec`.
- [object-mapper](references/object-mapper.md) — `MappingManager`, `Mapper` and `@Accessor` mapped
  to `@Entity`, `@Dao` and `@Mapper` with the annotation processor. Consult when code imports
  `com.datastax.driver.mapping`.
- [verification](references/verification.md) — Build checks, integration test against ScyllaDB in
  Docker, and how to confirm shard-aware routing. Consult before telling the user the migration is
  done.
