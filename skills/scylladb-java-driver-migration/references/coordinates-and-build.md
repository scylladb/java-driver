# Coordinates and Build

How to change the build files when moving from Java driver 3.x to 4.x.

## Artifact Mapping

The [What changes for ScyllaDB users](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#what-changes-for-scylladb-users) table in the migration guide maps each `com.scylladb` 3.x artifact to its 4.x replacement. In addition:

- Add `com.scylladb:java-driver-query-builder` only if the code uses `QueryBuilder`.
- For `scylla-driver-extras`, add nothing: most of its codecs are in `java-driver-core` as `ExtraTypeCodecs`; see [types-and-codecs](types-and-codecs.md).
- DataStax 3.x, `com.datastax.cassandra:cassandra-driver-core`, also moves to `com.scylladb:java-driver-core`. Its 3.x API is the same as ScyllaDB's 3.x, so the rest of this skill applies unchanged.

Use one version for every `java-driver-*` artifact. The latest is 4.19.4.0; check [Maven Central](https://central.sonatype.com/artifact/com.scylladb/java-driver-core) for newer releases.

### Wrong

```xml
<!-- Upstream driver: no shard-aware or tablet-aware routing -->
<dependency>
  <groupId>org.apache.cassandra</groupId>
  <artifactId>java-driver-core</artifactId>
  <version>4.19.0</version>
</dependency>
```

The same applies to `com.datastax.oss:java-driver-core`. Code that uses only the common API compiles against either one, and the mistake shows up only as lost performance in production. Code that calls ScyllaDB-only API, such as `Node.getShardingInfo()`, does not compile against them.

### Right: Maven

```xml
<properties>
  <scylla.driver.version>4.19.4.0</scylla.driver.version>
</properties>

<dependencies>
  <dependency>
    <groupId>com.scylladb</groupId>
    <artifactId>java-driver-core</artifactId>
    <version>${scylla.driver.version}</version>
  </dependency>
  <!-- Only if the code uses QueryBuilder -->
  <dependency>
    <groupId>com.scylladb</groupId>
    <artifactId>java-driver-query-builder</artifactId>
    <version>${scylla.driver.version}</version>
  </dependency>
  <!-- Only if the code uses the object mapper -->
  <dependency>
    <groupId>com.scylladb</groupId>
    <artifactId>java-driver-mapper-runtime</artifactId>
    <version>${scylla.driver.version}</version>
  </dependency>
</dependencies>
```

The mapper processor goes in the compiler plugin, not in `<dependencies>`:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-compiler-plugin</artifactId>
  <configuration>
    <annotationProcessorPaths>
      <path>
        <groupId>com.scylladb</groupId>
        <artifactId>java-driver-mapper-processor</artifactId>
        <version>${scylla.driver.version}</version>
      </path>
    </annotationProcessorPaths>
  </configuration>
</plugin>
```

If the project already lists other processors there, such as Lombok, add this one next to them. Listing `annotationProcessorPaths` turns off processor discovery on the classpath. It needs `maven-compiler-plugin` 3.5 or later; with an older pinned version the processor does not run and `UserMapperBuilder` is never generated.

For Kotlin `@Entity`, `@Dao`, or `@Mapper` sources, Java annotation processing does not run. Use [kapt](https://kotlinlang.org/docs/kapt.html) instead: a `kapt` execution of `kotlin-maven-plugin` with the processor in its `annotationProcessorPaths`, or the Gradle `kotlin-kapt` plugin with `kapt("com.scylladb:java-driver-mapper-processor:4.19.4.0")`.

### Right: Gradle

```groovy
dependencies {
    implementation("com.scylladb:java-driver-core:4.19.4.0")
    implementation("com.scylladb:java-driver-query-builder:4.19.4.0")   // only with QueryBuilder
    implementation("com.scylladb:java-driver-mapper-runtime:4.19.4.0")  // only with the mapper
    annotationProcessor("com.scylladb:java-driver-mapper-processor:4.19.4.0") // only with the mapper
}
```

## Java Version

- 4.19.2.0 and later are compiled for Java 11. On Java 8 they fail with `UnsupportedClassVersionError`. Keep the project's own compiler target unless it is below 11.
- 4.19.0.x is the last line that runs on Java 8. Use it only if the user cannot move off Java 8, and tell them it is a dead end.

## Guava

3.x exposed Guava in its API (`ListenableFuture`, `Futures`) and brought it in as a transitive dependency. 4.x shades Guava into `java-driver-guava-shaded`, so it is no longer on the application's classpath.

- Rewrite driver futures to `CompletionStage`; see [statements-and-results](statements-and-results.md#async-execution).
- If application code uses Guava for its own purposes, add `com.google.guava:guava` as a direct dependency. Do not import from the driver's shaded package (`com.datastax.oss.driver.shaded.guava`): it is internal and changes without notice.

## Logging

4.19.2.0 and later depend on `slf4j-api` 2.x; 4.19.0.x uses 1.7, so skip this section there. SLF4J 1.7 bindings are ignored, so the driver logs nothing and the application prints `No SLF4J providers were found` at startup. If the project has a 1.7-era binding, move it to its SLF4J 2 version:

| 1.7 binding | SLF4J 2 binding |
| --- | --- |
| `org.slf4j:slf4j-simple:1.7.x`, `slf4j-reload4j` | Same artifact, version 2.0.x |
| `ch.qos.logback:logback-classic:1.2.x` | `logback-classic` 1.3 or later |
| `org.apache.logging.log4j:log4j-slf4j-impl` | `org.apache.logging.log4j:log4j-slf4j2-impl` |

## Metrics and JMX

3.x reported metrics to JMX by default. 4.x does not, and session and node metrics are opt-in by name. To keep JMX, add `io.dropwizard.metrics:metrics-jmx`, as the [Metrics section](https://java-driver.docs.scylladb.com/stable/upgrade_guide/from_3x/index.md#metrics) of the migration guide says, and register a `JmxReporter` on the driver's registry, as shown in the [metrics manual](https://java-driver.docs.scylladb.com/stable/manual/core/metrics/index.md). To use Micrometer instead, add `com.scylladb:java-driver-metrics-micrometer` and set `advanced.metrics.factory.class = MicrometerMetricsFactory`.

## Both Drivers Side by Side

From step 3 to step 8 both drivers are on the classpath. They need different major versions of `io.dropwizard.metrics:metrics-core`: 3.x needs 3.2.x, 4.19.4.0 brings 4.2.x. Gradle always resolves the newer one; Maven resolves the one from the driver declared first. With metrics-core 4.x, a 3.x `Cluster` with JMX reporting on, the 3.x default, fails at startup with `NoClassDefFoundError: com/codahale/metrics/JmxReporter`.

Add `withoutJMXReporting()` to the 3.x `Cluster.builder()` (or `withoutMetrics()`, if the application does not read 3.x metrics) and keep it until step 8 deletes the `Cluster`. With its default settings, where metrics are off, a 4.x session starts with either metrics-core version.

`withoutJMXReporting()` removes the 3.x driver's MBeans. If monitoring or tests read them, tell the user. To keep them, add `io.dropwizard.metrics:metrics-jmx` and start a reporter, as the 3.x [Metrics 4 compatibility](https://java-driver.docs.scylladb.com/scylla-3.11.5.x/manual/metrics/index.md#metrics-4-compatibility) section shows:

```java
JmxReporter.forRegistry(cluster.getMetrics().getRegistry())
    .inDomain(cluster.getClusterName() + "-metrics")
    .build()
    .start();
```

## Remove the Old Artifacts

This is step 8. Do it when the driver-imports grep from SKILL.md finds only the 3.x `Cluster` setup, that is, when no other class uses 3.x. Close and delete the `Cluster`, with its `withoutJMXReporting()`. Remove every `scylla-driver-*` and `cassandra-driver-*` dependency, and any exclusion or version pin that existed only for them, such as Guava or metrics-core pins. Then rerun the driver-imports grep and the build-files grep; both must return nothing. The Guava grep may still match if the build now declares `com.google.guava:guava` itself.
