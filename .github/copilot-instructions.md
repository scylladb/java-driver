# Copilot Instructions: Scylla/Cassandra Java Driver

Purpose: Help AI coding agents be immediately productive in this repository by capturing the big-picture architecture, everyday workflows, project-specific conventions, and integration points.

## Big Picture
- Multi-module Maven repo. Key modules: core (driver runtime), query-builder (fluent CQL DSL), mapper-runtime + mapper-processor (annotation-based DAOs), metrics (micrometer, microprofile), guava-shaded (relocated Guava), core-shaded (relocates Netty/Jackson), test-infra, integration-tests, osgi-tests, distribution, examples, bom.
- Public API lives under `com.datastax.oss.driver.api.*` (e.g., `CqlSession`). Internals under `com.datastax.oss.driver.internal.*` are not user-facing and may change; be cautious editing API signatures (Revapi checks enforce compatibility).
- Architecture (see manual/developer): central `DriverContext`, event bus for decoupling, Netty pipeline for I/O, request execution layer, admin/metadata management. Defaults and tunables via Typesafe Config (HOCON) with a comprehensive `reference.conf`.

## Daily Workflows
- Prereqs: JDK 8 and Maven. For integration tests, Python/pip for CCM (Makefile auto-installs) and Linux kernel `aio-max-nr` bump is handled.
- **Build order**: `guava-shaded` must be installed to the local Maven repo before other modules can compile, because the shade plugin only produces the JAR at the `install` phase. Makefile targets handle this automatically (via internal `.install-guava-shaded`), but raw `mvn compile` or `mvn test` will fail without running `mvn install -pl guava-shaded` first.
- Fast path commands (Makefile wraps Maven):
  - Warm caches: `make download-all-dependencies`
  - Compile only: `make compile-all`
  - Unit tests: `make test-unit`
  - Integration tests (Scylla/Cassandra via CCM): `make test-integration-scylla` or `make test-integration-cassandra`
  - Static checks (verify without tests): `make check`
  - Format Java code: `make fix` (runs `mvn fmt:format` — Java files only, Google Java Style)
  - Format XML (pom.xml, etc.): `mvn xml-format:xml-format` (no Makefile target — must be run manually)
- **Formatting is enforced by the build.** After editing Java files, run `mvn fmt:format` (or `make fix`). After editing XML files, run `mvn xml-format:xml-format`. The build will fail if either check does not pass. See `CONTRIBUTING.md` for the full coding style guide.
- Test targeting: Use `MVNCMD` to pass Maven flags, e.g. run a single test:
  - `MVNCMD="mvn -B -Dtest=ClassName#method" make test-unit`
- Integration test versions:
  - `SCYLLA_VERSION` (e.g., `LATEST`, `LTS-LATEST`, or explicit), `CASSANDRA_VERSION` (e.g., `4-LATEST`). The Makefile resolves tags and installs CCM as needed.

## Project Conventions
- Configuration: Driver defaults in core’s `reference.conf`. Override by adding `application.conf` to your classpath under the `datastax-java-driver` root key.
- API stability: Backward compatibility enforced via Revapi/Clirr. Avoid breaking changes in `api` packages; update `revapi.json` only when justified.
- Shading:
  - `core-shaded` relocates Netty (`io.netty` → `com.datastax.oss.driver.shaded.netty`) and Jackson. Don’t add unshaded Netty/Jackson deps to shaded artifacts.
  - `guava-shaded` provides a relocated Guava; depend on it in modules needing Guava to avoid conflicts.
- Metrics: Optional modules `metrics/micrometer` and `metrics/microprofile` integrate with those ecosystems.
- OSGi: Manifests via Felix bundle plugin; dependency changes can affect OSGi imports—run `osgi-tests` if you touch packaging.
- Dependency alignment: Use `java-driver-bom` in downstream apps to keep module versions consistent.

## Integration Points
- Native protocol: `com.scylladb:native-protocol` for frame encoding/decoding.
- Networking: Netty; logging via SLF4J; optional compression via Snappy/LZ4.
- Optional: TinkerPop/Gremlin integration (optional scope), reactive-streams.

## Key Paths & Examples
- Core API (entry points): `core/src/main/java/com/datastax/oss/driver/api/core/` (e.g., `CqlSession`, `CqlSessionBuilder`).
- Internals (for contributors): `core/src/main/java/com/datastax/oss/driver/internal/**` (request execution, load balancing, metadata, etc.).
- Configuration reference: `core/src/main/resources/reference.conf` (copy keys into your `application.conf`).
- Developer internals overview: `manual/developer/` (context, event bus, netty pipeline, request execution, admin).
- Examples: `examples/` assumes a local single-node cluster on `localhost:9042`.

## Useful Maven/Make Invocations
- Install guava-shaded (required before raw Maven commands): `mvn install -pl guava-shaded`
- Full verify skipping javadoc: `mvn -B -V verify -DskipITs -DskipTests -Dmaven.javadoc.skip=true`
- Single test via surefire: `mvn -B -Dtest=ClassName#method test`
- Quick local install (skips tests, formatting, Revapi, javadoc, license headers): `mvn install -Pfast`
- Quick targeted install: `mvn install -Pfast -pl core,query-builder,mapper-runtime,mapper-processor,bom`
- Clean workspace: `make clean`

## Gotchas
- **Build order**: `guava-shaded` must be installed (`mvn install -pl guava-shaded`) before compiling other modules. Makefile targets handle this automatically, but direct Maven invocations will fail without it.
- **Code formatting**: Run `mvn fmt:format` (or `make fix`) after editing Java files and `mvn xml-format:xml-format` after editing XML files — the build enforces both formatters and will fail on unformatted code.
- **Compiler warnings are errors**: The build uses Error Prone with `failOnWarning=true` — any compiler warning will break the build.
- Don't surface internal packages in public APIs; Revapi and OSGi tests will fail.
- When touching shaded modules, ensure relocations stay correct and re-run shaded builds.
- Integration tests require CCM and may adjust system settings; prefer Makefile targets which bootstrap prerequisites.

If any of the above is unclear or you need deeper detail on a specific area (e.g., request execution, load balancing, or mapper codegen), please ask and we’ll refine these instructions.
